/*
 * Copyright 2019 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.io.bigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.bigquery.storage.v1beta1.AvroProto.AvroRows;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamPosition;
import com.google.cloud.hadoop.io.bigquery.DirectBigQueryInputFormat.DirectBigQueryInputSplit;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class DirectBigQueryRecordReaderTest {

  // Corresponds to a single nullable INTEGER
  private static final String RAW_SCHEMA =
      "{\"type\": \"record\", \"name\": \"__root__\", \"fields\": [{\"name\": \"f0_\", \"type\":"
          + " [\"null\", \"long\"]}]}";
  private Schema parsedSchema;

  private static final List<ReadRowsResponse> RESPONSES_123 =
      ImmutableList.of(
          ReadRowsResponse.newBuilder()
              .setAvroRows(
                  AvroRows.newBuilder()
                      .setRowCount(2)
                      .setSerializedBinaryRows(
                          ByteString.copyFrom(new byte[] {2, 2, 2, 4}))) // 1, 2
              .build(),
          ReadRowsResponse.newBuilder()
              .setAvroRows(
                  AvroRows.newBuilder()
                      .setRowCount(1)
                      .setSerializedBinaryRows(ByteString.copyFrom(new byte[] {2, 6}))) // 3
              .build());

  private DirectBigQueryInputSplit split = new DirectBigQueryInputSplit("session", RAW_SCHEMA, 5);
  private static final Stream STREAM = Stream.newBuilder().setName("session").build();

  @Mock private BigQueryStorageClient bqClient;
  // TODO: investigate google-cloud-java test classes to avoid so many mocks.
  @Mock private ServerStreamingCallable<ReadRowsRequest, ReadRowsResponse> readRows;
  @Mock private TaskAttemptContext taskContext;
  @Mock private ServerStream<ReadRowsResponse> rowsStream;

  private DirectBigQueryRecordReader reader;

  @Before
  public void setUp() {
    // Manually creating the parsed Schema is a pain so duplicate logic
    Parser parser = new Parser();
    parsedSchema = parser.parse(RAW_SCHEMA);

    MockitoAnnotations.initMocks(this);
    when(bqClient.readRowsCallable()).thenReturn(readRows);
    when(readRows.call(any(ReadRowsRequest.class))).thenReturn(rowsStream);

    reader = new TestDirectBigQueryRecordReader();
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(bqClient);
  }

  private void initialize() throws Exception {
    ReadRowsRequest request =
        ReadRowsRequest.newBuilder()
            .setReadPosition(StreamPosition.newBuilder().setStream(STREAM))
            .build();

    reader.initialize(split, taskContext);

    verify(bqClient).readRowsCallable();
    verify(readRows).call(eq(request));
  }

  @Test
  public void testInitialize() throws Exception {
    initialize();
  }

  @Test
  public void testEmpty() throws Exception {
    when(rowsStream.iterator()).thenReturn(ImmutableList.<ReadRowsResponse>of().iterator());

    // Set up reader
    initialize();

    // Don't bother testing current value before nextKeyValue, because it is undefined.

    assertThat(reader.nextKeyValue()).isFalse();

    // Don't bother testing current values after end, because it's undefined.
  }

  private GenericRecord avroRecord(int i) {
    Record rec = new Record(parsedSchema);
    rec.put(0, Integer.toUnsignedLong(i));
    return rec;
  }

  @Test
  public void testRead() throws Exception {
    when(rowsStream.iterator()).thenReturn(RESPONSES_123.iterator());

    // Set up reader
    initialize();

    for (int i = 1; i <= 3; i++) {
      assertThat(reader.nextKeyValue()).isTrue();
      assertThat(reader.getCurrentKey()).isEqualTo(NullWritable.get());
      assertThat(reader.getCurrentValue()).isEqualTo(avroRecord(i));
    }
    assertThat(reader.nextKeyValue()).isFalse();
  }

  @Test
  public void testLimiting() throws Exception {
    when(rowsStream.iterator()).thenReturn(RESPONSES_123.iterator());

    // Set limit lower
    split = new DirectBigQueryInputSplit("session", RAW_SCHEMA, 1);

    // Set up reader
    initialize();

    assertThat(reader.nextKeyValue()).isTrue();

    verify(bqClient).finalizeStream(eq(STREAM));

    assertThat(reader.getCurrentKey()).isEqualTo(NullWritable.get());
    assertThat(reader.getCurrentValue()).isEqualTo(avroRecord(1));

    // Test that we keep reading anyways

    for (int i = 2; i <= 3; i++) {
      assertThat(reader.nextKeyValue()).isTrue();
      assertThat(reader.getCurrentKey()).isEqualTo(NullWritable.get());
      assertThat(reader.getCurrentValue()).isEqualTo(avroRecord(i));
    }
    assertThat(reader.nextKeyValue()).isFalse();
  }

  class TestDirectBigQueryRecordReader extends DirectBigQueryRecordReader {

    @Override
    protected BigQueryStorageClient getClient(Configuration conf) {
      return bqClient;
    }
  }
}
