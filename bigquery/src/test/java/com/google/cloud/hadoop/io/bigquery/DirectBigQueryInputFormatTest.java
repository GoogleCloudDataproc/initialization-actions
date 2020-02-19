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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1beta1.AvroProto.AvroSchema;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.DataFormat;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import com.google.cloud.hadoop.io.bigquery.DirectBigQueryInputFormat.DirectBigQueryInputSplit;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class DirectBigQueryInputFormatTest {

  @Mock private BigQueryHelper bqHelper;
  @Mock private BigQueryStorageClient bqClient;
  @Mock private TaskAttemptContext taskContext;

  private JobConf config;
  private DirectBigQueryInputFormat input;
  private TableReference tableRef;

  // Sample projectIds for testing; one for owning the BigQuery jobs, another for the
  // TableReference.
  private String jobProjectId = "foo-project";
  private String dataProjectId = "publicdata";
  private String datasetId = "test_dataset";
  private String tableId = "test_table";

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    // Create table reference.
    tableRef =
        new TableReference()
            .setProjectId(dataProjectId)
            .setDatasetId(datasetId)
            .setTableId(tableId);

    Table table =
        new Table()
            .setTableReference(tableRef)
            .setLocation("test_location")
            .setNumRows(BigInteger.valueOf(23))
            .setNumBytes(3L * 128 * 1024 * 1024);

    when(bqHelper.getTable(any(TableReference.class))).thenReturn(table);

    config = new JobConf();
    config.set(BigQueryConfiguration.PROJECT_ID.getKey(), jobProjectId);
    config.set(BigQueryConfiguration.INPUT_PROJECT_ID.getKey(), dataProjectId);
    config.set(BigQueryConfiguration.INPUT_DATASET_ID.getKey(), datasetId);
    config.set(BigQueryConfiguration.INPUT_TABLE_ID.getKey(), tableId);
    config.set(MRJobConfig.NUM_MAPS, "3");
    config.set(BigQueryConfiguration.SKEW_LIMIT.getKey(), "1.2");
    config.set(BigQueryConfiguration.SQL_FILTER.getKey(), "foo == 0");
    config.set(BigQueryConfiguration.SELECTED_FIELDS.getKey(), "foo,bar");

    input = new TestDirectBigQueryInputFormat();
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(bqClient);
    verifyNoMoreInteractions(bqHelper);
  }

  @Test
  public void getSplits() throws IOException {
    JobContext jobContext = new JobContextImpl(config, new JobID());

    CreateReadSessionRequest request =
        CreateReadSessionRequest.newBuilder()
            .setTableReference(
                TableReferenceProto.TableReference.newBuilder()
                    .setProjectId("publicdata")
                    .setDatasetId("test_dataset")
                    .setTableId("test_table"))
            .setRequestedStreams(3) // request 3, but only get 2 back
            .setParent("projects/foo-project")
            .setReadOptions(
                TableReadOptions.newBuilder()
                    .addAllSelectedFields(ImmutableList.of("foo", "bar"))
                    .setRowRestriction("foo == 0")
                    .build())
            .setFormat(DataFormat.AVRO)
            .build();

    ReadSession session =
        ReadSession.newBuilder()
            .setAvroSchema(AvroSchema.newBuilder().setSchema("schema").build())
            .addAllStreams(
                ImmutableList.of(
                    Stream.newBuilder().setName("stream1").build(),
                    Stream.newBuilder().setName("stream2").build()))
            .build();
    ImmutableList<DirectBigQueryInputSplit> expected =
        ImmutableList.of(
            new DirectBigQueryInputSplit("stream1", "schema", 14),
            new DirectBigQueryInputSplit("stream2", "schema", 14));

    when(bqClient.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(session);
    try {
      List<InputSplit> splits = input.getSplits(jobContext);
      assertThat(splits).containsExactlyElementsIn(expected);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

    verify(bqHelper).getTable(tableRef);
    verify(bqClient).createReadSession(request);
  }

  @Test
  public void createRecordReader() {
    assertThat(
            input.createRecordReader(new DirectBigQueryInputSplit("foo", "schema", 7), taskContext))
        .isInstanceOf(DirectBigQueryRecordReader.class);
  }

  class TestDirectBigQueryInputFormat extends DirectBigQueryInputFormat {

    @Override
    protected BigQueryStorageClient getClient(Configuration config) {
      return bqClient;
    }

    @Override
    protected BigQueryHelper getBigQueryHelper(Configuration config) {
      return bqHelper;
    }
  }
}
