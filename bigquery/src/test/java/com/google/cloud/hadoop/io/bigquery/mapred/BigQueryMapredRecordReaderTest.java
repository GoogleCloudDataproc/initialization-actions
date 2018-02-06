/**
 * Copyright 2017 Google LLC
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
package com.google.cloud.hadoop.io.bigquery.mapred;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.gson.JsonObject;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link BigQueryMapredRecordReader}.
 */
@RunWith(JUnit4.class)
public class BigQueryMapredRecordReaderTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Mock private RecordReader<LongWritable, JsonObject> mockRecordReader;

  @Before public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After public void tearDown() {
    verifyNoMoreInteractions(mockRecordReader);
  }

  @Test public void testClose() throws IOException, InterruptedException {
    BigQueryMapredRecordReader recordReader =
        new BigQueryMapredRecordReader(mockRecordReader, 0);
    doNothing().when(mockRecordReader).close();
    recordReader.close();
    verify(mockRecordReader).close();
  }

  @Test public void testCreateKeyValue() {
    BigQueryMapredRecordReader recordReader =
        new BigQueryMapredRecordReader(mockRecordReader, 0);

    LongWritable w = recordReader.createKey();
    assertThat(w).isNotNull();

    JsonObject json = recordReader.createValue();
    assertThat(json).isNotNull();
  }

  @Test public void testGetPos() throws IOException, InterruptedException {
    BigQueryMapredRecordReader recordReader =
        new BigQueryMapredRecordReader(mockRecordReader, 1);

    when(mockRecordReader.getProgress()).thenReturn(256.0F);
    float f = recordReader.getPos();
    assertThat(f).isWithin(0.000001F).of(256.0F);

    verify(mockRecordReader).getProgress();
  }

  @Test public void testGetProgress() throws IOException, InterruptedException {
    BigQueryMapredRecordReader recordReader =
        new BigQueryMapredRecordReader(mockRecordReader, 0);

    // Happy-path is already tested by testGetPos

    when(mockRecordReader.getProgress()).thenThrow(new InterruptedException());
    expectedException.expect(IOException.class);
    try {
      recordReader.getProgress();
    } finally {
      verify(mockRecordReader).getProgress();
    }
  }

  @Test public void testNextData() throws IOException, InterruptedException {
    BigQueryMapredRecordReader recordReader =
        new BigQueryMapredRecordReader(mockRecordReader, 0);
    when(mockRecordReader.nextKeyValue()).thenReturn(true);
    when(mockRecordReader.getCurrentKey())
        .thenReturn(new LongWritable(123));
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("key1", "value1");
    when(mockRecordReader.getCurrentValue())
        .thenReturn(jsonObject);
    LongWritable key = new LongWritable(0);
    JsonObject value = new JsonObject();
    assertThat(recordReader.next(key, value)).isTrue();
    assertThat(key).isEqualTo(new LongWritable(123));
    assertThat(value).isEqualTo(jsonObject);

    verify(mockRecordReader).nextKeyValue();
    verify(mockRecordReader).getCurrentKey();
    verify(mockRecordReader).getCurrentValue();

    when(mockRecordReader.nextKeyValue()).thenThrow(new InterruptedException());
    expectedException.expect(IOException.class);
    try {
      recordReader.next(key, value);
    } finally {
      verify(mockRecordReader, times(2)).nextKeyValue();
    }
  }

  @Test public void testNextEof() throws IOException, InterruptedException {
    BigQueryMapredRecordReader recordReader =
        new BigQueryMapredRecordReader(mockRecordReader, 0);
    when(mockRecordReader.nextKeyValue())
        .thenReturn(false);
    LongWritable key = new LongWritable(0);
    JsonObject value = new JsonObject();
    assertThat(recordReader.next(key, value)).isFalse();

    verify(mockRecordReader).nextKeyValue();
  }

  @Test public void testCopyJsonObject() {
    JsonObject source = new JsonObject();
    source.addProperty("key1", "value1");
    source.addProperty("key2", 123);
    source.addProperty("key3", true);

    JsonObject destination = new JsonObject();
    destination.addProperty("key1", "different value");
    destination.addProperty("key4", "a value");

    assertThat(destination.has("key4")).isTrue();
    assertThat(source.equals(destination)).isFalse();

    BigQueryMapredRecordReader recordReader =
        new BigQueryMapredRecordReader(null, 0);
    recordReader.copyJsonObject(source, destination);

    assertThat(destination.has("key4")).isFalse();
    assertThat(source.equals(destination)).isTrue();
  }
}
