/*
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

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.gson.JsonObject;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link BigQueryMapredRecordWriter}. */
@RunWith(JUnit4.class)
public class BigQueryMapredRecordWriterTest {

  @Mock private org.apache.hadoop.mapreduce.RecordWriter<LongWritable, JsonObject> mockRecordWriter;
  @Mock private TaskAttemptContext mockTaskAttemptContext;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockRecordWriter);
    verifyNoMoreInteractions(mockTaskAttemptContext);
  }

  @Test
  public void testClose() throws IOException, InterruptedException {
    RecordWriter<LongWritable, JsonObject> recordWriter =
        new BigQueryMapredRecordWriter<LongWritable, JsonObject>(
            mockRecordWriter, mockTaskAttemptContext);
    Reporter reporter = null; // unused by code under test

    recordWriter.close(reporter);
    verify(mockRecordWriter).close(any(TaskAttemptContext.class));

    doThrow(new IOException("test")).when(mockRecordWriter).close(any(TaskAttemptContext.class));
    assertThrows(IOException.class, () -> recordWriter.close(reporter));

    verify(mockRecordWriter, times(2)).close(any(TaskAttemptContext.class));
  }

  @Test
  public void testWrite() throws IOException, InterruptedException {
    RecordWriter<LongWritable, JsonObject> recordWriter =
        new BigQueryMapredRecordWriter<LongWritable, JsonObject>(
            mockRecordWriter, mockTaskAttemptContext);
    LongWritable key = new LongWritable(123);
    JsonObject value = new JsonObject();

    recordWriter.write(key, value);

    verify(mockRecordWriter).write(eq(key), eq(value));
  }

  @Test
  public void testWrite_nullValue() throws IOException, InterruptedException {
    RecordWriter<LongWritable, JsonObject> recordWriter =
        new BigQueryMapredRecordWriter<LongWritable, JsonObject>(
            mockRecordWriter, mockTaskAttemptContext);
    LongWritable key = new LongWritable(123);

    recordWriter.write(key, null);

    verify(mockRecordWriter).write(eq(key), eq(null));
  }

  @Test
  public void testWrite_throwException() throws IOException, InterruptedException {
    RecordWriter<LongWritable, JsonObject> recordWriter =
        new BigQueryMapredRecordWriter<LongWritable, JsonObject>(
            mockRecordWriter, mockTaskAttemptContext);
    LongWritable key = new LongWritable(123);
    JsonObject value = new JsonObject();

    doThrow(new IOException("test"))
        .when(mockRecordWriter)
        .write(any(LongWritable.class), any(JsonObject.class));
    assertThrows(IOException.class, () -> recordWriter.write(key, value));

    verify(mockRecordWriter).write(eq(key), eq(value));
  }
}
