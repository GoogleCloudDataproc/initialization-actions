package com.google.cloud.hadoop.io.bigquery.mapred;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.gson.JsonObject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * Unit tests for {@link BigQueryMapredRecordWriter}.
 */
@RunWith(JUnit4.class)
public class BigQueryMapredRecordWriterTest {

  @Mock private org.apache.hadoop.mapreduce.RecordWriter<
      LongWritable, JsonObject> mockRecordWriter;
  @Mock private TaskAttemptContext mockTaskAttemptContext;

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Before public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After public void tearDown() {
    verifyNoMoreInteractions(mockRecordWriter);
    verifyNoMoreInteractions(mockTaskAttemptContext);
  }

  @Test public void testClose() throws IOException, InterruptedException {
    RecordWriter<LongWritable, JsonObject> recordWriter =
        new BigQueryMapredRecordWriter<LongWritable, JsonObject>(
        mockRecordWriter, mockTaskAttemptContext);
    Reporter reporter = null;   // unused by code under test

    recordWriter.close(reporter);
    verify(mockRecordWriter).close(any(TaskAttemptContext.class));

    doThrow(new IOException("test")).
      when(mockRecordWriter).close(any(TaskAttemptContext.class));
    expectedException.expect(IOException.class);
    try {
      recordWriter.close(reporter);
    } finally {
      verify(mockRecordWriter, times(2)).close(any(TaskAttemptContext.class));
    }
  }

  @Test public void testWrite() throws IOException, InterruptedException {
    RecordWriter<LongWritable, JsonObject> recordWriter =
        new BigQueryMapredRecordWriter<LongWritable, JsonObject>(
            mockRecordWriter, mockTaskAttemptContext);
    LongWritable key = new LongWritable(123);
    JsonObject value = new JsonObject();

    recordWriter.write(key, value);
    verify(mockRecordWriter).write(
        any(LongWritable.class), any(JsonObject.class));

    recordWriter.write(key, null);
    verify(mockRecordWriter, times(2)).write(
        any(LongWritable.class), any(JsonObject.class));

    doThrow(new IOException("test")).
      when(mockRecordWriter).write(
        any(LongWritable.class), any(JsonObject.class));
    expectedException.expect(IOException.class);
    try {
      recordWriter.write(key, value);
    } finally {
      verify(mockRecordWriter, times(3)).write(
        any(LongWritable.class), any(JsonObject.class));
    }
  }
}
