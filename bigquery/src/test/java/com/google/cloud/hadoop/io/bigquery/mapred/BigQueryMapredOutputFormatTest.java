package com.google.cloud.hadoop.io.bigquery.mapred;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.gson.JsonObject;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/**
 * Unit tests for {@link BigQueryMapredOutputFormat}.
 */
@RunWith(JUnit4.class)
public class BigQueryMapredOutputFormatTest {

  @Mock private FileSystem mockFileSystem;
  @Mock private org.apache.hadoop.mapreduce.OutputFormat<
      LongWritable, JsonObject> mockOutputFormat;
  @Mock private org.apache.hadoop.mapreduce.RecordWriter<
      LongWritable, JsonObject> mockMapreduceRecordWriter;
  @Mock private Progressable mockProgressable;

  @Before public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After public void tearDown() {
    verifyNoMoreInteractions(mockFileSystem);
    verifyNoMoreInteractions(mockOutputFormat);
    verifyNoMoreInteractions(mockMapreduceRecordWriter);
    verifyNoMoreInteractions(mockProgressable);
  }

  @Test public void testCheckOutputSpecs()
      throws IOException, InterruptedException {
    BigQueryMapredOutputFormat<LongWritable, JsonObject> outputFormat =
        new BigQueryMapredOutputFormat<LongWritable, JsonObject>();
    outputFormat.setMapreduceOutputFormat(mockOutputFormat);
    doNothing().when(mockOutputFormat).checkOutputSpecs(any(JobContext.class));

    JobConf jobConf = new JobConf();
    jobConf.set("mapreduce.job.dir", "/a/path/job_1_2");
    outputFormat.checkOutputSpecs(mockFileSystem, jobConf);

    verify(mockOutputFormat).checkOutputSpecs(any(JobContext.class));
  }

  @Test public void testGetRecordWriter()
      throws IOException, InterruptedException {
    BigQueryMapredOutputFormat<LongWritable, JsonObject> outputFormat =
        new BigQueryMapredOutputFormat<LongWritable, JsonObject>();
    outputFormat.setMapreduceOutputFormat(mockOutputFormat);
    when(mockOutputFormat.getRecordWriter(any(TaskAttemptContext.class))).
        thenReturn(mockMapreduceRecordWriter);

    JobConf jobConf = new JobConf();
    String taskId = "attempt_201401010000_0000_r_000000_0";
    jobConf.set("mapreduce.job.dir", "/a/path/job_1_2");
    jobConf.set("mapred.task.id", taskId);
    String name = "foo";
    RecordWriter<LongWritable, JsonObject> recordWriter =
        outputFormat.getRecordWriter(
            mockFileSystem, jobConf, name, mockProgressable);

    assertNotNull(recordWriter);
    verify(mockOutputFormat).getRecordWriter(any(TaskAttemptContext.class));
  }
}
