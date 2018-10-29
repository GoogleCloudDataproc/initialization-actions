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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.ShardedInputSplit;
import com.google.gson.JsonObject;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link BigQueryMapredInputFormat}.
 */
@RunWith(JUnit4.class)
public class BigQueryMapredInputFormatTest {

  static class TestSplit extends org.apache.hadoop.mapreduce.InputSplit
      implements Writable {
    private long length;
    private String[] locations;

    public TestSplit(long length, String[] locations) {
      this.length = length;
      this.locations = locations;
    }

    public long getLength() {
      return length;
    }

    public String[] getLocations() {
      return locations;
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(length);
      out.writeInt(locations.length);
      for (String location : locations) {
        out.writeUTF(location);
      }
    }

    public void readFields(DataInput in) throws IOException {
      length = in.readLong();
      int numLocations = in.readInt();
      locations = new String[numLocations];
      for (int ii = 0; ii < numLocations; ii++) {
        locations[ii] = in.readUTF();
      }
    }
  }
  @Mock private org.apache.hadoop.mapreduce.InputFormat<
      LongWritable, JsonObject> mockInputFormat;

  @Before public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test public void testGetSplits() throws IOException, InterruptedException {
    BigQueryMapredInputFormat inputFormat = new BigQueryMapredInputFormat();
    inputFormat.setMapreduceInputFormat(mockInputFormat);
    List<org.apache.hadoop.mapreduce.InputSplit> mapreduceSplits =
          new ArrayList<>();
    mapreduceSplits.add(new TestSplit(123, new String[]{"a", "b"}));
    mapreduceSplits.add(new TestSplit(456, new String[]{"x", "y", "z"}));
    when(mockInputFormat.getSplits(any(JobContext.class)))
        .thenReturn(mapreduceSplits);

    JobConf jobConf = new JobConf();
    jobConf.set("mapreduce.job.dir", "/a/path/job_1_2");
    int numSplits = 0;  // not used by the code under test
    InputSplit[] splits = inputFormat.getSplits(jobConf, numSplits);
    assertThat(splits).hasLength(2);
    assertThat(splits[0].getLength()).isEqualTo(123);
    assertThat(splits[0].getLocations()).hasLength(2);
    assertThat(splits[0].getLocations()[0]).isEqualTo("a");
    assertThat(splits[1].getLength()).isEqualTo(456);
    assertThat(splits[1].getLocations()).hasLength(3);
  }

  @Test public void testGetSplitsNull()
      throws IOException, InterruptedException {
    BigQueryMapredInputFormat inputFormat = new BigQueryMapredInputFormat();
    inputFormat.setMapreduceInputFormat(mockInputFormat);
    JobConf jobConf = new JobConf();
    jobConf.set("mapreduce.job.dir", "/a/path/job_1_2");
    int numSplits = 0;  // not used by the code under test
    when(mockInputFormat.getSplits(any(JobContext.class)))
        .thenReturn(null);
    InputSplit[] splits = inputFormat.getSplits(jobConf, numSplits);
    assertThat(splits).isNull();
  }

  @Test public void testGetSplitsException()
      throws IOException, InterruptedException {
    BigQueryMapredInputFormat inputFormat = new BigQueryMapredInputFormat();
    inputFormat.setMapreduceInputFormat(mockInputFormat);
    JobConf jobConf = new JobConf();
    jobConf.set("mapreduce.job.dir", "/a/path/job_1_2");
    int numSplits = 0;  // not used by the code under test
    when(mockInputFormat.getSplits(any(JobContext.class)))
        .thenThrow(new InterruptedException("test"));

    jobConf.set("mapred.bq.inputformat.configuration.dump", "true");

    assertThrows(IOException.class, () -> inputFormat.getSplits(jobConf, numSplits));
  }

  @Test public void testGetRecordReader() throws IOException {
    BigQueryMapredInputFormat inputFormat = new BigQueryMapredInputFormat();
    Path path = new Path("testpath");
    ShardedInputSplit testSplit =
        new ShardedInputSplit(path, 3);
    InputSplit inputSplit = new BigQueryMapredInputSplit(testSplit);
    JobConf jobConf = new JobConf();
    jobConf.set("mapreduce.job.dir", "/a/path/job_1_2");
    jobConf.set(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, "true");
    Reporter reporter = null; // not used by the code under test
    RecordReader<LongWritable, JsonObject> recordReader =
        inputFormat.getRecordReader(inputSplit, jobConf, reporter);
    assertThat(recordReader).isInstanceOf(BigQueryMapredRecordReader.class);
  }

  @Test public void testGetRecordReaderException()
      throws IOException, InterruptedException {
    BigQueryMapredInputFormat inputFormat = new BigQueryMapredInputFormat();
    inputFormat.setMapreduceInputFormat(mockInputFormat);
    Path path = new Path("testpath");
    ShardedInputSplit testSplit =
        new ShardedInputSplit(path, 3);
    InputSplit inputSplit = new BigQueryMapredInputSplit(testSplit);
    JobConf jobConf = new JobConf();
    jobConf.set("mapreduce.job.dir", "/a/path/job_1_2");
    Reporter reporter = null; // not used by the code under test
    when(mockInputFormat.createRecordReader(
            any(org.apache.hadoop.mapreduce.InputSplit.class),
            any(TaskAttemptContext.class)))
        .thenThrow(new InterruptedException("test"));
    assertThrows(
        IOException.class, () -> inputFormat.getRecordReader(inputSplit, jobConf, reporter));
  }
}
