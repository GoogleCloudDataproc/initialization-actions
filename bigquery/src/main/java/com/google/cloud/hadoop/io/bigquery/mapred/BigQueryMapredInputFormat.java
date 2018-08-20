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

import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

/**
 * InputFormat that uses the old mapred API so that we can do
 * streaming input with our BigQuery connector.
 */
public class BigQueryMapredInputFormat
    implements InputFormat<LongWritable, JsonObject> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private org.apache.hadoop.mapreduce.InputFormat<LongWritable, JsonObject>
      mapreduceInputFormat = new GsonBigQueryInputFormat();
  private boolean dumpConfiguration = false;

  /**
   * Calls through to {@link GsonBigQueryInputFormat#getSplits}.
   *
   * @param job The config passed to us from the streaming package.
   * @param numSplits We ignore this parameter.
   */
  public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException {
    dumpConfiguration =
        job.getBoolean("mapred.bq.inputformat.configuration.dump", false);

    if (dumpConfiguration) {
      logger.atFine().log("getSplits has this JobConf: %s", job);
      java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
      job.writeXml(baos);
      logger.atFine().log("Dump of job: %s", baos);
    }

    JobContext jobContext = BigQueryMapredJobContext.from(job);
      // There is no equivalent to numSplits on mapreduce.JobContext.

    List<org.apache.hadoop.mapreduce.InputSplit> mapreduceSplits;
    try {
      mapreduceSplits = mapreduceInputFormat.getSplits(jobContext);
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
    if (mapreduceSplits == null) {
      return null;
    }

    // TODO(user): We need to figure out a way to call
    // mapreduceSplits.cleanupJob(jobContext) when we are done.
    // See b/13455083 for some ideas.

    InputSplit[] splits = new InputSplit[mapreduceSplits.size()];
    int ii = 0;
    for (org.apache.hadoop.mapreduce.InputSplit mapreduceSplit :
        mapreduceSplits) {
      logger.atFine().log("Split[%s] = %s", ii, mapreduceSplit);
      splits[ii++] = new BigQueryMapredInputSplit(mapreduceSplit);
    }
    return splits;
  }

  /**
   * Get a RecordReader by calling through to {@link GsonBigQueryInputFormat#createRecordReader}.
   */
  public RecordReader<LongWritable, JsonObject> getRecordReader(
      InputSplit inputSplit, JobConf conf, Reporter reporter)
      throws IOException {
    Preconditions.checkArgument(
        inputSplit instanceof BigQueryMapredInputSplit,
        "Split must be an instance of BigQueryMapredInputSplit");
    try {
      // The assertion is that this taskAttemptId isn't actually used, but in Hadoop2 calling
      // toString() on an emptyJobID results in an NPE.
      TaskAttemptID taskAttemptId = new TaskAttemptID(new TaskID(
          new JobID("", 1), true, 1), 1);
      TaskAttemptContext context =
          ReflectedTaskAttemptContextFactory.getContext(conf, taskAttemptId);
      org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit =
          ((BigQueryMapredInputSplit) inputSplit).getMapreduceInputSplit();
      logger.atFine().log(
          "mapreduceInputSplit is %s, class is %s",
          mapreduceInputSplit, mapreduceInputSplit.getClass().getName());
      org.apache.hadoop.mapreduce.RecordReader<LongWritable, JsonObject>
          mapreduceRecordReader = mapreduceInputFormat.createRecordReader(
              mapreduceInputSplit, context);
      mapreduceRecordReader.initialize(mapreduceInputSplit, context);
      long splitLength = inputSplit.getLength();
      return new BigQueryMapredRecordReader(mapreduceRecordReader, splitLength);
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
  }

  @VisibleForTesting
  void setMapreduceInputFormat(
      org.apache.hadoop.mapreduce.InputFormat<LongWritable, JsonObject>
          inputFormat) {
    this.mapreduceInputFormat = inputFormat;
  }
}
