package com.google.cloud.hadoop.io.bigquery.mapred;

import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;

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

import java.io.IOException;
import java.util.List;

/**
 * InputFormat that uses the old mapred API so that we can do
 * streaming input with our BigQuery connector.
 */
public class BigQueryMapredInputFormat
    implements InputFormat<LongWritable, JsonObject> {

  protected static final LogUtil log =
      new LogUtil(BigQueryMapredInputFormat.class);

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
      log.debug("getSplits has this JobConf: %s", job);
      java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
      job.writeXml(baos);
      log.debug("Dump of job: %s", baos);
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
      log.debug("Split[%d] = %s", ii, mapreduceSplit);
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
      log.debug("mapreduceInputSplit is %s, class is %s",
          mapreduceInputSplit,
          mapreduceInputSplit.getClass().getName());
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
