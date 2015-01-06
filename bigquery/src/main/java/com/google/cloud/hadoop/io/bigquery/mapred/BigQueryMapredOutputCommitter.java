package com.google.cloud.hadoop.io.bigquery.mapred;

import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobStatus.State;

import java.io.IOException;


/**
 * OutputCommitter that uses the old mapred API so that we can do
 * streaming output with our BigQuery connector.
 */
class BigQueryMapredOutputCommitter extends OutputCommitter {

  /**
   * Mapping between JobStatus integer codes and JobStatus.State enum members.
   */
  public static final ImmutableMap<Integer, State> STATUS_TO_STATE =
      ImmutableMap.<Integer, State>builder()
          .put(JobStatus.RUNNING, State.RUNNING)
          .put(JobStatus.SUCCEEDED, State.SUCCEEDED)
          .put(JobStatus.FAILED, State.FAILED)
          .put(JobStatus.PREP, State.PREP)
          .put(JobStatus.KILLED, State.KILLED)
          .build();

  protected static final LogUtil log =
      new LogUtil(BigQueryMapredOutputCommitter.class);

  private org.apache.hadoop.mapreduce.OutputCommitter mapreduceOutputCommitter;

  public BigQueryMapredOutputCommitter() {
    // We need to create a BigQueryOutputCommitter, but we don't have
    // enough info to do that until we have the TaskAttemptContext,
    // so wait until then to create it.
    log.debug("BigQueryMapredOutputCommitter created");
  }

  // OutputCommitter methods

  @Override
  public void abortJob(JobContext jobContext, int status) throws IOException {
    log.debug("abortJob");
    Preconditions.checkState(mapreduceOutputCommitter != null,
        "mapreduceOutputCommitter must be initialized before abortJob");
    State state;
    if (STATUS_TO_STATE.containsKey(status)) {
      state = STATUS_TO_STATE.get(status);
    } else {
      throw new IllegalArgumentException("Bad value for status: " + status);
    }
    mapreduceOutputCommitter.abortJob(jobContext, state);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    log.debug("abortTask");
    initMapreduceOutputCommitter(taskContext);
    mapreduceOutputCommitter.abortTask(taskContext);
  }

  @Override
  public void cleanupJob(JobContext jobContext) throws IOException {
    log.debug("cleanupJob");
    Preconditions.checkState(mapreduceOutputCommitter != null,
        "mapreduceOutputCommitter must be initialized before cleanupJob");
    mapreduceOutputCommitter.cleanupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    log.debug("commitJob");
    Preconditions.checkState(mapreduceOutputCommitter != null,
        "mapreduceOutputCommitter must be initialized before commitJob");
    mapreduceOutputCommitter.commitJob(jobContext);
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    log.debug("commitTask");
    initMapreduceOutputCommitter(taskContext);
    mapreduceOutputCommitter.commitTask(taskContext);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    log.debug("needsTaskCommit");
    initMapreduceOutputCommitter(taskContext);
    return mapreduceOutputCommitter.needsTaskCommit(taskContext);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    log.debug("setupJob");
    Preconditions.checkState(mapreduceOutputCommitter != null,
        "mapreduceOutputCommitter must be initialized before setupJob");
    mapreduceOutputCommitter.setupJob(jobContext);
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    log.debug("setupTask");
    initMapreduceOutputCommitter(taskContext);
    mapreduceOutputCommitter.setupTask(taskContext);
  }

  // Initialize our mapreduce OutputCommitter the first time we are called.
  private void initMapreduceOutputCommitter(TaskAttemptContext taskContext)
      throws IOException {
    if (mapreduceOutputCommitter != null) {
      log.debug("Using existing mapreduceOutputCommitter");
      return;
    }

    // It would be nice to use the BigQueryOutputFormat that already exists
    // (there is one wrapped inside our BigQueryMapredOutputFormat), but
    // there does not seem to be an easy way to do that. So make another one.
    log.debug("Creating BigQueryOutputFormat");
    BigQueryOutputFormat<Object, JsonObject> mapreduceOutputFormat =
        new BigQueryOutputFormat<Object, JsonObject>();

    // Fortunately, mapred.TaskAttemptContext is a subclass of
    // mapreduce.TaskAttemptContext, so we can use it directly.
    try {
      log.debug("Creating mapreduce OutputCommit");
      mapreduceOutputCommitter = mapreduceOutputFormat.getOutputCommitter(
          taskContext);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  @VisibleForTesting
  void setMapreduceOutputCommitter(
      org.apache.hadoop.mapreduce.OutputCommitter outputCommitter) {
    this.mapreduceOutputCommitter = outputCommitter;
  }
}
