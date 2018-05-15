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

import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OutputFormat that uses the old mapred API so that we can do
 * streaming output with our BigQuery connector.
 */
class BigQueryMapredOutputFormat<K, V> implements OutputFormat<K, V> {

  protected static final Logger LOG =
      LoggerFactory.getLogger(BigQueryMapredOutputFormat.class);

  private org.apache.hadoop.mapreduce.OutputFormat<K, JsonObject>
      mapreduceOutputFormat = new BigQueryOutputFormat<K, JsonObject>();

  public BigQueryMapredOutputFormat() {
    LOG.debug("BigQueryMapredOutputFormat created");
  }

  public void checkOutputSpecs(FileSystem ignored, JobConf job)
      throws IOException {
    LOG.debug("checkOutputSpecs");
    JobContext jobContext = BigQueryMapredJobContext.from(job);
    try {
      mapreduceOutputFormat.checkOutputSpecs(jobContext);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {
    // We assume the name is the task ID.
    String taskId = job.get("mapred.task.id");
    Preconditions.checkArgument(taskId != null, "mapred.task.id must be set");
    LOG.debug("getRecordWriter name={}, mapred.task.id={}", name, taskId);
    TaskAttemptID taskAttemptId = TaskAttemptID.forName(taskId);
    LOG.debug("TaskAttemptId={}", taskAttemptId);
    TaskAttemptContext context =
        ReflectedTaskAttemptContextFactory.getContext(job, taskAttemptId);
    org.apache.hadoop.mapreduce.RecordWriter<K, JsonObject>
        mapreduceRecordWriter;
    try {
      mapreduceRecordWriter = mapreduceOutputFormat.getRecordWriter(context);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
    return new BigQueryMapredRecordWriter<K, V>(mapreduceRecordWriter, context);
  }

  @VisibleForTesting
  void setMapreduceOutputFormat(
      org.apache.hadoop.mapreduce.OutputFormat<K, JsonObject> outputFormat) {
    this.mapreduceOutputFormat = outputFormat;
  }
}
