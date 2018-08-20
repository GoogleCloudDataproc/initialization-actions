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
package com.google.cloud.hadoop.io.bigquery;

import static com.google.common.flogger.LazyArgs.lazy;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.util.HadoopToStringUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * An OutputCommitter that commits tables specified in job output dataset in Bigquery. This is
 * called before job start, after task completion, job completion, task cancellation, and job
 * abortion.
 */
public class BigQueryOutputCommitter extends OutputCommitter {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Id of project used to describe the project under which all connector operations occur.
  private String projectId;

  // Fully-qualified id of the temporary table the connector writes into.
  private TableReference tempTableRef;

  // Fully-qualified id of the final destination table we desire the output to go to.
  private TableReference finalTableRef;

  // Wrapper around some Bigquery API methods and convenience methods.
  private BigQueryHelper bigQueryHelper;

  /**
   * Creates a bigquery output committer.
   *
   * @param projectId the job's project id.
   * @param tempTableRef the fully-qualified temp table to write to.
   * @param finalTableRef the fully-qualified destination table on commit.
   * @param configuration the task's configuration
   * @throws IOException on IO Error.
   */
  public BigQueryOutputCommitter(
      String projectId, TableReference tempTableRef,
      TableReference finalTableRef, Configuration configuration)
      throws IOException {
    this.projectId = projectId;
    this.tempTableRef = tempTableRef;
    this.finalTableRef = finalTableRef;
    // Get Bigquery.
    try {
      BigQueryFactory bigQueryFactory = new BigQueryFactory();
      this.bigQueryHelper = bigQueryFactory.getBigQueryHelper(configuration);
    } catch (GeneralSecurityException e) {
      throw new IOException("Could not get Bigquery", e);
    }
  }

  /**
   * Creates the temporary dataset that will contain all of the task work tables.
   *
   * @param context the job's context.
   * @throws IOException on IO Error.
   */
  @Override
  public void setupJob(JobContext context) throws IOException {
    logger.atFine().log("setupJob(%s)", lazy(() -> HadoopToStringUtil.toString(context)));
    // Create dataset.
    DatasetReference datasetReference = new DatasetReference();
    datasetReference.setProjectId(tempTableRef.getProjectId());
    datasetReference.setDatasetId(tempTableRef.getDatasetId());

    Dataset tempDataset = new Dataset();
    tempDataset.setDatasetReference(datasetReference);
    tempDataset.setLocation(getLocation(context));

    // Insert dataset into Bigquery.
    Bigquery.Datasets datasets = bigQueryHelper.getRawBigquery().datasets();

    // TODO(user): Maybe allow the dataset to exist already instead of throwing 409 here.
    logger.atFine().log(
        "Creating temporary dataset '%s' for project '%s'",
        tempTableRef.getDatasetId(), tempTableRef.getProjectId());

    // NB: Even though this "insert" makes it look like we can specify a different projectId than
    // the one which owns the dataset, it actually has to match.
    datasets.insert(tempTableRef.getProjectId(), tempDataset).execute();
  }

  /**
   * Deletes the temporary dataset, including all of the work tables.
   *
   * @param context the job's context.
   * @throws IOException
   */
  @Override
  public void cleanupJob(JobContext context) throws IOException {
    logger.atFine().log("cleanupJob(%s)", lazy(() -> HadoopToStringUtil.toString(context)));
    Bigquery.Datasets datasets = bigQueryHelper.getRawBigquery().datasets();
    try {
      logger.atFine().log(
          "cleanupJob: Deleting dataset '%s' from project '%s'",
          tempTableRef.getDatasetId(), tempTableRef.getProjectId());
      datasets.delete(tempTableRef.getProjectId(), tempTableRef.getDatasetId())
          .setDeleteContents(true)
          .execute();
    } catch (IOException e) {
      // Error is swallowed as job has completed successfully and the only failure is deleting
      // temporary data.
      // This matches the FileOutputCommitter pattern.
      logger.atWarning().withCause(e).log(
          "Could not delete dataset. Temporary data not cleaned up.");
    }
  }

  /**
   * For cleaning up the job's output after job failure.
   *
   * @param jobContext Context of the job whose output is being written.
   * @param status Final run state of the job, should be JobStatus.KILLED or JobStatus.FAILED.
   * @throws IOException on IO Error.
   */
  public void abortJob(JobContext jobContext, int status) throws IOException {
    logger.atFine().log(
        "abortJob(%s, %s)", lazy(() -> HadoopToStringUtil.toString(jobContext)), status);
    cleanupJob(jobContext);
  }

  /**
   * For committing job's output after successful job completion. Note that this is invoked for jobs
   * with final run state as JobStatus.SUCCEEDED.
   *
   * @param jobContext Context of the job whose output is being written.
   * @throws IOException on IO Error.
   */
  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    logger.atFine().log("commitJob(%s)", lazy(() -> HadoopToStringUtil.toString(jobContext)));
    cleanupJob(jobContext);
  }

  /**
   * No task setup required.
   *
   * @throws IOException on IO Error.
   */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    logger.atFine().log("setupTask(%s)", lazy(() -> HadoopToStringUtil.toString(context)));
    // BigQueryOutputCommitter's setupTask doesn't do anything. Because the
    // temporary task table is created on demand when the
    // task is writing.
  }

  /**
   * Moves the files from the working dataset to the job output table.
   *
   * @param context the task context.
   * @throws IOException on IO Error.
   */
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    logger.atFine().log("commitTask(%s)", lazy(() -> HadoopToStringUtil.toString(context)));

    // Create a table copy request object.
    JobConfigurationTableCopy copyTableConfig = new JobConfigurationTableCopy();

    // Set the table to get results from.
    copyTableConfig.setSourceTable(tempTableRef);

    // Set the table to put results into.
    copyTableConfig.setDestinationTable(finalTableRef);

    copyTableConfig.setWriteDisposition("WRITE_APPEND");

    JobConfiguration config = new JobConfiguration();
    config.setCopy(copyTableConfig);

    JobReference jobReference =
        bigQueryHelper.createJobReference(
            projectId, context.getTaskAttemptID().toString(), getLocation(context));

    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Run the job.
    logger.atFine().log(
        "commitTask: Running table copy from %s to %s",
        BigQueryStrings.toString(tempTableRef), BigQueryStrings.toString(finalTableRef));
    Job response = bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);
    logger.atFine().log("Got response '%s'", response);

    // Poll until job is complete.
    try {
      BigQueryUtils.waitForJobCompletion(
          bigQueryHelper.getRawBigquery(), projectId, jobReference, context);
    } catch (InterruptedException e) {
      throw new IOException("Could not check if results of task were transferred.", e);
    }
    logger.atInfo().log(
        "Saved output of task to table '%s' using project '%s'",
        BigQueryStrings.toString(finalTableRef), projectId);
  }

  /**
   * Deletes the work table.
   *
   * @param context the task's context.
   */
  @Override
  public void abortTask(TaskAttemptContext context) {
    logger.atFine().log("abortTask(%s)", lazy(() -> HadoopToStringUtil.toString(context)));
    // Cleanup of per-task temporary tables will be performed at job cleanup time.
  }

  /**
   * Did this task write any files into the working dataset?
   *
   * @param context the task's context.
   * @throws IOException on IO Error.
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return needsTaskCommit(context.getTaskAttemptID());
  }

  /**
   * Did this task write any files into the working dataset?
   *
   * @param attemptId the task's context.
   * @throws IOException on IO Error.
   */
  @VisibleForTesting
  public boolean needsTaskCommit(TaskAttemptID attemptId) throws IOException {
    logger.atFine().log(
        "needsTaskCommit(%s) - tempTableRef: '%s'",
        attemptId, lazy(() -> BigQueryStrings.toString(tempTableRef)));

    boolean tableExists = bigQueryHelper.tableExists(tempTableRef);
    logger.atFine().log("needsTaskCommit -> %s", tableExists);
    return tableExists;
  }

  /**
   * Sets Bigquery for testing purposes.
   */
  @VisibleForTesting
  void setBigQueryHelper(BigQueryHelper helper) {
    this.bigQueryHelper = helper;
  }

  private String getLocation(JobContext context) {
    Configuration config = context.getConfiguration();
    return config.get(
        BigQueryConfiguration.DATA_LOCATION_KEY, BigQueryConfiguration.DATA_LOCATION_DEFAULT);
  }
}
