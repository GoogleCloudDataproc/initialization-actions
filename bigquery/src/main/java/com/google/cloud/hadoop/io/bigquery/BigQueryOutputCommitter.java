package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.HadoopToStringUtil;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * An OutputCommitter that commits tables specified in job output dataset in Bigquery. This is
 * called before job start, after task completion, job completion, task cancellation, and job
 * abortion.
 */
public class BigQueryOutputCommitter
    extends OutputCommitter {
  // Logger.
  protected static final LogUtil log = new LogUtil(BigQueryOutputCommitter.class);

  // Used for specialized handling of various API-defined exceptions.
  private ApiErrorExtractor errorExtractor = new ApiErrorExtractor();

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
      log.error("Could not get Bigquery", e);
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
  public void setupJob(JobContext context)
      throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("setupJob(%s)", HadoopToStringUtil.toString(context));
    }
    // Create dataset.
    DatasetReference datasetReference = new DatasetReference();
    datasetReference.setProjectId(tempTableRef.getProjectId());
    datasetReference.setDatasetId(tempTableRef.getDatasetId());

    Dataset tempDataset = new Dataset();
    tempDataset.setDatasetReference(datasetReference);

    // Insert dataset into Bigquery.
    Bigquery.Datasets datasets = bigQueryHelper.getRawBigquery().datasets();

    // TODO(user): Maybe allow the dataset to exist already instead of throwing 409 here.
    log.debug("Creating temporary dataset '%s' for project '%s'",
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
  public void cleanupJob(JobContext context)
      throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("cleanupJob(%s)", HadoopToStringUtil.toString(context));
    }
    Bigquery.Datasets datasets = bigQueryHelper.getRawBigquery().datasets();
    Configuration config = context.getConfiguration();
    try {
      log.debug("cleanupJob: Deleting dataset '%s' from project '%s'",
          tempTableRef.getDatasetId(), tempTableRef.getProjectId());
      datasets.delete(tempTableRef.getProjectId(), tempTableRef.getDatasetId())
          .setDeleteContents(true)
          .execute();
    } catch (IOException e) {
      // Error is swallowed as job has completed successfully and the only failure is deleting
      // temporary data.
      // This matches the FileOutputCommitter pattern.
      log.warn("Could not delete dataset. Temporary data not cleaned up.", e);
    }
  }

  /**
   * For cleaning up the job's output after job failure.
   *
   * @param jobContext Context of the job whose output is being written.
   * @param status Final run state of the job, should be JobStatus.KILLED or JobStatus.FAILED.
   * @throws IOException on IO Error.
   */
  public void abortJob(JobContext jobContext, int status)
      throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("abortJob(%s, %d)", HadoopToStringUtil.toString(jobContext), status);
    }
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
  public void commitJob(JobContext jobContext)
      throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("commitJob(%s)", HadoopToStringUtil.toString(jobContext));
    }
    cleanupJob(jobContext);
  }

  /**
   * No task setup required.
   *
   * @throws IOException on IO Error.
   */
  @Override
  public void setupTask(TaskAttemptContext context)
      throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("setupTask(%s)", HadoopToStringUtil.toString(context));
    }
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
  public void commitTask(TaskAttemptContext context)
      throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("commitTask(%s)", HadoopToStringUtil.toString(context));
    }

    // Create a table copy request object.
    JobConfigurationTableCopy copyTableConfig = new JobConfigurationTableCopy();

    // Set the table to get results from.
    copyTableConfig.setSourceTable(tempTableRef);

    // Set the table to put results into.
    copyTableConfig.setDestinationTable(finalTableRef);

    copyTableConfig.setWriteDisposition("WRITE_APPEND");

    JobConfiguration config = new JobConfiguration();
    config.setCopy(copyTableConfig);

    JobReference jobReference = bigQueryHelper.createJobReference(
        projectId, context.getTaskAttemptID().toString());

    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Run the job.
    log.debug("commitTask: Running table copy from %s to %s",
        BigQueryStrings.toString(tempTableRef), BigQueryStrings.toString(finalTableRef));
    Insert insert = bigQueryHelper.getRawBigquery().jobs().insert(projectId, job);
    try {
      Job response = insert.execute();
      bigQueryHelper.checkJobIdEquality(job, response);
    } catch (IOException ioe) {
      if (errorExtractor.itemAlreadyExists(ioe)) {
        log.info(String.format(
            "Continuing normally after catching exception for duplicate jobId '%s'",
            jobReference.getJobId(), ioe));
      } else {
        throw ioe;
      }
    }

    // Poll until job is complete.
    try {
      BigQueryUtils.waitForJobCompletion(
          bigQueryHelper.getRawBigquery(), projectId, jobReference, context);
    } catch (InterruptedException e) {
      log.error("Could not check if results of task were transfered.", e);
      throw new IOException("Could not check if results of task were transfered.", e);
    }
    log.info("Saved output of task to table '%s' using project '%s'",
        BigQueryStrings.toString(finalTableRef), projectId);
  }

  /**
   * Deletes the work table.
   *
   * @param context the task's context.
   */
  @Override
  public void abortTask(TaskAttemptContext context) {
    if (log.isDebugEnabled()) {
      log.debug("abortTask(%s)", HadoopToStringUtil.toString(context));
    }
    // Cleanup of per-task temporary tables will be performed at job cleanup time.
  }

  /**
   * Did this task write any files into the working dataset?
   *
   * @param context the task's context.
   * @throws IOException on IO Error.
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context)
      throws IOException {
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
    if (log.isDebugEnabled()) {
      log.debug("needsTaskCommit(%s) - tempTableRef: '%s'",
          attemptId,
          BigQueryStrings.toString(tempTableRef));
    }

    boolean tableExists = bigQueryHelper.tableExists(tempTableRef);
    log.debug("needsTaskCommit -> %s", tableExists);
    return tableExists;
  }

  /**
   * Sets Bigquery for testing purposes.
   */
  @VisibleForTesting
  void setBigQueryHelper(BigQueryHelper helper) {
    this.bigQueryHelper = helper;
  }

  @VisibleForTesting
  void setErrorExtractor(ApiErrorExtractor errorExtractor) {
    this.errorExtractor = errorExtractor;
  }
}
