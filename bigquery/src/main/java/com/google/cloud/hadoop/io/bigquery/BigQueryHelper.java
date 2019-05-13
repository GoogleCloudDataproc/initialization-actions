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

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.ExternalDataConfiguration;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Wrapper for BigQuery API.
 */
public class BigQueryHelper {
  // BigQuery job_ids must match this pattern.
  public static final String BIGQUERY_JOB_ID_PATTERN = "[a-zA-Z0-9_-]+";

  // Maximum number of characters in a BigQuery job_id.
  public static final int BIGQUERY_JOB_ID_MAX_LENGTH = 1024;

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Used for specialized handling of various API-defined exceptions.
  private ApiErrorExtractor errorExtractor = ApiErrorExtractor.INSTANCE;

  private Bigquery service;

  public BigQueryHelper(Bigquery service) {
    this.service = service;
  }

  /**
   * Returns the underlying Bigquery instance used for communicating with the BigQuery API.
   */
  public Bigquery getRawBigquery() {
    return service;
  }

  /**
   * Performs a federated import on data from GCS into BigQuery via a table insert.
   *
   * @param projectId the project on whose behalf to perform the load.
   * @param tableRef the reference to the destination table.
   * @param schema the schema of the source data to populate the destination table by.
   * @param sourceFormat the file format of the source data.
   * @param gcsPaths the location of the source data in GCS.
   * @throws IOException
   */
  public void importFederatedFromGcs(
      String projectId,
      TableReference tableRef,
      @Nullable TableSchema schema,
      BigQueryFileFormat sourceFormat,
      List<String> gcsPaths)
      throws IOException {
    logger.atInfo().log(
        "Importing into federated table '%s' from %s paths; path[0] is '%s'",
        BigQueryStrings.toString(tableRef),
        gcsPaths.size(),
        gcsPaths.isEmpty() ? "(empty)" : gcsPaths.get(0));

    ExternalDataConfiguration externalConf = new ExternalDataConfiguration();
    externalConf.setSchema(schema);
    externalConf.setSourceUris(gcsPaths);
    externalConf.setSourceFormat(sourceFormat.getFormatIdentifier());

    // Auto detect the schema if we're not given one, otherwise use the passed schema.
    if (schema == null) {
      logger.atInfo().log("No federated import schema provided, auto detecting schema.");
      externalConf.setAutodetect(true);
    } else {
      logger.atInfo().log("Using provided federated import schema '%s'.", schema.toString());
    }

    Table table = new Table();
    table.setTableReference(tableRef);
    table.setExternalDataConfiguration(externalConf);

    service.tables().insert(projectId, tableRef.getDatasetId(), table).execute();
  }

  /**
   * Imports data from GCS into BigQuery via a load job. Optionally polls for completion before
   * returning.
   *
   * @param projectId the project on whose behalf to perform the load.
   * @param tableRef the reference to the destination table.
   * @param schema the schema of the source data to populate the destination table by.
   * @param kmsKeyName the Cloud KMS encryption key used to protect the output table.
   * @param sourceFormat the file format of the source data.
   * @param writeDisposition the write disposition of the output table.
   * @param gcsPaths the location of the source data in GCS.
   * @param awaitCompletion if true, block and poll until job completes, otherwise return as soon as
   *     the job has been successfully dispatched.
   * @throws IOException
   * @throws InterruptedException if interrupted while waiting for job completion.
   */
  public void importFromGcs(
      String projectId,
      TableReference tableRef,
      @Nullable TableSchema schema,
      @Nullable String kmsKeyName,
      BigQueryFileFormat sourceFormat,
      String writeDisposition,
      List<String> gcsPaths,
      boolean awaitCompletion)
      throws IOException, InterruptedException {
    logger.atInfo().log(
        "Importing into table '%s' from %s paths; path[0] is '%s'; awaitCompletion: %s",
        BigQueryStrings.toString(tableRef),
        gcsPaths.size(),
        gcsPaths.isEmpty() ? "(empty)" : gcsPaths.get(0),
        awaitCompletion);

    // Create load conf with minimal requirements.
    JobConfigurationLoad loadConfig = new JobConfigurationLoad();
    loadConfig.setSchema(schema);
    loadConfig.setSourceFormat(sourceFormat.getFormatIdentifier());
    loadConfig.setSourceUris(gcsPaths);
    loadConfig.setDestinationTable(tableRef);
    loadConfig.setWriteDisposition(writeDisposition);
    if (!Strings.isNullOrEmpty(kmsKeyName)) {
      loadConfig.setDestinationEncryptionConfiguration(
          new EncryptionConfiguration().setKmsKeyName(kmsKeyName));
    }
    // Auto detect the schema if we're not given one, otherwise use the passed schema.
    if (schema == null) {
      logger.atInfo().log("No import schema provided, auto detecting schema.");
      loadConfig.setAutodetect(true);
    } else {
      logger.atInfo().log("Using provided import schema '%s'.", schema.toString());
    }

    JobConfiguration config = new JobConfiguration();
    config.setLoad(loadConfig);

    // Get the dataset to determine the location
    Dataset dataset =
        service.datasets().get(tableRef.getProjectId(), tableRef.getDatasetId()).execute();

    JobReference jobReference =
        createJobReference(projectId, "direct-bigqueryhelper-import", dataset.getLocation());
    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Insert and run job.
    insertJobOrFetchDuplicate(projectId, job);

    if (awaitCompletion) {
      // Poll until job is complete.
      BigQueryUtils.waitForJobCompletion(getRawBigquery(), projectId, jobReference, () -> {});
    }
  }

  /**
   * Exports BigQuery results into GCS, polls for completion before returning.
   *
   * @param projectId the project on whose behalf to perform the export.
   * @param tableRef the table to export.
   * @param gcsPaths the GCS paths to export to.
   * @param awaitCompletion if true, block and poll until job completes, otherwise return as soon as
   *        the job has been successfully dispatched.
   *
   * @throws IOException on IO error.
   * @throws InterruptedException on interrupt.
   */
  public void exportBigQueryToGcs(String projectId, TableReference tableRef, List<String> gcsPaths,
      boolean awaitCompletion) throws IOException, InterruptedException {
    logger.atFine().log(
        "exportBigQueryToGcs(bigquery, '%s', '%s', '%s', '%s')",
        projectId, BigQueryStrings.toString(tableRef), gcsPaths, awaitCompletion);
    logger.atInfo().log(
        "Exporting table '%s' to %s paths; path[0] is '%s'; awaitCompletion: %s",
        BigQueryStrings.toString(tableRef),
        gcsPaths.size(),
        gcsPaths.isEmpty() ? "(empty)" : gcsPaths.get(0),
        awaitCompletion);

    // Create job and configuration.
    JobConfigurationExtract extractConfig = new JobConfigurationExtract();

    // Set source.
    extractConfig.setSourceTable(tableRef);

    // Set destination.
    extractConfig.setDestinationUris(gcsPaths);
    extractConfig.set("destinationFormat", "NEWLINE_DELIMITED_JSON");

    JobConfiguration config = new JobConfiguration();
    config.setExtract(extractConfig);

    // Get the table to determine the location
    Table table = getTable(tableRef);

    JobReference jobReference =
        createJobReference(projectId, "direct-bigqueryhelper-export", table.getLocation());

    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Insert and run job.
    insertJobOrFetchDuplicate(projectId, job);

    if (awaitCompletion) {
      // Poll until job is complete.
      BigQueryUtils.waitForJobCompletion(service, projectId, jobReference, () -> {});
    }
  }

  /**
   * Returns true if the table exists, or false if not.
   */
  public boolean tableExists(TableReference tableRef) throws IOException {
    try {
      Table fetchedTable = service.tables().get(
          tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()).execute();
      logger.atFine().log(
          "Successfully fetched table '%s' for tableRef '%s'", fetchedTable, tableRef);
      return true;
    } catch (IOException ioe) {
      if (errorExtractor.itemNotFound(ioe)) {
        return false;
      } else {
        // Unhandled exceptions should just propagate up.
        throw ioe;
      }
    }
  }

  /**
   * Gets the specified table resource by table ID. This method does not return the data in the
   * table, it only returns the table resource, which describes the structure of this table.
   *
   * @param tableRef The BigQuery table reference.
   * @return The table resource, which describes the structure of this table.
   * @throws IOException
   */
  public Table getTable(TableReference tableRef)
      throws IOException {
    Bigquery.Tables.Get getTablesReply = service.tables().get(
        tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId());
    return getTablesReply.execute();
  }

  /**
   * Creates a new JobReference with a unique jobId generated from {@code jobIdPrefix} plus a
   * randomly generated UUID String.
   */
  public JobReference createJobReference(
      String projectId, String jobIdPrefix, @Nullable String location) {
    Preconditions.checkArgument(projectId != null, "projectId must not be null.");
    Preconditions.checkArgument(jobIdPrefix != null, "jobIdPrefix must not be null.");
    Preconditions.checkArgument(jobIdPrefix.matches(BIGQUERY_JOB_ID_PATTERN),
        "jobIdPrefix '%s' must match pattern '%s'", jobIdPrefix, BIGQUERY_JOB_ID_PATTERN);

    String fullJobId = String.format("%s-%s", jobIdPrefix, UUID.randomUUID().toString());
    Preconditions.checkArgument(fullJobId.length() <= BIGQUERY_JOB_ID_MAX_LENGTH,
        "fullJobId '%s' has length '%s'; must be less than or equal to %s",
        fullJobId, fullJobId.length(), BIGQUERY_JOB_ID_MAX_LENGTH);
    return new JobReference().setProjectId(projectId).setJobId(fullJobId).setLocation(location);
  }

  /**
   * Helper to check for non-null Job.getJobReference().getJobId() and quality of the getJobId()
   * between {@code expected} and {@code actual}, using Preconditions.checkState.
   */
  public void checkJobIdEquality(Job expected, Job actual) {
    Preconditions.checkState(actual.getJobReference() != null
        && actual.getJobReference().getJobId() != null
        && expected.getJobReference() != null
        && expected.getJobReference().getJobId() != null
        && actual.getJobReference().getJobId().equals(expected.getJobReference().getJobId()),
        "jobIds must match in '[expected|actual].getJobReference()' (got '%s' vs '%s')",
        expected.getJobReference(), actual.getJobReference());
  }

  /**
   * Tries to run jobs().insert(...) with the provided {@code projectId} and {@code job}, which
   * returns a {@code Job} under normal operation, which is then returned from this method.
   * In case of an exception being thrown, if the cause was "409 conflict", then we issue a
   * separate "jobs().get(...)" request and return the results of that fetch instead.
   * Other exceptions propagate out as normal.
   */
  public Job insertJobOrFetchDuplicate(String projectId, Job job) throws IOException {
    Preconditions.checkArgument(
        job.getJobReference() != null && job.getJobReference().getJobId() != null,
        "Require non-null JobReference and JobId inside; getJobReference() == '%s'",
        job.getJobReference());
    Insert insert = service.jobs().insert(projectId, job);
    Job response = null;
    try {
      response = insert.execute();
      logger.atFine().log("Successfully inserted job '%s'. Response: '%s'", job, response);
    } catch (IOException ioe) {
      if (errorExtractor.itemAlreadyExists(ioe)) {
        logger.atInfo().withCause(ioe).log(
            "Fetching existing job after catching exception for duplicate jobId '%s'",
            job.getJobReference().getJobId());
        response = service.jobs().get(projectId, job.getJobReference().getJobId()).execute();
      } else {
        throw new IOException(
            String.format("Unhandled exception trying to insert job '%s'", job), ioe);
      }
    }
    checkJobIdEquality(job, response);
    return response;
  }

  @VisibleForTesting
  void setErrorExtractor(ApiErrorExtractor errorExtractor) {
    this.errorExtractor = errorExtractor;
  }
}
