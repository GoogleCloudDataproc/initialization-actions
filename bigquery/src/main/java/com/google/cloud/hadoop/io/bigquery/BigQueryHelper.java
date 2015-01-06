package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.LogUtil;

import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.List;

/**
 * Wrapper for BigQuery API.
 */
public class BigQueryHelper {

  // Logger.
  protected static final LogUtil log = new LogUtil(BigQueryHelper.class);

  private Bigquery service;

  public BigQueryHelper(Bigquery service) {
    this.service = service;
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
    log.debug("exportBigQueryToGcs(bigquery, '%s', '%s', '%s', '%s')", projectId,
        BigQueryStrings.toString(tableRef), gcsPaths, awaitCompletion);
    log.info("Exporting table '%s' to %d paths; path[0] is '%s'; awaitCompletion: %s",
        BigQueryStrings.toString(tableRef), gcsPaths.size(), gcsPaths.get(0), awaitCompletion);

    // Create job and configuration.
    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationExtract extractConfig = new JobConfigurationExtract();

    // Set source.
    extractConfig.setSourceTable(tableRef);

    // Set destination.
    extractConfig.setDestinationUris(gcsPaths);
    extractConfig.set("destinationFormat", "NEWLINE_DELIMITED_JSON");
    config.setExtract(extractConfig);
    job.setConfiguration(config);

    // Insert and run job.
    Insert insert = service.jobs().insert(projectId, job);
    insert.setProjectId(projectId);
    JobReference jobId = insert.execute().getJobReference();

    // Create anonymous Progressable object
    Progressable progressable = new Progressable() {
      @Override
      public void progress() {
        // TODO(user): ensure task doesn't time out
      }
    };

    if (awaitCompletion) {
      // Poll until job is complete.
      BigQueryUtils.waitForJobCompletion(service, projectId, jobId, progressable);
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
   * Gets the schema of this table.
   *
   * @param tableRef The BigQuery table reference.
   * @return value or null for none
   * @throws IOException
   */
  public TableSchema getTableSchema(TableReference tableRef)
      throws IOException {
    Table table = getTable(tableRef);
    return table.getSchema();
  }

}
