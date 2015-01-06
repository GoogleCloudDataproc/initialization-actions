package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.List;

/**
 * A Export decorator that will attempt to perform a query during the export prepare phase.
 */
public class QueryBasedExport implements Export {

  protected static final LogUtil log = new LogUtil(QueryBasedExport.class);

  private final String query;
  private final Bigquery bigQueryClient;
  private final String projectId;
  private final TableReference tableToExport;
  private final Export delegate;
  private final boolean deleteIntermediateTable;

  public QueryBasedExport(
      Export delegate,
      String query,
      String projectId,
      Bigquery bigQueryClient,
      TableReference tableToExport,
      boolean deleteIntermediateTable) {
    this.query = query;
    this.bigQueryClient = bigQueryClient;
    this.projectId = projectId;
    this.tableToExport = tableToExport;
    this.delegate = delegate;
    this.deleteIntermediateTable = deleteIntermediateTable;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return delegate.getSplits(context);
  }

  @Override
  public List<String> getExportPaths() throws IOException {
    return delegate.getExportPaths();
  }

  @Override
  public void beginExport() throws IOException {
    delegate.beginExport();
  }

  @Override
  public void waitForUsableMapReduceInput() throws IOException, InterruptedException {
    delegate.waitForUsableMapReduceInput();
  }

  @Override
  public void prepare() throws IOException {
    if (!Strings.isNullOrEmpty(query)) {
      log.info("Invoking query '%s' and saving to '%s' before beginning export/read.",
          query, BigQueryStrings.toString(tableToExport));
      try {
        runQuery(bigQueryClient, projectId, tableToExport, query);
      } catch (InterruptedException ie) {
        throw new IOException(
            String.format("Interrupted during query '%s' into table '%s'",
                query, BigQueryStrings.toString(tableToExport)), ie);
      }
    }
    delegate.prepare();
  }

  @Override
  public void cleanupExport() throws IOException {
    if (deleteIntermediateTable) {
      log.info(
          "Deleting input intermediate table: %s:%s.%s",
          tableToExport.getProjectId(),
          tableToExport.getDatasetId(),
          tableToExport.getTableId());

      Bigquery.Tables tables = bigQueryClient.tables();
      Bigquery.Tables.Delete delete = tables.delete(
          tableToExport.getProjectId(), tableToExport.getDatasetId(), tableToExport.getTableId());
      delete.execute();
    }

    delegate.cleanupExport();
  }

  /**
   * Runs the query in BigQuery and writes results to a temporary table.
   *
   * @param bigquery the Bigquery instance to use.
   * @param projectId the project on whose behalf the query will be run.
   * @param tableRef the table to write the results to.
   * @param query the query to run.
   * @throws IOException on IO error.
   * @throws InterruptedException on interrupt.
   */
  @VisibleForTesting
  static void runQuery(Bigquery bigquery, String projectId, TableReference tableRef, String query)
      throws IOException, InterruptedException {
    log.debug("runQuery(bigquery, '%s', '%s', '%s')",
        projectId, BigQueryStrings.toString(tableRef), query);

    // Create a query statement and query request object.
    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    queryConfig.setAllowLargeResults(true);
    queryConfig.setQuery(query);

    // Set the table to put results into.
    queryConfig.setDestinationTable(tableRef);

    // Require table to be empty.
    queryConfig.setWriteDisposition("WRITE_EMPTY");
    config.setQuery(queryConfig);
    job.setConfiguration(config);

    // Run the job.
    Bigquery.Jobs.Insert insert = bigquery.jobs().insert(projectId, job);
    insert.setProjectId(projectId);
    JobReference jobId = insert.execute().getJobReference();

    // Create anonymous Progressable object
    Progressable progressable = new Progressable() {
      @Override
      public void progress() {
        // TODO(user): ensure task doesn't time out
      }
    };

    // Poll until job is complete.
    BigQueryUtils.waitForJobCompletion(bigquery, projectId, jobId, progressable);
  }

}
