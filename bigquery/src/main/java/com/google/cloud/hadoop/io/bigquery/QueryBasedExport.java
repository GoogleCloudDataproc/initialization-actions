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
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.Progressable;

/**
 * A Export decorator that will attempt to perform a query during the export prepare phase.
 */
public class QueryBasedExport implements Export {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final String query;
  private final BigQueryHelper bigQueryHelper;
  private final String projectId;
  private final TableReference tableToExport;
  private final Export delegate;
  private final boolean deleteIntermediateTable;

  public QueryBasedExport(
      Export delegate,
      String query,
      String projectId,
      BigQueryHelper bigQueryHelper,
      TableReference tableToExport,
      boolean deleteIntermediateTable) {
    this.query = query;
    this.bigQueryHelper = bigQueryHelper;
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
      logger.atInfo().log(
          "Invoking query '%s' and saving to '%s' before beginning export/read.",
          query, BigQueryStrings.toString(tableToExport));
      try {
        runQuery(bigQueryHelper, projectId, tableToExport, query);
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
      logger.atInfo().log(
          "Deleting input intermediate table: %s:%s.%s",
          tableToExport.getProjectId(), tableToExport.getDatasetId(), tableToExport.getTableId());

      Bigquery.Tables tables = bigQueryHelper.getRawBigquery().tables();
      Bigquery.Tables.Delete delete = tables.delete(
          tableToExport.getProjectId(), tableToExport.getDatasetId(), tableToExport.getTableId());
      delete.execute();
    }

    delegate.cleanupExport();
  }

  /**
   * Runs the query in BigQuery and writes results to a temporary table.
   *
   * @param bigQueryHelper the Bigquery instance to use.
   * @param projectId the project on whose behalf the query will be run.
   * @param tableRef the table to write the results to.
   * @param query the query to run.
   * @throws IOException on IO error.
   * @throws InterruptedException on interrupt.
   */
  @VisibleForTesting
  static void runQuery(
      BigQueryHelper bigQueryHelper, String projectId, TableReference tableRef, String query)
      throws IOException, InterruptedException {
    logger.atFine().log(
        "runQuery(bigQueryHelper, '%s', '%s', '%s')",
        projectId, lazy(() -> BigQueryStrings.toString(tableRef)), query);

    // Create a query statement and query request object.
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    queryConfig.setAllowLargeResults(true);
    queryConfig.setQuery(query);

    // Set the table to put results into.
    queryConfig.setDestinationTable(tableRef);

    // Require table to be empty.
    queryConfig.setWriteDisposition("WRITE_EMPTY");

    JobConfiguration config = new JobConfiguration();
    config.setQuery(queryConfig);

    Table table = bigQueryHelper.getTable(tableRef);

    JobReference jobReference =
        bigQueryHelper.createJobReference(projectId, "querybasedexport", table.getLocation());

    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Run the job.
    Job response = bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);
    logger.atFine().log("Got response '%s'", response);

    // Create anonymous Progressable object
    Progressable progressable = new Progressable() {
      @Override
      public void progress() {
        // TODO(user): ensure task doesn't time out
      }
    };

    // Poll until job is complete.
    BigQueryUtils.waitForJobCompletion(
        bigQueryHelper.getRawBigquery(), projectId, jobReference, progressable);
  }

}
