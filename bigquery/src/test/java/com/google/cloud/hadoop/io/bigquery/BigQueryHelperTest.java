package com.google.cloud.hadoop.io.bigquery;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

/**
 * Unit tests for BigQueryHelper.
 */
@RunWith(JUnit4.class)
public class BigQueryHelperTest {
  // Mocks for Bigquery API objects.
  private Bigquery mockBigquery;
  private Bigquery.Jobs mockBigqueryJobs;
  private Bigquery.Jobs.Get mockBigqueryJobsGet;
  private Bigquery.Jobs.Insert mockBigqueryJobsInsert;
  private Bigquery.Tables mockBigqueryTables;
  private Bigquery.Tables.Get mockBigqueryTablesGet;

  // JobStatus to return for testing.
  private JobStatus jobStatus;

  // Bigquery Job result to return for testing.
  private Job jobHandle;

  // Fakes for Bigquery API objects which are final classes (cannot be mocked using Mockito).
  private Table fakeTable;
  private TableSchema fakeTableSchema;

  // Sample projectId for testing - for owning the BigQuery jobs.
  private String jobProjectId = "google.com:foo-project";

  // Sample TableReference for BigQuery.
  private TableReference tableRef;
  private String projectId = "google.com:bar-project";
  private String datasetId = "test_dataset";
  private String tableId = "test_table";

  // Sample jobId for JobReference for mockBigqueryJobs.
  private String jobId = "bigquery-job-1234";

  @Before
  public void setUp() throws IOException {
    GsonBigQueryInputFormat.log.setLevel(LogUtil.Level.DEBUG);

    // Create fake job reference.
    JobReference fakeJobReference = new JobReference();
    fakeJobReference.setJobId(jobId);

    // Create the job result.
    jobHandle = new Job();
    jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);
    jobHandle.setStatus(jobStatus);
    jobHandle.setJobReference(fakeJobReference);

    // Mock BigQuery.
    mockBigquery = mock(Bigquery.class);
    mockBigqueryJobs = mock(Bigquery.Jobs.class);
    mockBigqueryJobsGet = mock(Bigquery.Jobs.Get.class);
    mockBigqueryJobsInsert = mock(Bigquery.Jobs.Insert.class);
    mockBigqueryTables = mock(Bigquery.Tables.class);
    mockBigqueryTablesGet = mock(Bigquery.Tables.Get.class);

    // Mocks for Bigquery jobs.
    when(mockBigquery.jobs()).thenReturn(mockBigqueryJobs);

    // Mock getting Bigquery job.
    when(mockBigqueryJobs.get(jobProjectId, fakeJobReference.getJobId())).thenReturn(
        mockBigqueryJobsGet);
    when(mockBigqueryJobsGet.execute()).thenReturn(jobHandle);

    // Mock inserting Bigquery job.
    when(mockBigqueryJobs.insert(any(String.class), any(Job.class))).thenReturn(
        mockBigqueryJobsInsert);
    when(mockBigqueryJobsInsert.execute()).thenReturn(jobHandle);

    // Fake table.
    fakeTableSchema = new TableSchema();
    fakeTable = new Table().setSchema(fakeTableSchema);

    // Mocks for Bigquery tables.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);

    // Mocks for getting Bigquery table.
    when(mockBigqueryTables.get(any(String.class), any(String.class), any(String.class)))
        .thenReturn(mockBigqueryTablesGet);

    // Mock for executing get Bigquery table.
    when(mockBigqueryTablesGet.execute()).thenReturn(fakeTable);

    // Create table reference.
    tableRef = new TableReference();
    tableRef.setProjectId(projectId);
    tableRef.setDatasetId(datasetId);
    tableRef.setTableId(tableId);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockBigquery);
    verifyNoMoreInteractions(mockBigqueryJobs);
    verifyNoMoreInteractions(mockBigqueryJobsGet);
    verifyNoMoreInteractions(mockBigqueryJobsInsert);
    verifyNoMoreInteractions(mockBigqueryTables);
    verifyNoMoreInteractions(mockBigqueryTablesGet);
  }

  /**
   * Tests exportBigQueryToGCS method of BigQueryHelper .
   */
  @Test
  public void testExportBigQueryToGcsSingleShardAwaitCompletion() throws IOException,
      InterruptedException {
    // Run exportBigQueryToGCS method.
    new BigQueryHelper(mockBigquery).exportBigQueryToGcs(jobProjectId, tableRef,
        ImmutableList.of("test-export-path"), true);

    // Verify correct calls to BigQuery are made.
    verify(mockBigquery, times(2)).jobs();

    // Verify correct calls to BigQuery.Jobs are made.
    verify(mockBigqueryJobs, times(1)).get(eq(jobProjectId), eq(jobId));
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
    Job job = jobCaptor.getValue();
    assertEquals("test-export-path",
        job.getConfiguration().getExtract().getDestinationUris().get(0));
    assertEquals(tableRef, job.getConfiguration().getExtract().getSourceTable());

    // Verify correct calls to BigQuery.Jobs.Get are made.
    verify(mockBigqueryJobsGet, times(1)).execute();

    // Verify correct calls to BigQuery.Jobs.Insert are made.
    verify(mockBigqueryJobsInsert, times(1)).setProjectId(eq(jobProjectId));
    verify(mockBigqueryJobsInsert, times(1)).execute();
  }
  
  /**
   * Tests getTable method of BigQueryHelper.
   *
   * @throws IOException
   */
  @Test
  public void testGetTable() throws IOException {
    BigQueryHelper helper = new BigQueryHelper(mockBigquery);
    Table table = helper.getTable(tableRef);

    // Verify correct calls are made.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables, times(1)).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet, times(1)).execute();
    assertEquals(table, fakeTable);
  }

  /**
   * Tests getTableSchema method of BigQueryHelper.
   *
   * @throws IOException
   */
  @Test
  public void testGetTableSchema() throws IOException {
    BigQueryHelper helper = new BigQueryHelper(mockBigquery);
    TableSchema tableSchema = helper.getTableSchema(tableRef);

    // Verify correct calls are made.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables, times(1)).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet, times(1)).execute();
    assertEquals(tableSchema, fakeTableSchema);
  }
  
}
