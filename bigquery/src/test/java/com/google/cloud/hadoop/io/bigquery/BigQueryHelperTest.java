package com.google.cloud.hadoop.io.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
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
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for BigQueryHelper.
 */
@RunWith(JUnit4.class)
public class BigQueryHelperTest {
  // Mocks for Bigquery API objects.
  @Mock private Bigquery mockBigquery;
  @Mock private Bigquery.Jobs mockBigqueryJobs;
  @Mock private Bigquery.Jobs.Get mockBigqueryJobsGet;
  @Mock private Bigquery.Jobs.Insert mockBigqueryJobsInsert;
  @Mock private Bigquery.Tables mockBigqueryTables;
  @Mock private Bigquery.Tables.Get mockBigqueryTablesGet;
  @Mock private ApiErrorExtractor mockErrorExtractor;

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

  // The instance being tested.
  private BigQueryHelper helper;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    Logger.getLogger(GsonBigQueryInputFormat.class).setLevel(Level.DEBUG);

    // Create fake job reference.
    JobReference fakeJobReference = new JobReference();
    fakeJobReference.setProjectId(jobProjectId);
    fakeJobReference.setJobId(jobId);

    // Create the job result.
    jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);

    jobHandle = new Job();
    jobHandle.setStatus(jobStatus);
    jobHandle.setJobReference(fakeJobReference);

    // Mocks for Bigquery jobs.
    when(mockBigquery.jobs()).thenReturn(mockBigqueryJobs);

    // Mock getting Bigquery job.
    when(mockBigqueryJobs.get(any(String.class), any(String.class)))
        .thenReturn(mockBigqueryJobsGet);

    // Mock inserting Bigquery job.
    when(mockBigqueryJobs.insert(any(String.class), any(Job.class))).thenReturn(
        mockBigqueryJobsInsert);

    // Fake table.
    fakeTableSchema = new TableSchema();
    fakeTable = new Table().setSchema(fakeTableSchema);

    // Mocks for Bigquery tables.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);

    // Mocks for getting Bigquery table.
    when(mockBigqueryTables.get(any(String.class), any(String.class), any(String.class)))
        .thenReturn(mockBigqueryTablesGet);

    // Create table reference.
    tableRef = new TableReference();
    tableRef.setProjectId(projectId);
    tableRef.setDatasetId(datasetId);
    tableRef.setTableId(tableId);

    helper = new BigQueryHelper(mockBigquery);
    helper.setErrorExtractor(mockErrorExtractor);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockBigquery);
    verifyNoMoreInteractions(mockBigqueryJobs);
    verifyNoMoreInteractions(mockBigqueryJobsGet);
    verifyNoMoreInteractions(mockBigqueryJobsInsert);
    verifyNoMoreInteractions(mockBigqueryTables);
    verifyNoMoreInteractions(mockBigqueryTablesGet);
    verifyNoMoreInteractions(mockErrorExtractor);
  }

  /**
   * Tests exportBigQueryToGCS method of BigQueryHelper .
   */
  @Test
  public void testExportBigQueryToGcsSingleShardAwaitCompletion() throws IOException,
      InterruptedException {
    when(mockBigqueryTablesGet.execute()).thenReturn(fakeTable);

    final ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    doAnswer(new Answer<Job>() {
      @Override
      public Job answer(InvocationOnMock invocationOnMock) throws Throwable {
        verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
        return jobCaptor.getValue();
      }
    }).when(mockBigqueryJobsInsert).execute();
    when(mockBigqueryJobsGet.execute()).thenReturn(jobHandle);

    // Run exportBigQueryToGCS method.
    helper.exportBigQueryToGcs(jobProjectId, tableRef,
        ImmutableList.of("test-export-path"), true);

    // Verify correct calls to BigQuery are made.
    verify(mockBigquery, times(2)).jobs();

    // Verify correct calls to BigQuery.Jobs are made.
    verify(mockBigqueryJobs, times(1)).get(
        eq(jobProjectId),
        eq(jobCaptor.getValue().getJobReference().getJobId()));
    Job job = jobCaptor.getValue();
    assertEquals("test-export-path",
        job.getConfiguration().getExtract().getDestinationUris().get(0));
    assertEquals(tableRef, job.getConfiguration().getExtract().getSourceTable());

    // Verify correct calls to BigQuery.Jobs.Get are made.
    verify(mockBigqueryJobsGet, times(1)).execute();

    // Verify correct calls to BigQuery.Jobs.Insert are made.
    verify(mockBigqueryJobsInsert, times(1)).execute();
  }

  /**
   * Tests getTable method of BigQueryHelper.
   *
   * @throws IOException
   */
  @Test
  public void testGetTable() throws IOException {
    when(mockBigqueryTablesGet.execute()).thenReturn(fakeTable);

    Table table = helper.getTable(tableRef);

    // Verify correct calls are made.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables, times(1)).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet, times(1)).execute();
    assertEquals(table, fakeTable);
  }

  @Test
  public void testTableExistsTrue() throws IOException {
    when(mockBigqueryTablesGet.execute()).thenReturn(fakeTable);

    boolean exists = helper.tableExists(tableRef);

    // Verify correct calls are made.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables, times(1)).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet, times(1)).execute();

    assertEquals(true, exists);
  }

  @Test
  public void testTableExistsFalse() throws IOException {
    IOException fakeNotFoundException = new IOException("Fake not found exception");
    when(mockBigqueryTablesGet.execute())
        .thenThrow(fakeNotFoundException);
    when(mockErrorExtractor.itemNotFound(any(IOException.class)))
        .thenReturn(true);

    boolean exists = helper.tableExists(tableRef);

    // Verify correct calls are made.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables, times(1)).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet, times(1)).execute();
    verify(mockErrorExtractor, times(1)).itemNotFound(eq(fakeNotFoundException));

    assertEquals(false, exists);
  }

  @Test
  public void testTableExistsUnhandledException() throws IOException {
    IOException fakeUnhandledException = new IOException("Fake unhandled exception");
    when(mockBigqueryTablesGet.execute())
        .thenThrow(fakeUnhandledException);
    when(mockErrorExtractor.itemNotFound(any(IOException.class)))
        .thenReturn(false);

    try {
      helper.tableExists(tableRef);
      fail("Expected IOException during tableExists(tableRef), got no exception");
    } catch (IOException ioe) {
      assertEquals(fakeUnhandledException, ioe);
    }

    // Verify correct calls are made.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables, times(1)).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet, times(1)).execute();
    verify(mockErrorExtractor, times(1)).itemNotFound(eq(fakeUnhandledException));
  }

  @Test
  public void testInsertJobOrFetchDuplicateBasicInsert() throws IOException {
    when(mockBigqueryJobsInsert.execute()).thenReturn(jobHandle);

    assertEquals(jobHandle, helper.insertJobOrFetchDuplicate(jobProjectId, jobHandle));

    verify(mockBigquery, times(1)).jobs();
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
    Job job = jobCaptor.getValue();
    assertEquals(jobHandle, job);
    verify(mockBigqueryJobsInsert, times(1)).execute();
  }

  @Test
  public void testInsertJobOrFetchDuplicateAlreadyExistsException() throws IOException {
    IOException fakeConflictException = new IOException("fake 409 conflict");
    when(mockBigqueryJobsInsert.execute())
        .thenThrow(fakeConflictException);
    when(mockErrorExtractor.itemAlreadyExists(any(IOException.class)))
        .thenReturn(true);
    when(mockBigqueryJobsGet.execute()).thenReturn(jobHandle);

    assertEquals(jobHandle, helper.insertJobOrFetchDuplicate(jobProjectId, jobHandle));

    verify(mockBigquery, times(2)).jobs();
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
    Job job = jobCaptor.getValue();
    assertEquals(jobHandle, job);
    verify(mockBigqueryJobsInsert, times(1)).execute();
    verify(mockBigqueryJobs, times(1)).get(eq(jobProjectId), eq(jobId));
    verify(mockBigqueryJobsGet, times(1)).execute();
    verify(mockErrorExtractor).itemAlreadyExists(eq(fakeConflictException));
  }

  @Test
  public void testInsertJobOrFetchDuplicateUnhandledException() throws IOException {
    IOException unhandledException = new IOException("unhandled exception");
    when(mockBigqueryJobsInsert.execute())
        .thenThrow(unhandledException);
    when(mockErrorExtractor.itemAlreadyExists(any(IOException.class)))
        .thenReturn(false);

    try {
      helper.insertJobOrFetchDuplicate(jobProjectId, jobHandle);
      fail("Expected IOException on insertJobOrFetchDuplicate, got no exception.");
    } catch (IOException ioe) {
      assertEquals(unhandledException, ioe);
    }

    verify(mockBigquery, times(1)).jobs();
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
    Job job = jobCaptor.getValue();
    assertEquals(jobHandle, job);
    verify(mockBigqueryJobsInsert, times(1)).execute();
    verify(mockErrorExtractor).itemAlreadyExists(eq(unhandledException));
  }
}
