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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.LoggerConfig;
import java.io.IOException;
import java.util.logging.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Unit tests for BigQueryHelper. */
@RunWith(JUnit4.class)
public class BigQueryHelperTest {

  /** Verify exceptions are being thrown. */
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

  // Sample KMS key name.
  private final String kmsKeyName =
      "projects/google.com:foo-project/locations/us-west1/keyRings/ring-1/cryptoKeys/key-1";

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
    LoggerConfig.getConfig(GsonBigQueryInputFormat.class).setLevel(Level.FINE);

    // Create fake job reference.
    JobReference fakeJobReference = new JobReference().setProjectId(jobProjectId).setJobId(jobId);

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
    when(mockBigqueryJobsGet.setLocation(any(String.class))).thenReturn(mockBigqueryJobsGet);

    // Mock inserting Bigquery job.
    when(mockBigqueryJobs.insert(any(String.class), any(Job.class)))
        .thenReturn(mockBigqueryJobsInsert);

    // Fake table.
    fakeTableSchema = new TableSchema();
    fakeTable = new Table().setSchema(fakeTableSchema).setLocation("test_location");

    // Mocks for Bigquery tables.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);
    when(mockBigqueryTables.get(any(String.class), any(String.class), any(String.class)))
        .thenReturn(mockBigqueryTablesGet);

    Datasets datasets = Mockito.mock(Datasets.class);
    Datasets.Get datasetsGet = Mockito.mock(Datasets.Get.class);
    Dataset dataset = new Dataset().setLocation("test_location");
    when(mockBigquery.datasets()).thenReturn(datasets);
    when(datasets.get(any(String.class), any(String.class))).thenReturn(datasetsGet);
    when(datasetsGet.execute()).thenReturn(dataset);

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

  /** Tests importBigQueryFromGcs method of BigQueryHelper. */
  @Test
  public void testImportBigQueryFromGcs() throws Exception {
    when(mockBigqueryTablesGet.execute()).thenReturn(fakeTable);

    final ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    doAnswer(
            new Answer<Job>() {
              @Override
              public Job answer(InvocationOnMock invocationOnMock) throws Throwable {
                verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
                return jobCaptor.getValue();
              }
            })
        .when(mockBigqueryJobsInsert)
        .execute();
    when(mockBigqueryJobsGet.execute()).thenReturn(jobHandle);

    // Run importBigQueryFromGcs method.
    helper.importFromGcs(
        jobProjectId,
        tableRef,
        fakeTableSchema,
        /* timePartitioning= */ null,
        kmsKeyName,
        BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
        BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION.getDefault(),
        BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION.getDefault(),
        ImmutableList.of("test-import-path"),
        true);

    // Verify correct calls to BigQuery are made.
    verify(mockBigquery, times(2)).jobs();

    // Verify correct calls to BigQuery.Jobs are made.
    verify(mockBigqueryJobs, times(1))
        .get(eq(jobProjectId), eq(jobCaptor.getValue().getJobReference().getJobId()));
    Job job = jobCaptor.getValue();
    assertThat(job.getConfiguration().getLoad().getSourceUris()).contains("test-import-path");
    assertThat(job.getConfiguration().getLoad().getDestinationTable()).isEqualTo(tableRef);
    assertThat(job.getJobReference().getLocation()).isEqualTo("test_location");

    // Verify we poll for job in correct location
    verify(mockBigqueryJobsGet).setLocation(eq("test_location"));

    // Verify correct calls to BigQuery.Jobs.Get are made.
    verify(mockBigqueryJobsGet, times(1)).execute();

    // Verify correct calls to BigQuery.Jobs.Insert are made.
    verify(mockBigqueryJobsInsert, times(1)).execute();

    verify(mockBigquery).datasets();
  }

  /** Tests exportBigQueryToGCS method of BigQueryHelper. */
  @Test
  public void testExportBigQueryToGcsSingleShardAwaitCompletion() throws Exception {
    when(mockBigqueryTablesGet.execute()).thenReturn(fakeTable);

    final ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    doAnswer(
            new Answer<Job>() {
              @Override
              public Job answer(InvocationOnMock invocationOnMock) throws Throwable {
                verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
                return jobCaptor.getValue();
              }
            })
        .when(mockBigqueryJobsInsert)
        .execute();
    when(mockBigqueryJobsGet.execute()).thenReturn(jobHandle);

    // Run exportBigQueryToGCS method.
    helper.exportBigQueryToGcs(jobProjectId, tableRef, ImmutableList.of("test-export-path"), true);

    // Verify correct calls to BigQuery are made.
    verify(mockBigquery, times(2)).jobs();

    // Verify correct calls to BigQuery.Jobs are made.
    verify(mockBigqueryJobs, times(1))
        .get(eq(jobProjectId), eq(jobCaptor.getValue().getJobReference().getJobId()));
    Job job = jobCaptor.getValue();
    assertThat(job.getConfiguration().getExtract().getDestinationUris().get(0))
        .isEqualTo("test-export-path");
    assertThat(job.getConfiguration().getExtract().getSourceTable()).isEqualTo(tableRef);
    assertThat(job.getJobReference().getLocation()).isEqualTo("test_location");

    // Verify we poll for job in correct location
    verify(mockBigqueryJobsGet).setLocation(eq("test_location"));

    // Verify correct calls to BigQuery.Jobs.Get are made.
    verify(mockBigqueryJobsGet, times(1)).execute();

    // Verify correct calls to BigQuery.Jobs.Insert are made.
    verify(mockBigqueryJobsInsert, times(1)).execute();

    verify(mockBigquery).tables();
    verify(mockBigqueryTables).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet).execute();
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
    assertThat(fakeTable).isEqualTo(table);
  }

  @Test
  public void testTableExistsTrue() throws IOException {
    when(mockBigqueryTablesGet.execute()).thenReturn(fakeTable);

    boolean exists = helper.tableExists(tableRef);

    // Verify correct calls are made.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables, times(1)).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet, times(1)).execute();

    assertThat(exists).isTrue();
  }

  @Test
  public void testTableExistsFalse() throws IOException {
    IOException fakeNotFoundException = new IOException("Fake not found exception");
    when(mockBigqueryTablesGet.execute()).thenThrow(fakeNotFoundException);
    when(mockErrorExtractor.itemNotFound(any(IOException.class))).thenReturn(true);

    boolean exists = helper.tableExists(tableRef);

    // Verify correct calls are made.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables, times(1)).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet, times(1)).execute();
    verify(mockErrorExtractor, times(1)).itemNotFound(eq(fakeNotFoundException));

    assertThat(exists).isFalse();
  }

  @Test
  public void testTableExistsUnhandledException() throws IOException {
    IOException fakeUnhandledException = new IOException("Fake unhandled exception");
    when(mockBigqueryTablesGet.execute()).thenThrow(fakeUnhandledException);
    when(mockErrorExtractor.itemNotFound(any(IOException.class))).thenReturn(false);

    IOException thrown = assertThrows(IOException.class, () -> helper.tableExists(tableRef));
    assertThat(thrown).hasCauseThat().isEqualTo(fakeUnhandledException);

    // Verify correct calls are made.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables, times(1)).get(eq(projectId), eq(datasetId), eq(tableId));
    verify(mockBigqueryTablesGet, times(1)).execute();
    verify(mockErrorExtractor, times(1)).itemNotFound(eq(fakeUnhandledException));
  }

  @Test
  public void testInsertJobOrFetchDuplicateBasicInsert() throws IOException {
    when(mockBigqueryJobsInsert.execute()).thenReturn(jobHandle);

    assertThat(helper.insertJobOrFetchDuplicate(jobProjectId, jobHandle)).isEqualTo(jobHandle);

    verify(mockBigquery, times(1)).jobs();
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
    Job job = jobCaptor.getValue();
    assertThat(job).isEqualTo(jobHandle);
    verify(mockBigqueryJobsInsert, times(1)).execute();
  }

  @Test
  public void testInsertJobOrFetchDuplicateAlreadyExistsException() throws IOException {
    IOException fakeConflictException = new IOException("fake 409 conflict");
    when(mockBigqueryJobsInsert.execute()).thenThrow(fakeConflictException);
    when(mockErrorExtractor.itemAlreadyExists(any(IOException.class))).thenReturn(true);
    when(mockBigqueryJobsGet.execute()).thenReturn(jobHandle);

    assertThat(helper.insertJobOrFetchDuplicate(jobProjectId, jobHandle)).isEqualTo(jobHandle);

    verify(mockBigquery, times(2)).jobs();
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
    Job job = jobCaptor.getValue();
    assertThat(job).isEqualTo(jobHandle);
    verify(mockBigqueryJobsInsert, times(1)).execute();
    verify(mockBigqueryJobs, times(1)).get(eq(jobProjectId), eq(jobId));
    verify(mockBigqueryJobsGet, times(1)).execute();
    verify(mockErrorExtractor).itemAlreadyExists(eq(fakeConflictException));
  }

  @Test
  public void testInsertJobOrFetchDuplicateUnhandledException() throws IOException {
    IOException unhandledException = new IOException("unhandled exception");
    when(mockBigqueryJobsInsert.execute()).thenThrow(unhandledException);
    when(mockErrorExtractor.itemAlreadyExists(any(IOException.class))).thenReturn(false);

    IOException thrown =
        assertThrows(
            IOException.class, () -> helper.insertJobOrFetchDuplicate(jobProjectId, jobHandle));
    assertThat(thrown).hasMessageThat().contains(jobHandle.toString());
    assertThat(thrown).hasCauseThat().isEqualTo(unhandledException);

    verify(mockBigquery, times(1)).jobs();
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    verify(mockBigqueryJobs, times(1)).insert(eq(jobProjectId), jobCaptor.capture());
    Job job = jobCaptor.getValue();
    assertThat(job).isEqualTo(jobHandle);
    verify(mockBigqueryJobsInsert, times(1)).execute();
    verify(mockErrorExtractor).itemAlreadyExists(eq(unhandledException));
  }
}
