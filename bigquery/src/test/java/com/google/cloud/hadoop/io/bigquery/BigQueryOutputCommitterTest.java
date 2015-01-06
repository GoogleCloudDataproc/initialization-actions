package com.google.cloud.hadoop.io.bigquery;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs;
import com.google.api.services.bigquery.Bigquery.Jobs.Get;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.fs.gcs.InMemoryGoogleHadoopFileSystem;
import com.google.cloud.hadoop.testing.CredentialConfigurationUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for BigQueryOutputCommitter.
 */
@RunWith(JUnit4.class)
public class BigQueryOutputCommitterTest {
  // Sample BigQueryOutputCommitter.
  private static BigQueryOutputCommitter committerInstance;

  // Sample Job context for testing.
  private static JobContext jobContext;

  // Sample projectId owning the BigQuery jobs.
  private static final String JOB_PROJECT_ID = "job-owner-project";

  // Sample projectId for the final table.
  private static final String FINAL_PROJECT_ID = "final-project";

  // Sample destination datasetId.
  private static final String FINAL_DATASET_ID = "test_final_dataset";

  // Sample destination tableId.
  private static final String FINAL_TABLE_ID = "test_final_table";

  // Sample projectId for the temp table.
  private static final String TEMP_PROJECT_ID = "temp-project";

  // Sample temp datasetId.
  private static final String TEMP_DATASET_ID = "test_temp_dataset";

  // Sample temp tableId.
  private static final String TEMP_TABLE_ID = "test_temp_table";

  // In memory file system for testing.
  private Configuration conf;

  // The expected Dataset object based on our project/dataset-id settings.
  private Dataset expectedTempDataset;

  // The expected TableReference for temp writes.
  private TableReference tempTableRef;

  // The expected final destination TableReference.
  private TableReference finalTableRef;

  // Mock Bigquery for testing.
  @Mock
  private Bigquery mockBigquery;

  // Mock Bigquery.Datasets for testing.
  @Mock
  private Bigquery.Datasets mockBigqueryDatasets;

  // Mock Bigquery.Datasets.Insert for testing.
  @Mock
  private Bigquery.Datasets.Insert mockBigqueryDatasetsInsert;

  // Mock Bigquery.Datasets.Delete for testing.
  @Mock
  private Bigquery.Datasets.Delete mockBigqueryDatasetsDelete;

  // Mock Bigquery.Tables for testing.
  @Mock
  private Bigquery.Tables mockBigqueryTables;

  // Mock Bigquery.Tables.Delete for testing.
  @Mock
  private Bigquery.Tables.Delete mockBigqueryTablesDelete;

  // Sample TaskAttempt context for testing.
  @Mock
  private TaskAttemptContext context;

  /**
   * Sets up common objects for testing before each test.
   */
  @Before
  public void setUp() 
      throws IOException {
    // Generate Mocks.
    MockitoAnnotations.initMocks(this);

    // Generate the configuration.
    conf = InMemoryGoogleHadoopFileSystem.getSampleConfiguration();
    expectedTempDataset = new Dataset()
        .setDatasetReference(new DatasetReference()
            .setProjectId(TEMP_PROJECT_ID)
            .setDatasetId(TEMP_DATASET_ID));
    CredentialConfigurationUtil.addTestConfigurationSettings(conf);

    // Create job context.
    jobContext = org.apache.hadoop.mapreduce.Job.getInstance(conf);

    tempTableRef = new TableReference()
        .setProjectId(TEMP_PROJECT_ID)
        .setDatasetId(TEMP_DATASET_ID)
        .setTableId(TEMP_TABLE_ID);

    finalTableRef = new TableReference()
        .setProjectId(FINAL_PROJECT_ID)
        .setDatasetId(FINAL_DATASET_ID)
        .setTableId(FINAL_TABLE_ID);

    // Set OutputCommitter.
    committerInstance =
        new BigQueryOutputCommitter(JOB_PROJECT_ID, tempTableRef, finalTableRef, conf);
    committerInstance.setBigquery(mockBigquery);
  }

  /**
   * Verifies there are no more interactions.
   */
  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockBigquery);
    verifyNoMoreInteractions(mockBigqueryTables);
    verifyNoMoreInteractions(mockBigqueryTablesDelete);
    verifyNoMoreInteractions(mockBigqueryDatasets);
    verifyNoMoreInteractions(mockBigqueryDatasetsInsert);
    verifyNoMoreInteractions(mockBigqueryDatasetsDelete);
  }
  
  /**
   * Tests the setupJob method of BigQueryOutputFormat.
   */
  @Test
  public void testSetupJob() 
      throws IOException {
    // Mock method calls.
    when(mockBigquery.datasets()).thenReturn(mockBigqueryDatasets);
    when(mockBigqueryDatasets.insert(any(String.class), any(Dataset.class)))
        .thenReturn(mockBigqueryDatasetsInsert);

    // Run method and verify calls.
    committerInstance.setupJob(jobContext);
    verify(mockBigquery).datasets();
    verify(mockBigqueryDatasets).insert(eq(TEMP_PROJECT_ID), eq(expectedTempDataset));
    verify(mockBigqueryDatasetsInsert, times(1)).execute();
  }

  /**
   * Tests the setupTask method of BigQueryOutputFormat.
   */
  @Test
  public void testSetupTask() 
      throws IOException {
    // Mock method calls.
    when(mockBigquery.datasets()).thenReturn(mockBigqueryDatasets);
    when(mockBigqueryDatasets.insert(any(String.class), any(Dataset.class)))
        .thenReturn(mockBigqueryDatasetsInsert);

    // Run method and verify calls.
    committerInstance.setupTask(context);
    // Tear down verifies no calls are made.
  }

  /**
   * Tests the cleanupJob method of BigQueryOutputFormat.
   */
  @Test
  public void testCleanupJobWithIntermediateDelete() 
      throws IOException {
    // Set intermediate table for deletion.
    jobContext.getConfiguration().setBoolean(
        BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, true);

    // Mock method calls to delete temporary table.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);

    // Mock method calls.
    when(mockBigquery.datasets()).thenReturn(mockBigqueryDatasets);
    when(mockBigqueryDatasets.delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID)))
        .thenReturn(mockBigqueryDatasetsDelete);
    when(mockBigqueryDatasetsDelete.setDeleteContents(true)).thenReturn(mockBigqueryDatasetsDelete);

    // Run method and verify calls.
    committerInstance.cleanupJob(jobContext);

    verify(mockBigquery).datasets();
    verify(mockBigqueryDatasets).delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID));
    verify(mockBigqueryDatasetsDelete).setDeleteContents(true);
    verify(mockBigqueryDatasetsDelete, times(1)).execute();
  }

  /**
   * Tests the cleanupJob method of BigQueryOutputFormat with no "intermediate delete";
   * "intermediate delete" only refers to the InputFormat side.
   */
  @Test
  public void testCleanupJobWithNoIntermediateDelete() 
      throws IOException {
    // Set intermediate table for deletion.
    jobContext.getConfiguration()
        .setBoolean(BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, false);

    // Mock method calls.
    when(mockBigquery.datasets()).thenReturn(mockBigqueryDatasets);
    when(mockBigqueryDatasets.delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID)))
        .thenReturn(mockBigqueryDatasetsDelete);
    when(mockBigqueryDatasetsDelete.setDeleteContents(true)).thenReturn(mockBigqueryDatasetsDelete);

    // Run method and verify calls.
    committerInstance.cleanupJob(jobContext);
    verify(mockBigquery).datasets();
    verify(mockBigqueryDatasets).delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID));
    verify(mockBigqueryDatasetsDelete).setDeleteContents(true);
    verify(mockBigqueryDatasetsDelete, times(1)).execute();
  }

  /**
   * Tests the cleanupJob method of BigQueryOutputFormat with error thrown.
   */
  @Test
  public void testCleanupJobWithError() 
      throws IOException {
    // Mock method calls to delete temporary table.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);

    // Mock method calls.
    when(mockBigquery.datasets()).thenReturn(mockBigqueryDatasets);
    when(mockBigqueryDatasets.delete(any(String.class), any(String.class)))
        .thenThrow(new IOException());

    // Run method and verify calls.
    committerInstance.cleanupJob(jobContext);
    verify(mockBigquery).datasets();
    verify(mockBigqueryDatasets).delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID));
  }

  /**
   * Tests the abortJob method of BigQueryOutputFormat with intermediate delete.
   */
  @Test
  public void testAbortJobWithIntermediateDelete() 
      throws IOException {
    // Mock method calls to delete temporary table.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);

    // Mock method calls.
    when(mockBigquery.datasets()).thenReturn(mockBigqueryDatasets);
    when(mockBigqueryDatasets.delete(any(String.class), any(String.class)))
        .thenReturn(mockBigqueryDatasetsDelete);
    when(mockBigqueryDatasetsDelete.setDeleteContents(true)).thenReturn(mockBigqueryDatasetsDelete);


    // Run method and verify calls.
    committerInstance.abortJob(jobContext, 1);
    verify(mockBigquery).datasets();
    verify(mockBigqueryDatasets).delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID));
    verify(mockBigqueryDatasetsDelete).setDeleteContents(true);
    verify(mockBigqueryDatasetsDelete, times(1)).execute();
  }

  /**
   * Tests the abortJob method of BigQueryOutputFormat with no intermediate delete.
   */
  @Test
  public void testAbortJobWithNoIntermediateDelete() 
      throws IOException {
    // Set intermediate table for deletion.
    jobContext.getConfiguration()
        .setBoolean(BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, false);

    // Mock method calls.
    when(mockBigquery.datasets()).thenReturn(mockBigqueryDatasets);
    when(mockBigqueryDatasets.delete(any(String.class), any(String.class)))
        .thenReturn(mockBigqueryDatasetsDelete);
    when(mockBigqueryDatasetsDelete.setDeleteContents(true)).thenReturn(mockBigqueryDatasetsDelete);


    // Run method and verify calls.
    committerInstance.abortJob(jobContext, 1);
    verify(mockBigquery).datasets();
    verify(mockBigqueryDatasets).delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID));
    verify(mockBigqueryDatasetsDelete).setDeleteContents(true);
    verify(mockBigqueryDatasetsDelete, times(1)).execute();
  }

  /**
   * Tests the abortJob method of BigQueryOutputFormat with intermediate delete.
   */
  @Test
  public void testCommitJobWithIntermediateDelete() 
      throws IOException {
    // Set intermediate table for deletion.
    jobContext.getConfiguration().setBoolean(
        BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, true);

    // Mock method calls to delete temporary table.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);

    // Mock method calls.
    when(mockBigquery.datasets()).thenReturn(mockBigqueryDatasets);
    when(mockBigqueryDatasets.delete(any(String.class), any(String.class)))
        .thenReturn(mockBigqueryDatasetsDelete);
    when(mockBigqueryDatasetsDelete.setDeleteContents(true)).thenReturn(mockBigqueryDatasetsDelete);


    // Run method and verify calls.
    committerInstance.commitJob(jobContext);
    verify(mockBigquery).datasets();
    verify(mockBigqueryDatasets).delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID));
    verify(mockBigqueryDatasetsDelete).setDeleteContents(true);
    verify(mockBigqueryDatasetsDelete, times(1)).execute();
  }

  /**
   * Tests the abortJob method of BigQueryOutputFormat with no intermediate delete.
   */
  @Test
  public void testCommitJobWithNoIntermediateDelete() 
      throws IOException {
    // Set intermediate table for deletion.
    jobContext.getConfiguration()
        .setBoolean(BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, false);

    // Mock method calls.
    when(mockBigquery.datasets()).thenReturn(mockBigqueryDatasets);
    when(mockBigqueryDatasets.delete(any(String.class), any(String.class)))
        .thenReturn(mockBigqueryDatasetsDelete);
    when(mockBigqueryDatasetsDelete.setDeleteContents(true)).thenReturn(mockBigqueryDatasetsDelete);


    // Run method and verify calls.
    committerInstance.commitJob(jobContext);
    verify(mockBigquery).datasets();
    verify(mockBigqueryDatasets).delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID));
    verify(mockBigqueryDatasetsDelete).setDeleteContents(true);
    verify(mockBigqueryDatasetsDelete, times(1)).execute();
  }

  /**
   * Tests the commitTask method of BigQueryOutputFormat.
   */
  @Test
  public void testCommitTask() 
      throws IOException {
    // Set mock JobReference.
    JobReference mockJobReference = new JobReference();

    // Create the job result to return.
    Job job = new Job();
    JobStatus jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);
    job.setStatus(jobStatus);
    job.setJobReference(mockJobReference);

    // Mock the return of the commit task method calls.
    Bigquery.Jobs jobs = mock(Bigquery.Jobs.class);
    Bigquery.Jobs.Insert jobInsert = mock(Bigquery.Jobs.Insert.class);
    Jobs mockBigQueryJobs = mock(Bigquery.Jobs.class);
    Get mockJobsGet = mock(Bigquery.Jobs.Get.class);
    when(jobs.insert(eq(JOB_PROJECT_ID), any(Job.class))).thenReturn(jobInsert);
    when(jobInsert.execute()).thenReturn(job);
    when(mockBigquery.jobs()).thenReturn(jobs).thenReturn(mockBigQueryJobs);
    when(mockBigQueryJobs.get(JOB_PROJECT_ID, mockJobReference.getJobId()))
        .thenReturn(mockJobsGet).thenReturn(mockJobsGet);
    when(mockJobsGet.execute()).thenReturn(job);

    // Run method and verify calls.
    committerInstance.commitTask(context);
    verify(mockBigquery, times(2)).jobs();

    // Verify the contents of the Job.
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobs).insert(eq(JOB_PROJECT_ID), jobCaptor.capture());
    Job capturedJob = jobCaptor.getValue();
    assertEquals(tempTableRef, capturedJob.getConfiguration().getCopy().getSourceTable());
    assertEquals(finalTableRef, capturedJob.getConfiguration().getCopy().getDestinationTable());

    verify(jobInsert).execute();
    verify(mockBigQueryJobs).get(JOB_PROJECT_ID, mockJobReference.getJobId());
    verify(mockJobsGet).execute();
  }

  /**
   * Tests the commitTask method of BigQueryOutputFormat with error thrown.
   */
  @Test
  public void testCommitTaskError() 
      throws IOException {
    // Set mock JobReference.
    JobReference mockJobReference = new JobReference();

    // Create the job result to return.
    Job job = new Job();
    JobStatus jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);
    job.setStatus(jobStatus);
    job.setJobReference(mockJobReference);

    // Mock the return of the commit task method calls.
    Bigquery.Jobs jobs = mock(Bigquery.Jobs.class);
    Bigquery.Jobs.Insert jobInsert = mock(Bigquery.Jobs.Insert.class);
    Jobs mockBigQueryJobs = mock(Bigquery.Jobs.class);
    Get mockJobsGet = mock(Bigquery.Jobs.Get.class);
    when(jobs.insert(eq(JOB_PROJECT_ID), any(Job.class))).thenReturn(jobInsert);
    when(jobInsert.execute()).thenReturn(job);
    when(mockBigquery.jobs()).thenReturn(jobs).thenReturn(mockBigQueryJobs);
    when(mockBigQueryJobs.get(JOB_PROJECT_ID, mockJobReference.getJobId()))
        .thenReturn(mockJobsGet).thenReturn(mockJobsGet);
    when(mockJobsGet.execute()).thenReturn(job);

    // Run method and verify calls.
    committerInstance.commitTask(context);
    verify(mockBigquery, times(2)).jobs();
    verify(jobs).insert(eq(JOB_PROJECT_ID), any(Job.class));
    verify(jobInsert).execute();
    verify(mockBigQueryJobs).get(JOB_PROJECT_ID, mockJobReference.getJobId());
    verify(mockJobsGet).execute();
  }

  /**
   * Tests the abortTask method of BigQueryOutputFormat.
   */
  @Test
  public void testAbortTask() 
      throws IOException {
    // Mock method calls.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);
    when(mockBigqueryTables.delete(any(String.class), any(String.class), any(String.class)))
        .thenReturn(mockBigqueryTablesDelete);

    // Run method and verify calls.
    committerInstance.abortTask(context);
    verify(mockBigquery).tables();
    verify(mockBigqueryTables).delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID), eq(TEMP_TABLE_ID));
    verify(mockBigqueryTablesDelete).execute();
  }

  /**
   * Tests the abortTask method of BigQueryOutputFormat when error is thrown.
   */
  @Test
  public void testAbortTaskError() 
      throws IOException {
    // Mock method calls.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);
    when(mockBigqueryTables.delete(any(String.class), any(String.class), any(String.class)))
        .thenThrow(new IOException());
    // Run method and verify calls.
    committerInstance.abortTask(context);
    verify(mockBigqueryTables).delete(eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID), eq(TEMP_TABLE_ID));
    verify(mockBigquery, times(1)).tables();
  }

  /**
   * Tests the needsTaskCommit method of BigQueryOutputFormat.
   */
  @Test
  public void testNeedsTaskCommit() 
      throws IOException {
    // Create a list of tables to mock the current Bigquery state.
    List<TableList.Tables> tables = new ArrayList<>();
    TableList.Tables table = new TableList.Tables();
    TableReference tableReference = new TableReference();
    tableReference.setTableId(TEMP_TABLE_ID);
    table.setTableReference(tableReference);
    tables.add(table);

    // Bigquery state with no written table in directory.
    TableList nullTableList = new TableList();

    // Bigquery state with table written to directory.
    TableList oneTableList = new TableList();
    oneTableList.setTables(tables);

    // Mock method calls.
    Bigquery.Tables mockBigqueryTables = mock(Bigquery.Tables.class);
    Bigquery.Tables.List mockTablesList = mock(Bigquery.Tables.List.class);
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);
    when(mockBigqueryTables.list(
        eq(TEMP_PROJECT_ID), eq(TEMP_DATASET_ID))).thenReturn(mockTablesList);
    when(mockTablesList.execute()).thenReturn(nullTableList).thenReturn(oneTableList);

    // Run method and verify calls.
    Assert.assertEquals(false, committerInstance.needsTaskCommit(context));
    Assert.assertEquals(true, committerInstance.needsTaskCommit(context));
    verify(mockBigquery, times(2)).tables();
  }
}
