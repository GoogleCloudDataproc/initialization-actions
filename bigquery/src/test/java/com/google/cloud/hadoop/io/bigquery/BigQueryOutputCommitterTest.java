/**
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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.fs.gcs.InMemoryGoogleHadoopFileSystem;
import com.google.cloud.hadoop.testing.CredentialConfigurationUtil;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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

  private TaskAttemptID fakeTaskId;

  private JobReference jobReference;

  @Mock private Bigquery mockBigquery;
  @Mock private Bigquery.Datasets mockBigqueryDatasets;
  @Mock private Bigquery.Datasets.Delete mockBigqueryDatasetsDelete;
  @Mock private Bigquery.Datasets.Insert mockBigqueryDatasetsInsert;
  @Mock private Bigquery.Tables mockBigqueryTables;
  @Mock private Bigquery.Tables.Delete mockBigqueryTablesDelete;
  @Mock private Bigquery.Tables.Get mockBigqueryTablesGet;
  @Mock private Bigquery.Jobs mockBigqueryJobs;
  @Mock private Bigquery.Jobs.Insert mockBigqueryJobsInsert;
  @Mock private Bigquery.Jobs.Get mockBigqueryJobsGet;
  @Mock private BigQueryHelper mockBigQueryHelper;
  @Mock private TaskAttemptContext mockTaskAttemptContext;

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
            .setDatasetId(TEMP_DATASET_ID))
        .setLocation(conf.get(BigQueryConfiguration.DATA_LOCATION_KEY,
                              BigQueryConfiguration.DATA_LOCATION_DEFAULT));
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

    fakeTaskId = new TaskAttemptID(new TaskID("foo_task", 123, false, 42), 2);

    when(mockBigQueryHelper.getRawBigquery())
        .thenReturn(mockBigquery);

    jobReference = new JobReference()
        .setProjectId(JOB_PROJECT_ID)
        .setJobId("foo_task_123_r_42_2_12345");

    when(mockBigquery.jobs())
        .thenReturn(mockBigqueryJobs);
    when(mockBigqueryJobs.insert(any(String.class), any(Job.class)))
        .thenReturn(mockBigqueryJobsInsert);
    when(mockBigqueryJobs.get(any(String.class), any(String.class)))
        .thenReturn(mockBigqueryJobsGet);

    // Set OutputCommitter.
    committerInstance =
        new BigQueryOutputCommitter(JOB_PROJECT_ID, tempTableRef, finalTableRef, conf);
    committerInstance.setBigQueryHelper(mockBigQueryHelper);
  }

  /**
   * Verifies there are no more interactions.
   */
  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockBigquery);
    verifyNoMoreInteractions(mockBigqueryTables);
    verifyNoMoreInteractions(mockBigqueryTablesDelete);
    verifyNoMoreInteractions(mockBigqueryTablesGet);
    verifyNoMoreInteractions(mockBigqueryDatasets);
    verifyNoMoreInteractions(mockBigqueryDatasetsInsert);
    verifyNoMoreInteractions(mockBigqueryDatasetsDelete);
    verifyNoMoreInteractions(mockBigQueryHelper);
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
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
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
    committerInstance.setupTask(mockTaskAttemptContext);
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
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
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
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
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
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
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
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
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
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
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
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
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
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
  }

  /**
   * Tests the commitTask method of BigQueryOutputFormat.
   */
  @Test
  public void testCommitTask() 
      throws IOException {
    // Create the job result to return.
    Job job = new Job();
    JobStatus jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);
    job.setStatus(jobStatus);
    job.setJobReference(jobReference);

    // Mock the return of the commit task method calls.
    when(mockBigqueryJobsGet.execute()).thenReturn(job);

    when(mockTaskAttemptContext.getTaskAttemptID())
        .thenReturn(fakeTaskId);
    when(mockBigQueryHelper.createJobReference(any(String.class), any(String.class)))
        .thenReturn(jobReference);
    when(mockBigQueryHelper.insertJobOrFetchDuplicate(any(String.class), any(Job.class)))
        .thenReturn(job);

    // Run method and verify calls.
    committerInstance.commitTask(mockTaskAttemptContext);

    verify(mockBigquery, times(1)).jobs();

    // Verify the contents of the Job.
    ArgumentCaptor<Job> jobCaptor = ArgumentCaptor.forClass(Job.class);
    verify(mockBigQueryHelper).insertJobOrFetchDuplicate(eq(JOB_PROJECT_ID), jobCaptor.capture());
    Job capturedJob = jobCaptor.getValue();
    assertThat(capturedJob.getConfiguration().getCopy().getSourceTable()).isEqualTo(tempTableRef);
    assertThat(capturedJob.getConfiguration().getCopy().getDestinationTable())
        .isEqualTo(finalTableRef);

    verify(mockBigqueryJobs).get(JOB_PROJECT_ID, jobReference.getJobId());
    verify(mockBigqueryJobsGet).execute();
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();

    verify(mockTaskAttemptContext, atLeastOnce()).getTaskAttemptID();
    verify(mockBigQueryHelper).createJobReference(
        eq(JOB_PROJECT_ID), eq(fakeTaskId.toString()));
  }

  /**
   * Tests the commitTask method of BigQueryOutputFormat with error set in JobStatus.
   */
  @Test
  public void testCommitTaskError() 
      throws IOException {
    // Create the job result to return.
    Job job = new Job();
    JobStatus jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);
    job.setStatus(jobStatus);
    job.setJobReference(jobReference);

    // Mock the return of the commit task method calls.
    when(mockBigqueryJobsGet.execute()).thenReturn(job);

    when(mockTaskAttemptContext.getTaskAttemptID())
        .thenReturn(fakeTaskId);
    when(mockBigQueryHelper.createJobReference(any(String.class), any(String.class)))
        .thenReturn(jobReference);
    when(mockBigQueryHelper.insertJobOrFetchDuplicate(any(String.class), any(Job.class)))
        .thenReturn(job);

    // Run method and verify calls.
    committerInstance.commitTask(mockTaskAttemptContext);

    verify(mockBigquery, times(1)).jobs();
    verify(mockBigQueryHelper).insertJobOrFetchDuplicate(eq(JOB_PROJECT_ID), any(Job.class));
    verify(mockBigqueryJobs).get(JOB_PROJECT_ID, jobReference.getJobId());
    verify(mockBigqueryJobsGet).execute();
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();

    verify(mockTaskAttemptContext, atLeastOnce()).getTaskAttemptID();
    verify(mockBigQueryHelper).createJobReference(
        eq(JOB_PROJECT_ID), eq(fakeTaskId.toString()));
  }

  /**
   * Tests the commitTask method of BigQueryOutputFormat with unhandled exception thrown on insert.
   */
  @Test
  public void testCommitTaskUnhandledException() 
      throws IOException {
    when(mockTaskAttemptContext.getTaskAttemptID())
        .thenReturn(fakeTaskId);
    when(mockBigQueryHelper.createJobReference(any(String.class), any(String.class)))
        .thenReturn(jobReference);
    IOException fakeUnhandledException = new IOException("fake unhandled exception");
    when(mockBigQueryHelper.insertJobOrFetchDuplicate(any(String.class), any(Job.class)))
        .thenThrow(fakeUnhandledException);

    // Run method and verify calls.
    IOException ioe =
        assertThrows(IOException.class, () -> committerInstance.commitTask(mockTaskAttemptContext));
    assertThat(ioe).isEqualTo(fakeUnhandledException);

    verify(mockBigQueryHelper).insertJobOrFetchDuplicate(eq(JOB_PROJECT_ID), any(Job.class));
    verify(mockTaskAttemptContext, atLeastOnce()).getTaskAttemptID();
    verify(mockBigQueryHelper).createJobReference(
        eq(JOB_PROJECT_ID), eq(fakeTaskId.toString()));
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
    committerInstance.abortTask(mockTaskAttemptContext);
    // Tear down verifies no calls are made.
  }

  /**
   * Tests the needsTaskCommit method of BigQueryOutputFormat.
   */
  @Test
  public void testNeedsTaskCommit() 
      throws IOException {
    when(mockBigQueryHelper.tableExists(any(TableReference.class)))
        .thenReturn(false)
        .thenReturn(true);

    // Run method and verify calls.
    assertThat(committerInstance.needsTaskCommit(mockTaskAttemptContext)).isFalse();
    assertThat(committerInstance.needsTaskCommit(mockTaskAttemptContext)).isTrue();

    verify(mockBigQueryHelper, times(2)).tableExists(eq(tempTableRef));
    verify(mockBigQueryHelper, never()).getRawBigquery();
  }
}
