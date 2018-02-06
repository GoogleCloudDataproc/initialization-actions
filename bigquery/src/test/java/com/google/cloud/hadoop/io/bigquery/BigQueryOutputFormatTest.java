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
import static org.junit.Assert.fail;

import com.google.cloud.hadoop.testing.CredentialConfigurationUtil;
import com.google.gson.JsonObject;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/**
 * Unit tests for BigQueryOutputFormat.
 */
@RunWith(JUnit4.class)
public class BigQueryOutputFormatTest {
  public static final int DEFAULT_TEST_OUTPUT_BUFFER_SIZE = 1 * 1024 * 1024;

  // OutputFormat to use for testing.
  private BigQueryOutputFormat<LongWritable, JsonObject> outputFormat;

  // Sample output schema.
  private static String tableSchema =
      "[{'name': 'Name','type': 'STRING'},{'name': 'Number','type': 'INTEGER'}]";

  // Sample projectId for the owner of the BigQuery job.
  private static String jobProjectId = "test_job_project";

  // Sample projectId for the owner of the output table.
  private static String outputProjectId = "test_output_project";

  // Sample tableId.
  private static String tableId = "test_table";

  // Sample datasetId.
  private static String datasetId = "test_dataset";

  // Sample numRecordsInBatch.
  private static String bufferSize = Integer.toString(DEFAULT_TEST_OUTPUT_BUFFER_SIZE);

  /**
   * Sets up common objects for testing before each test.
   */
  @Before
  public void setUp() {
    // Construct the OutputFormat.
    outputFormat = new BigQueryOutputFormat<>();
  }

  /** Tests the checkOutputSpecs method of BigQueryOutputFormat. */
  @Test
  public void testCheckOutputSpecs() throws IOException {
    // Initialize the jobs.
    JobContext goodContext = getJobContext(
        tableSchema, jobProjectId, outputProjectId, tableId, datasetId, bufferSize);
    JobContext nullNumRecords =
        getJobContext(tableSchema, jobProjectId, outputProjectId, tableId, datasetId, null);
    JobContext emptyTableSchema =
        getJobContext("", jobProjectId, outputProjectId, tableId, datasetId, bufferSize);
    JobContext nullTableSchema =
        getJobContext(null, jobProjectId, outputProjectId, tableId, datasetId, bufferSize);
    JobContext emptyJobProjectId =
        getJobContext(tableSchema, "", outputProjectId, tableId, datasetId, bufferSize);
    JobContext nullJobProjectId =
        getJobContext(tableSchema, null, outputProjectId, tableId, datasetId, bufferSize);
    JobContext emptyOutputProjectId =
        getJobContext(tableSchema, jobProjectId, "", tableId, datasetId, bufferSize);
    JobContext nullOutputProjectId =
        getJobContext(tableSchema, jobProjectId, null, tableId, datasetId, bufferSize);
    JobContext emptyTableId =
        getJobContext(tableSchema, jobProjectId, outputProjectId, "", datasetId, bufferSize);
    JobContext nullTableId = getJobContext(
        tableSchema, jobProjectId, outputProjectId, null, datasetId, bufferSize);
    JobContext emptyDatasetId =
        getJobContext(tableSchema, jobProjectId, outputProjectId, tableId, "", bufferSize);
    JobContext nullDatasetId =
        getJobContext(tableSchema, jobProjectId, outputProjectId, tableId, null, bufferSize);
    JobContext zeroNumRecords =
        getJobContext(tableSchema, jobProjectId, outputProjectId, tableId, datasetId, "0");
    JobContext negativeNumRecords =
        getJobContext(tableSchema, jobProjectId, outputProjectId, tableId, datasetId, "-1");

    // Assert checkOutputSpecs succeeds.
    outputFormat.checkOutputSpecs(goodContext);
    outputFormat.checkOutputSpecs(nullNumRecords);

    // Assert checkOutputSpecs fails with empty fields.
    assertCheckOutputSpecsFailure(
        emptyTableSchema, "Empty is not a valid setting for the tableSchema parameter.");
    // Assert checkOutputSpecs fails with null fields.
    assertCheckOutputSpecsFailure(
        nullTableSchema, "Null is not a valid setting for the tableSchema parameter.");

    // Assert checkOutputSpecs fails with empty jobProjectId.
    assertCheckOutputSpecsFailure(
        emptyJobProjectId, "Empty is not a valid setting for the jobProjectId parameter.");
    // Assert checkOutputSpecs fails with null jobProjectId.
    assertCheckOutputSpecsFailure(
        nullJobProjectId, "Null is not a valid setting for the jobProjectId parameter.");

    // Assert checkOutputSpecs fails with empty OutputProjectId.
    assertCheckOutputSpecsFailure(
        emptyOutputProjectId, "Empty is not a valid setting for the OutputProjectId parameter.");
    // Assert checkOutputSpecs fails with null OutputProjectId.
    assertCheckOutputSpecsFailure(
        nullOutputProjectId, "Null is not a valid setting for the OutputProjectId parameter.");

    // Assert checkOutputSpecs fails with empty tableId.
    assertCheckOutputSpecsFailure(
        emptyTableId, "Empty is not a valid setting for the tableId parameter.");
    // Assert checkOutputSpecs fails with null tableId.
    assertCheckOutputSpecsFailure(
        nullTableId, "Null is not a valid setting for the tableId parameter.");

    // Assert checkOutputSpecs fails with empty datasetId.
    assertCheckOutputSpecsFailure(
        emptyDatasetId, "Empty is not a valid setting for the datasetId parameter.");
    // Assert checkOutputSpecs fails with null datasetId.
    assertCheckOutputSpecsFailure(
        nullDatasetId, "Null is not a valid setting for the datasetId parameter.");

    // Assert checkOutputSpecs fails with zero numRecordsInBatch.
    assertCheckOutputSpecsFailure(
        zeroNumRecords, "Zero is not a valid setting for the numRecordsInBatch parameter.");
    // Assert checkOutputSpecs fails with negative numRecordsInBatch.
    assertCheckOutputSpecsFailure(
        negativeNumRecords, "Negative is not a valid setting for the numRecordsInBatch parameter.");
  }

  /**
   * Tests the getOutputCommitter method of BigQueryOutputFormat.
   */
  @Test
  public void testGetOutputCommitter() 
      throws IOException, InterruptedException {
    // Create the configuration.
    JobConf jobConf = new JobConf();
    BigQueryConfiguration.configureBigQueryOutput(
        jobConf,
        outputProjectId,
        datasetId,
        tableId,
        tableSchema);
    CredentialConfigurationUtil.addTestConfigurationSettings(jobConf);

    // Create the job and context.
    Job job = new Job(jobConf, "testGetOutputCommitter");

    TaskAttemptID taskAttemptID = new TaskAttemptID(
        new TaskID(new JobID("", 1), false /* isMap */, 1), 1);
    // Get the OutputCommitter
    BigQueryOutputCommitter outputCommitter =
        outputFormat.getOutputCommitter(job.getConfiguration(), taskAttemptID);
    try {
      outputCommitter.needsTaskCommit(taskAttemptID);
      Assert.fail();
    } catch (IOException e) {
      // Expected.
    }
  }

  /**
   * Tests the getRecordWriter method of BigQueryOutputFormat.
   */
  @Test
  public void testGetRecordWriter() 
      throws IOException {
    // Create the configuration.
    Configuration conf = new Configuration();
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, jobProjectId);
    conf.set(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY, outputProjectId);
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, tableId);
    conf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, datasetId);
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY, tableSchema);
    CredentialConfigurationUtil.addTestConfigurationSettings(conf);

    // Create the job and context.
    Job job = new Job(conf, "testGetOutputCommitter");
    TaskAttemptID taskAttemptId = new TaskAttemptID(new TaskID(
        new JobID("", 1), false /* isMap */, 1), 1);
    TaskAttemptContext context = Mockito.mock(TaskAttemptContext.class);
    Mockito.when(context.getConfiguration()).thenReturn(conf);
    Mockito.when(context.getTaskAttemptID()).thenReturn(taskAttemptId);
    RecordWriter<LongWritable, JsonObject> recordWriter = outputFormat.getRecordWriter(context);
    assertThat(recordWriter).isInstanceOf(BigQueryRecordWriter.class);
  }

  /**
   * Tests the getUniqueTable method of BigQueryOutputFormat.
   */
  @Test
  public void testGetUniqueTable() 
      throws IOException {
    // Sample tableId for testing.
    String tableId = "test_tableId";

    // Create the configuration.
    Configuration conf = new Configuration();
    CredentialConfigurationUtil.addTestConfigurationSettings(conf);

    // Create the job and context.
    Job job = new Job(conf, "testGetOutputCommitter");

    // The task index out of N parallel tasks.
    int taskNumber = 3;

    // The attempt number for the same task index.
    int taskAttempt = 2;

    // Job number; the Nth job the user submitted.
    int jobNumber = 42;
    String jobIdString = "201402240120";
    JobID jobId = new JobID(jobIdString, jobNumber);
    TaskAttemptID taskAttemptId =
        new TaskAttemptID(new TaskID(jobId, false, taskNumber), taskAttempt);

    String uniqueTable = BigQueryOutputFormat.getUniqueTable(taskAttemptId.toString(), tableId);
    assertThat(uniqueTable)
        .isEqualTo(
            String.format(
                "test_tableId_attempt_%s_%04d_r_%06d_%d",
                jobIdString, jobNumber, taskNumber, taskAttempt));

    tableId = "test_tableId$20160808";
    uniqueTable = BigQueryOutputFormat.getUniqueTable(taskAttemptId.toString(), tableId);
    assertThat(uniqueTable)
        .isEqualTo(
            String.format(
                "test_tableId__20160808_attempt_%s_%04d_r_%06d_%d",
                jobIdString, jobNumber, taskNumber, taskAttempt));
  }


  /**
   * Tests the getUniqueTable method of BigQueryOutputFormat.
   */
  @Test
  public void testGetTempDataset() 
      throws IOException {
    // Sample tableId for testing.
    String datasetId = "test_datasetId";

    // Create the configuration.
    Configuration conf = new Configuration();
    CredentialConfigurationUtil.addTestConfigurationSettings(conf);
    conf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, datasetId);
    // Create the job and context.
    Job job = new Job(conf, "testGetOutputCommitter");
    TaskAttemptID taskAttemptId = new TaskAttemptID("foojob12345", 42, true, 55, 1);
    String tempDataset = BigQueryOutputFormat.getTempDataset(conf, taskAttemptId);
    assertThat(tempDataset)
        .isEqualTo(datasetId + BigQueryOutputFormat.TEMP_NAME + "job_foojob12345_0042");
  }

  /**
   * Helper method to assert checkOutputSpecs fails for a given JobContext.
   *
   * @param jobContext the JobContext to test.
   */
  public void assertCheckOutputSpecsFailure(JobContext jobContext, String message) {
    try {
      outputFormat.checkOutputSpecs(jobContext);
      fail(message);
    } catch (IllegalArgumentException | IOException e) {
      // Expected.
    }
  }

  /**
   * Helper method to return a JobContext with the datasetId and numRecordsInBatch set.
   *
   * @param tableSchema the schema of the records to write.
   * @param jobProjectId the project owning the BigQuery jobs.
   * @param outputProjectId the project owning the table to write records into.
   * @param tableId the table to write records into.
   * @param datasetId the dataset to write records into.
   * @param writeBufferSize the size of the write buffer, in bytes.
   */
  public JobContext getJobContext(String tableSchema, String jobProjectId, String outputProjectId,
      String tableId, String datasetId, String writeBufferSize) throws IOException {
    // Initialize the job.
    Configuration conf = new Configuration();
    CredentialConfigurationUtil.addTestConfigurationSettings(conf);
    if (tableSchema != null) {
      conf.set(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY, tableSchema);
    }
    if (jobProjectId != null) {
      conf.set(BigQueryConfiguration.PROJECT_ID_KEY, jobProjectId);
    }
    if (outputProjectId != null) {
      conf.set(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY, outputProjectId);
    }
    if (tableId != null) {
      conf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, tableId);
    }
    if (datasetId != null) {
      conf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, datasetId);
    }
    if (writeBufferSize != null) {
      conf.set(BigQueryConfiguration.OUTPUT_WRITE_BUFFER_SIZE_KEY, writeBufferSize);
    }
    return Job.getInstance(conf);
  }
}
