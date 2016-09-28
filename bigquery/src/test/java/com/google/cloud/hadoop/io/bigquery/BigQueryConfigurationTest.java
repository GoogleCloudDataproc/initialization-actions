package com.google.cloud.hadoop.io.bigquery;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.fs.gcs.InMemoryGoogleHadoopFileSystem;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for BigQueryConfigurationTest.
 */
@RunWith(JUnit4.class)
public class BigQueryConfigurationTest {
  // Sample jobProjectId.
  private static String jobProjectId;

  // Sample projectId for input.
  private static String inputProjectId;

  // Sample datasetId for input.
  private static String inputDatasetId;

  // Sample tableId for input.
  private static String inputTableId;

  // Sample projectId for output.
  private static String outputProjectId;

  // Sample datasetId for output.
  private static String outputDatasetId;

  // Sample tableId for output.
  private static String outputTableId;

  // Sample query for testing for output.
  private static String outputTableSchema;

  // Sample gcs bucket for io.
  private static String gcsBucket;

  // Sample gcs temporary path for io.
  private static String gcsTempPath;

  // The Job Configuration for testing.
  private static JobConf conf;

  @Mock private JobID mockJobID;

  /**
   * Set up before all classes.
   *
   * @throws IOException on IOError.
   */
  @Before
  public void setUp() throws IOException {
    // Generate Mocks.
    MockitoAnnotations.initMocks(this);

    jobProjectId = "google.com:foo-project";

    // Set input parameters for testing.
    inputProjectId = "google.com:input-project";
    inputDatasetId = "test_input_dataset";
    inputTableId = "test_input_table";

    // Set output parameters for testing.
    outputProjectId = "google.com:output-project";
    outputDatasetId = "test_output_dataset";
    outputTableId = "test_output_table";
    outputTableSchema = "test_schema";

    // Set GSC io parameters for testing.
    gcsBucket = "test";
    gcsTempPath = "gs://test";

    // Generate a sample configuration to properly handle gs:// paths.
    Configuration config = InMemoryGoogleHadoopFileSystem.getSampleConfiguration();
    conf = new JobConf(config);
    new BigQueryConfiguration();
  }

  /**
   * Tests the BigQueryConfiguration getTemporaryPathRoot method's response for a custom path.
   *
   * @throws IOException on IOError.
   */
  @Test
  public void testGetTemporaryPathRootSpecific() throws IOException {
    // Set an explicit path.
    conf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, gcsTempPath);

    assertEquals(gcsTempPath, BigQueryConfiguration.getTemporaryPathRoot(conf, mockJobID));
  }

  /**
   * Tests the BigQueryConfiguration getTemporaryPathRoot method's default response.
   *
   * @throws IOException on IOError.
   */
  @Test
  public void testGetTemporaryPathRootDefault() throws IOException {
    // Set the bucket for the default path.
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket);

    // Mock the JobID's toString which is used to generate the temporary path.
    when(mockJobID.toString()).thenReturn("test_job_id");

    checkNotNull(BigQueryConfiguration.getTemporaryPathRoot(conf, mockJobID));
  }

  /**
   * Tests the BigQueryConfiguration configureBigQueryInput method.
   */
  @Test
  public void testConfigureBigQueryInput() throws IOException {
    BigQueryConfiguration.configureBigQueryInput(
        conf,
        inputProjectId,
        inputDatasetId,
        inputTableId);
    assertEquals(inputProjectId, conf.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY));
    assertEquals(inputDatasetId, conf.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY));
    assertEquals(inputTableId, conf.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY));

    // By default, the job-level projectId inherits the input projectId if it's not already set.
    assertEquals(inputProjectId, conf.get(BigQueryConfiguration.PROJECT_ID_KEY));
  }

  /**
   * Tests the BigQueryConfiguration configureBigQueryOutput method.
   */
  @Test
  public void testConfigureBigQueryOutput() throws IOException {
    BigQueryConfiguration.configureBigQueryOutput(
        conf,
        outputProjectId,
        outputDatasetId,
        outputTableId,
        outputTableSchema);
    assertEquals(outputProjectId, conf.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY));
    assertEquals(outputDatasetId, conf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY));
    assertEquals(outputTableId, conf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY));
    assertEquals(outputTableSchema, conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY));

    // By default, the job-level projectId inherits the output projectId if it's not already set.
    assertEquals(outputProjectId, conf.get(BigQueryConfiguration.PROJECT_ID_KEY));
  }

  @Test
  public void testConfigureBigQueryInputThenOutput() throws IOException {
    BigQueryConfiguration.configureBigQueryInput(
        conf,
        inputProjectId,
        inputDatasetId,
        inputTableId);
    BigQueryConfiguration.configureBigQueryOutput(
        conf,
        outputProjectId,
        outputDatasetId,
        outputTableId,
        outputTableSchema);

    assertEquals(inputProjectId, conf.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY));
    assertEquals(inputDatasetId, conf.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY));
    assertEquals(inputTableId, conf.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY));
    assertEquals(outputProjectId, conf.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY));
    assertEquals(outputDatasetId, conf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY));
    assertEquals(outputTableId, conf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY));
    assertEquals(outputTableSchema, conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY));

    // Job level projectId got the inputProjectId just because we called it first.
    assertEquals(inputProjectId, conf.get(BigQueryConfiguration.PROJECT_ID_KEY));
  }

  @Test
  public void testConfigureBigQueryInputThenOutputWithPresetJobProject() throws IOException {
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, jobProjectId);
    BigQueryConfiguration.configureBigQueryInput(
        conf,
        inputProjectId,
        inputDatasetId,
        inputTableId);
    BigQueryConfiguration.configureBigQueryOutput(
        conf,
        outputProjectId,
        outputDatasetId,
        outputTableId,
        outputTableSchema);

    assertEquals(inputProjectId, conf.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY));
    assertEquals(inputDatasetId, conf.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY));
    assertEquals(inputTableId, conf.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY));
    assertEquals(outputProjectId, conf.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY));
    assertEquals(outputDatasetId, conf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY));
    assertEquals(outputTableId, conf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY));
    assertEquals(outputTableSchema, conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY));

    // Job level projectId remains unaltered by setting input/output projects.
    assertEquals(jobProjectId, conf.get(BigQueryConfiguration.PROJECT_ID_KEY));
  }

  @Test
  public void testConfigureBigQueryDefaultToJobProject() throws IOException {
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, jobProjectId);

    BigQueryConfiguration.configureBigQueryInput(
        conf,
        "",
        inputDatasetId,
        inputTableId);

    assertEquals(jobProjectId, conf.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY));
    assertEquals(inputDatasetId, conf.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY));
    assertEquals(inputTableId, conf.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY));

    BigQueryConfiguration.configureBigQueryOutput(
        conf,
        null,
        outputDatasetId,
        outputTableId,
        outputTableSchema);

    assertEquals(jobProjectId, conf.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY));
    assertEquals(outputDatasetId, conf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY));
    assertEquals(outputTableId, conf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY));
    assertEquals(outputTableSchema, conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY));

    // Job level projectId remains unaltered by setting input/output projects.
    assertEquals(jobProjectId, conf.get(BigQueryConfiguration.PROJECT_ID_KEY));
  }

  @Test
  public void testConfigureBigQueryDefaultToJobProjectFullyQualifiedNames() throws IOException {
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, jobProjectId);

    BigQueryConfiguration.configureBigQueryInput(
        conf, String.format("%s.%s", inputDatasetId, inputTableId));

    assertEquals(jobProjectId, conf.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY));
    assertEquals(inputDatasetId, conf.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY));
    assertEquals(inputTableId, conf.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY));

    BigQueryConfiguration.configureBigQueryOutput(
        conf, String.format("%s.%s", outputDatasetId, outputTableId), outputTableSchema);

    assertEquals(jobProjectId, conf.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY));
    assertEquals(outputDatasetId, conf.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY));
    assertEquals(outputTableId, conf.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY));
    assertEquals(outputTableSchema, conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY));

    // Job level projectId remains unaltered by setting input/output projects.
    assertEquals(jobProjectId, conf.get(BigQueryConfiguration.PROJECT_ID_KEY));
  }
}
