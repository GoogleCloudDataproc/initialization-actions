package com.google.cloud.hadoop.io.bigquery;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

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

  // The Job Configuration for testing.
  private static JobConf conf;

  /**
   * Set up before all classes.
   */
  @Before
  public void setUp() {
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

    Configuration config = new Configuration();
    conf = new JobConf(config);
    new BigQueryConfiguration();
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
