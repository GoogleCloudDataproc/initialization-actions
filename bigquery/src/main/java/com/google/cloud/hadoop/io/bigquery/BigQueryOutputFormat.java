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

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * @deprecated Use {@link IndirectBigQueryOutputFormat} instead.
 *     <p>An OutputFormat that sends the output of a Hadoop job directly to BigQuery.
 *     <p>BigQueryOutputFormat accepts key, value pairs, but the returned BigQueryRecordWriter
 *     writes only the value to the database as each BigQuery value already contains a BigQuery key.
 *     <p>This is deprecated because it creates many temporary tables, which can drain user quota.
 * @param <K> Key type.
 * @param <V> Value type must be JsonObject or a derived type..
 */
@Deprecated
public class BigQueryOutputFormat<K, V extends JsonObject> extends OutputFormat<K, V> {
  // Construct format for output table name.
  public static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  // Suffix to add to output datasetId to get the temporary working datasetId before also
  // appending the JobID as the final suffix for the temporary datasetId.
  public static final String TEMP_NAME = "_hadoop_temporary_";

  protected static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Checks for validity of the output-specification for the job. Typically checks that it does not
   * already exist, throwing an exception when it already exists, so that output is not overwritten.
   * However as we only add entities to a table and are not overwriting, this method only checks if
   * the fields, projectId, tableId, datasetId, are not null or empty and if the numRecordsInBatch
   * is a positive int.
   *
   * <p>TODO(user): check fields is a properly formatted TableSchema.
   *
   * @param context the job's context.
   * @throws IOException on IO Error.
   */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException {
    // Check the proper values in the configuration are set.
    ConfigurationUtil.getMandatoryConfig(
        context.getConfiguration(), BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_OUTPUT);

    // Check that numRecordsInBatch is a positive int.
    Preconditions.checkArgument(
        context.getConfiguration().getInt(
            BigQueryConfiguration.OUTPUT_WRITE_BUFFER_SIZE_KEY,
            BigQueryConfiguration.OUTPUT_WRITE_BUFFER_SIZE_DEFAULT) >= 1,
        "Output write buffer size should be a positive integer.");
  }

  /**
   * Gets the OutputCommiter which ensures that the temporary files are cleaned up and output
   * commits are scheduled.
   *
   * @param context the task's context.
   * @throws InterruptedException on Interrupt.
   * @throws IOException on IO Error.
   */
  @Override
  public BigQueryOutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return getOutputCommitter(context.getConfiguration(), context.getTaskAttemptID());
  }

  @VisibleForTesting
  public BigQueryOutputCommitter getOutputCommitter(
      Configuration configuration, TaskAttemptID taskAttemptId)
      throws IOException, InterruptedException {
    // Check the proper values in the configuration are set.
    ConfigurationUtil.getMandatoryConfig(
        configuration, BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_OUTPUT);
    // Get parameters from the context.
    String projectId = configuration.get(BigQueryConfiguration.PROJECT_ID_KEY);

    TableReference tempTableRef = getTempTableReference(configuration, taskAttemptId);
    TableReference finalTableRef = getFinalTableReference(configuration);

    logger.atFine().log(
        "Returning BigQueryOutputCommitter('%s', '%s', '%s'",
        projectId, BigQueryStrings.toString(tempTableRef), BigQueryStrings.toString(finalTableRef));
    return new BigQueryOutputCommitter(projectId, tempTableRef, finalTableRef, configuration);
  }

  /**
   * Returns a new RecordWriter for writing outputs to the BigQuery.
   *
   * @param context the task's context.
   * @throws IOException on IOError.
   */
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
    // Check the proper values in the configuration are set.
    Map<String, String> mandatoryConfig = ConfigurationUtil.getMandatoryConfig(
        context.getConfiguration(), BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_OUTPUT);
    // Get RecordWriter parameters from the configuration.
    int writeBufferSize =
        context.getConfiguration().getInt(
            BigQueryConfiguration.OUTPUT_WRITE_BUFFER_SIZE_KEY,
            BigQueryConfiguration.OUTPUT_WRITE_BUFFER_SIZE_DEFAULT);
    String jobProjectId = mandatoryConfig.get(BigQueryConfiguration.PROJECT_ID_KEY);
    String tableSchema = mandatoryConfig.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY);

    TableReference tempTableRef =
        getTempTableReference(context.getConfiguration(), context.getTaskAttemptID());

    logger.atFine().log(
        "Returning new BigqueryRecordWriter for fields: '%s', project: '%s', table: '%s'",
        tableSchema, jobProjectId, BigQueryStrings.toString(tempTableRef));
    // Return a new BigQueryRecordWriter.
    return new BigQueryRecordWriter<>(
        context.getConfiguration(),
        context,
        context.getTaskAttemptID().toString(),
        BigQueryUtils.getSchemaFromString(tableSchema),
        jobProjectId,
        tempTableRef,
        writeBufferSize);
  }

  /**
   * Retrieves the fully-qualified TableReference for the desired final destination table for the
   * output.
   */
  static TableReference getFinalTableReference(Configuration configuration) {
    String outputProjectId = configuration.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY);
    String outputDatasetId = configuration.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY);
    String outputTableId = configuration.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY);

    TableReference finalTableRef = new TableReference()
        .setProjectId(outputProjectId)
        .setDatasetId(outputDatasetId)
        .setTableId(outputTableId);
    return finalTableRef;
  }

  /**
   * Deterministically generates a full TempTableReference based on the desired final output table
   * specified in {@code context}.
   */
  static TableReference getTempTableReference(
      Configuration configuration, TaskAttemptID taskAttemptId) {
    String outputProjectId =
        configuration.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY);
    String outputTableId =
        configuration.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY);

    String outputTempDatasetId = getTempDataset(configuration, taskAttemptId);
    String outputTempTable = getUniqueTable(taskAttemptId.toString(), outputTableId);

    TableReference tempTableRef = new TableReference()
        .setProjectId(outputProjectId)
        .setDatasetId(outputTempDatasetId)
        .setTableId(outputTempTable);
    return tempTableRef;
  }

  /**
   * Generates the temporary dataset for a particular context.
   *
   * @return a temporary datasetId for the working directory.
   */
  static String getTempDataset(Configuration configuration, TaskAttemptID taskAttemptId) {
    return configuration.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY) + TEMP_NAME
        + taskAttemptId.getJobID().toString();
  }

  /**
   * Generates a unique table name, based on the task id and table name.
   *
   * @param taskAttemptId The string ID for the task attempt.
   * @param tableId the final output table id.
   * @return a string like [tableId]_attempt_..._r_00001_1.
   */
  static String getUniqueTable(String taskAttemptId, String tableId) {
    return String.format(
        "%s_%s", tableId.replace("$", "__"), taskAttemptId.toString());
  }
}
