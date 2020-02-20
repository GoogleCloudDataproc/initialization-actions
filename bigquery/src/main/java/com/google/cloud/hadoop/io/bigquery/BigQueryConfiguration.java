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

import static com.google.cloud.hadoop.util.ConfigurationUtil.getMandatoryConfig;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.util.HadoopConfigurationProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;

/**
 * A container for configuration property names for jobs with BigQuery input/output.
 *
 *  The job can be configured programmatically using the static methods in this class,
 * GsonBigQueryInputFormat, and BigqueryOutputFormat}.
 *
 * Alternatively, the properties can be set in the configuration xml files with proper values.
 */
public class BigQueryConfiguration {

  public static final String BIGQUERY_CONFIG_PREFIX = "mapred.bq";

  /** Configuration key for the BigQuery API endpoint root URL. */
  public static final HadoopConfigurationProperty<String> BQ_ROOT_URL =
      new HadoopConfigurationProperty<>("mapred.bq.bigquery.root.url", Bigquery.DEFAULT_ROOT_URL);

  /**
   * Configuration key for project ID on whose behalf to perform BigQuery operations, and the
   * default project for referencing datasets when input/output datasets are not explicitly
   * specified.
   */
  public static final HadoopConfigurationProperty<String> PROJECT_ID =
      new HadoopConfigurationProperty<>("mapred.bq.project.id");

  // Configuration keys for input connector.

  /** Configuration key for project ID of the dataset accessed by this input connector. */
  public static final HadoopConfigurationProperty<String> INPUT_PROJECT_ID =
      new HadoopConfigurationProperty<>("mapred.bq.input.project.id");

  /** Configuration key for ID of the dataset accessed by this input connector. */
  public static final HadoopConfigurationProperty<String> INPUT_DATASET_ID =
      new HadoopConfigurationProperty<>("mapred.bq.input.dataset.id");

  /** Configuration key for ID of the table written by this input connector. */
  public static final HadoopConfigurationProperty<String> INPUT_TABLE_ID =
      new HadoopConfigurationProperty<>("mapred.bq.input.table.id");

  /** Configuration key for the GCS temp path this connector uses. */
  public static final HadoopConfigurationProperty<String> TEMP_GCS_PATH =
      new HadoopConfigurationProperty<>("mapred.bq.temp.gcs.path");

  /** Configuration key for the GCS bucket holding TEMP_GCS_PATH */
  public static final HadoopConfigurationProperty<String> GCS_BUCKET =
      new HadoopConfigurationProperty<>("mapred.bq.gcs.bucket");

  /** Configuration key for whether to delete the intermediate GCS-export files. */
  public static final HadoopConfigurationProperty<Boolean> DELETE_EXPORT_FILES_FROM_GCS =
      new HadoopConfigurationProperty<>("mapred.bq.input.export.files.delete", true);

  /**
   * Number of milliseconds to wait between listStatus calls inside of nextKeyValue when no new
   * files are available yet for reading; not that this polling is not done when files are already
   * available for reading.
   */
  public static final HadoopConfigurationProperty<Integer>
      DYNAMIC_FILE_LIST_RECORD_READER_POLL_INTERVAL_MS =
          new HadoopConfigurationProperty<>(
              "mapred.bq.dynamic.file.list.record.reader.poll.interval", 10_000);

  public static final HadoopConfigurationProperty<Integer>
      DYNAMIC_FILE_LIST_RECORD_READER_POLL_MAX_ATTEMPTS =
          new HadoopConfigurationProperty<>(
              "mapred.bq.dynamic.file.list.record.reader.poll.max.attempts", -1);

  /** A list of all necessary Configuration keys for input connector. */
  public static final ImmutableList<HadoopConfigurationProperty<?>>
      MANDATORY_CONFIG_PROPERTIES_INPUT =
          ImmutableList.of(PROJECT_ID, INPUT_PROJECT_ID, INPUT_DATASET_ID, INPUT_TABLE_ID);

  // Configuration keys for output connector.

  /**
   * Configuration key for the output project ID of the dataset accessed by the output format. This
   * key is stored as a {@link String}.
   */
  public static final HadoopConfigurationProperty<String> OUTPUT_PROJECT_ID =
      new HadoopConfigurationProperty<>("mapred.bq.output.project.id");

  /**
   * Configuration key for numeric ID of the output dataset accessed by the output format. This key
   * is stored as a {@link String}.
   */
  public static final HadoopConfigurationProperty<String> OUTPUT_DATASET_ID =
      new HadoopConfigurationProperty<>("mapred.bq.output.dataset.id");

  /**
   * Configuration key for numeric ID of the output table written by the output format. This key is
   * stored as a {@link String}.
   */
  public static final HadoopConfigurationProperty<String> OUTPUT_TABLE_ID =
      new HadoopConfigurationProperty<>("mapred.bq.output.table.id");

  /**
   * Configuration key for the output table schema used by the output format. This key is stored as
   * a {@link String}.
   */
  public static final HadoopConfigurationProperty<String> OUTPUT_TABLE_SCHEMA =
      new HadoopConfigurationProperty<>("mapred.bq.output.table.schema");

  /**
   * Configuration key for the output table partitioning used by the output format. This key is
   * stored as a {@link String}.
   */
  public static final HadoopConfigurationProperty<String> OUTPUT_TABLE_PARTITIONING =
      new HadoopConfigurationProperty<>("mapred.bq.output.table.partitioning");

  /**
   * Configuration key for the Cloud KMS encryption key that will be used to protect output BigQuery
   * table. This key is stored as a {@link String}.
   */
  public static final HadoopConfigurationProperty<String> OUTPUT_TABLE_KMS_KEY_NAME =
      new HadoopConfigurationProperty<>("mapred.bq.output.table.kmskeyname");

  /**
   * Configuration key for the write disposition of the output table. This specifies the action that
   * occurs if the destination table already exists. This key is stored as a {@link String}. By
   * default, if the table already exists, * BigQuery appends data to the output table.
   */
  public static final HadoopConfigurationProperty<String> OUTPUT_TABLE_WRITE_DISPOSITION =
      new HadoopConfigurationProperty<>("mapred.bq.output.table.writedisposition", "WRITE_APPEND");

  /**
   * Configuration key for the create disposition of the output table. This specifies if job should
   * create a table for loading data. This key is stored as a {@link String}. By default, if the
   * table does not exist, * BigQuery creates the table.
   */
  public static final HadoopConfigurationProperty<String> OUTPUT_TABLE_CREATE_DISPOSITION =
      new HadoopConfigurationProperty<>(
          "mapred.bq.output.table.createdisposition", "CREATE_IF_NEEDED");

  /**
   * Configuration key for the file format of the files outputted by the wrapped FileOutputFormat.
   * This key is stored as a serialized {@link BigQueryFileFormat}.
   */
  public static final HadoopConfigurationProperty<String> OUTPUT_FILE_FORMAT =
      new HadoopConfigurationProperty<>("mapred.bq.output.gcs.fileformat");

  /**
   * Configuration key for the FileOutputFormat class that's going to be wrapped by the output
   * format. This key is stored as a {@link Class}.
   */
  public static final HadoopConfigurationProperty<Class<?>> OUTPUT_FORMAT_CLASS =
      new HadoopConfigurationProperty<>("mapred.bq.output.gcs.outputformatclass");

  /**
   * Configuration key indicating whether temporary data stored in GCS should be deleted after the
   * output job is complete. This is true by default. This key is ignored when using federated
   * storage. This key is stored as a {@link Boolean}.
   */
  public static final HadoopConfigurationProperty<Boolean> OUTPUT_CLEANUP_TEMP =
      new HadoopConfigurationProperty<>("mapred.bq.output.gcs.cleanup", true);

  /** Size of the output buffer, in bytes, to use for BigQuery output. */
  public static final HadoopConfigurationProperty<Integer> OUTPUT_WRITE_BUFFER_SIZE =
      new HadoopConfigurationProperty<>("mapred.bq.output.buffer.size", 64 * 1024 * 1024);

  /**
   * Configure the location of the temporary dataset. Currently supported values are "US" and "EU".
   */
  public static final HadoopConfigurationProperty<String> DATA_LOCATION =
      new HadoopConfigurationProperty<>("mapred.bq.output.location", "US");

  /** A list of all necessary Configuration keys. */
  public static final ImmutableList<String> MANDATORY_CONFIG_PROPERTIES_OUTPUT =
      ImmutableList.of(
          PROJECT_ID.getKey(),
          OUTPUT_PROJECT_ID.getKey(),
          OUTPUT_DATASET_ID.getKey(),
          OUTPUT_TABLE_ID.getKey(),
          OUTPUT_TABLE_SCHEMA.getKey());

  public static final HadoopConfigurationProperty<String> SQL_FILTER =
      new HadoopConfigurationProperty<>("mapred.bq.input.sql.filter", "");
  public static final HadoopConfigurationProperty<String> SELECTED_FIELDS =
      new HadoopConfigurationProperty<>("mapred.bq.input.selected.fields");
  public static final HadoopConfigurationProperty<Double> SKEW_LIMIT =
      new HadoopConfigurationProperty<>("mapred.bq.input.skew.limit", 1.5);

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Sets the Bigquery access related fields in the JobConf for input connector.
   *
   * @param config the job configuration.
   * @param projectId the project containing the table to read the intermediate results to.
   * @param datasetId the dataset to write the intermediate results to.
   * @param tableId the table to write the intermediate results to.
   */
  public static void configureBigQueryInput(
      Configuration config,
      String projectId,
      String datasetId,
      String tableId)
      throws IOException {
    // Check preconditions.
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(datasetId), "datasetId must not be null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableId), "tableId must not be null or empty.");

    // Project is optional, if not set use default project.
    if (!Strings.isNullOrEmpty(projectId)) {
      logger.atInfo().log("Using specified project-id '%s' for input", projectId);
      config.set(INPUT_PROJECT_ID.getKey(), projectId);

      // For user-friendliness, we'll helpfully backfill the input-specific projectId into the
      // "global" projectId for now.
      // TODO(user): Maybe don't try to be user-friendly here.
      if (Strings.isNullOrEmpty(PROJECT_ID.get(config, config::get))) {
        logger.atWarning().log(
            "No job-level projectId specified in '%s', using '%s' for it.",
            PROJECT_ID.getKey(), projectId);
        config.set(PROJECT_ID.getKey(), projectId);
      }
    } else {
      String defaultProjectId = getMandatoryConfig(config, PROJECT_ID);
      logger.atInfo().log(
          "Using default project-id '%s' since none specified for input.", defaultProjectId);
      config.set(INPUT_PROJECT_ID.getKey(), defaultProjectId);
    }
    config.set(INPUT_DATASET_ID.getKey(), datasetId);
    config.set(INPUT_TABLE_ID.getKey(), tableId);
  }

  /**
   * Sets the Bigquery access related fields in the JobConf for input connector.
   *
   * @param config the job configuration.
   * @param fullyQualifiedInputTableId input-table id of the form
   *     [optional projectId]:[datasetId].[tableId]
   */
  public static void configureBigQueryInput(
      Configuration config, String fullyQualifiedInputTableId)
      throws IOException {
    TableReference parsedTable = BigQueryStrings.parseTableReference(fullyQualifiedInputTableId);
    configureBigQueryInput(
        config, parsedTable.getProjectId(), parsedTable.getDatasetId(), parsedTable.getTableId());
  }

  /**
   * Sets the Bigquery access related fields in the JobConf for output connector.
   *
   * @param config the job configuration.
   * @param projectId the project containing the table to write the results to.
   * @param datasetId the dataset to write the results to.
   * @param tableId the table to write the results to.
   * @param tableSchema the output table schema used by this output connector.
   */
  public static void configureBigQueryOutput(
      Configuration config,
      String projectId,
      String datasetId,
      String tableId,
      String tableSchema)
      throws IOException {
    // Check preconditions.
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(datasetId), "datasetId must not be null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableId), "tableId must not be null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableSchema), "tableSchema must not be null or empty.");

    // Project is optional, if not set use default project.
    if (!Strings.isNullOrEmpty(projectId)) {
      logger.atInfo().log("Using specified project-id '%s' for output", projectId);
      config.set(OUTPUT_PROJECT_ID.getKey(), projectId);

      // For user-friendliness, we'll helpfully backfill the input-specific projectId into the
      // "global" projectId for now.
      // TODO(user): Maybe don't try to be user-friendly here.
      if (Strings.isNullOrEmpty(PROJECT_ID.get(config, config::get))) {
        logger.atWarning().log(
            "No job-level projectId specified in '%s', using '%s' for it.",
            PROJECT_ID.getKey(), projectId);
        config.set(PROJECT_ID.getKey(), projectId);
      }
    } else {
      String defaultProjectId = getMandatoryConfig(config, PROJECT_ID);
      logger.atInfo().log(
          "Using default project-id '%s' since none specified for output.", defaultProjectId);
      config.set(OUTPUT_PROJECT_ID.getKey(), defaultProjectId);
    }
    config.set(OUTPUT_DATASET_ID.getKey(), datasetId);
    config.set(OUTPUT_TABLE_ID.getKey(), tableId);
    config.set(OUTPUT_TABLE_SCHEMA.getKey(), tableSchema);
  }

  /**
   * Sets the Bigquery access related fields in the JobConf for output connector.
   *
   * @param config the job configuration.
   * @param fullyQualifiedOutputTableId output-table id of the form
   *     [optional projectId]:[datasetId].[tableId]
   * @param tableSchema the output table schema used by this output connector.
   */
  public static void configureBigQueryOutput(
      Configuration config, String fullyQualifiedOutputTableId, String tableSchema)
      throws IOException {
    TableReference parsedTable = BigQueryStrings.parseTableReference(fullyQualifiedOutputTableId);
    configureBigQueryOutput(
        config, parsedTable.getProjectId(), parsedTable.getDatasetId(), parsedTable.getTableId(),
        tableSchema);
  }

  /**
   * Resolves to provided {@link #TEMP_GCS_PATH} or fallbacks to a temporary path based on {@link
   * #GCS_BUCKET} and {@code jobId}.
   *
   * @param conf the configuration to fetch the keys from.
   * @param jobId the ID of the job requesting a working path. Optional (could be {@code null}) if
   *     {@link #TEMP_GCS_PATH} is provided.
   * @return the temporary directory path.
   * @throws IOException if the file system of the derived working path isn't GCS.
   */
  public static String getTemporaryPathRoot(Configuration conf, @Nullable JobID jobId)
      throws IOException {
    // Try using the temporary gcs path.
    String pathRoot = conf.get(BigQueryConfiguration.TEMP_GCS_PATH.getKey());

    if (Strings.isNullOrEmpty(pathRoot)) {
      checkNotNull(jobId, "jobId is required if '%s' is not set", TEMP_GCS_PATH.getKey());
      logger.atInfo().log(
          "Fetching key '%s' since '%s' isn't set explicitly.",
          GCS_BUCKET.getKey(), TEMP_GCS_PATH.getKey());

      String gcsBucket = conf.get(GCS_BUCKET.getKey());
      if (Strings.isNullOrEmpty(gcsBucket)) {
        throw new IOException(
            "Must supply a value for configuration setting: " + GCS_BUCKET.getKey());
      }

      pathRoot = String.format("gs://%s/hadoop/tmp/bigquery/%s", gcsBucket, jobId);
    }

    logger.atInfo().log("Using working path: '%s'", pathRoot);
    Path workingPath = new Path(pathRoot);

    FileSystem fs = workingPath.getFileSystem(conf);
    Preconditions.checkState("gs".equals(fs.getScheme()), "Export FS must be GCS ('gs' scheme).");
    return pathRoot;
  }
}
