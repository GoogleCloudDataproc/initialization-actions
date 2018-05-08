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

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container for configuration property names for jobs with BigQuery input/output.
 *
 *  The job can be configured programmatically using the static methods in this class,
 * GsonBigQueryInputFormat, and BigqueryOutputFormat}.
 *
 * Alternatively, the properties can be set in the configuration xml files with proper values.
 */
public class BigQueryConfiguration {
  /**
   * Configuration key for project ID on whose behalf to perform BigQuery operations, and the
   * default project for referencing datasets when input/output datasets are not explicitly
   * specified.
   */
  public static final String PROJECT_ID_KEY = "mapred.bq.project.id";

  // Configuration keys for input connector.

  /** Configuration key for project ID of the dataset accessed by this input connector. */
  public static final String INPUT_PROJECT_ID_KEY = "mapred.bq.input.project.id";

  /** Configuration key for ID of the dataset accessed by this input connector. */
  public static final String INPUT_DATASET_ID_KEY = "mapred.bq.input.dataset.id";

  /** Configuration key for ID of the table written by this input connector. */
  public static final String INPUT_TABLE_ID_KEY = "mapred.bq.input.table.id";

  /**
   * @deprecated Issue queries from the BigQuery command-line tool instead, outside of a MapReduce.
   */
  @Deprecated public static final String INPUT_QUERY_KEY = "mapred.bq.input.query";

  /** Configuration key for the GCS temp path this connector uses. */
  public static final String TEMP_GCS_PATH_KEY = "mapred.bq.temp.gcs.path";

  /** Configuration key for the GCS bucket holding TEMP_GCS_PATH_KEY */
  public static final String GCS_BUCKET_KEY = "mapred.bq.gcs.bucket";

  /**
   * @deprecated Tables exist outside the scope of MapReduces; use BigQuery CLI instead.
   */
  @Deprecated
  public static final String DELETE_INTERMEDIATE_TABLE_KEY = "mapred.bq.query.results.table.delete";
  public static final boolean DELETE_INTERMEDIATE_TABLE_DEFAULT = false;

  /** Configuration key for whether to delete the intermediate GCS-export files. */
  public static final String DELETE_EXPORT_FILES_FROM_GCS_KEY =
      "mapred.bq.input.export.files.delete";
  public static final boolean DELETE_EXPORT_FILES_FROM_GCS_DEFAULT = true;

  /**
   * @deprecated Configuration key specifying whether to start the map phase concurrently with the
   *     BigQuery export. Since BigQuery exports are significantly faster than when this was
   *     implemented it is no longer suggested.
   */
  @Deprecated
  public static final String ENABLE_SHARDED_EXPORT_KEY = "mapred.bq.input.sharded.export.enable";

  public static final boolean ENABLE_SHARDED_EXPORT_DEFAULT = false;

  /**
   * Number of milliseconds to wait between listStatus calls inside of nextKeyValue when no
   * new files are available yet for reading; not that this polling is not done when files
   * are already available for reading.
   */
  public static final String DYNAMIC_FILE_LIST_RECORD_READER_POLL_INTERVAL_MS_KEY =
      "mapred.bq.dynamic.file.list.record.reader.poll.interval";
  public static final int DYNAMIC_FILE_LIST_RECORD_READER_POLL_INTERVAL_MS_DEFAULT = 10000;

  /** A list of all necessary Configuration keys for input connector. */
  public static final ImmutableList<String> MANDATORY_CONFIG_PROPERTIES_INPUT =
      ImmutableList.of(
          PROJECT_ID_KEY, INPUT_PROJECT_ID_KEY, INPUT_DATASET_ID_KEY, INPUT_TABLE_ID_KEY);

  // Configuration keys for output connector.

  /**
   * Configuration key for the output project ID of the dataset accessed by the output format. This
   * key is stored as a {@link String}.
   */
  public static final String OUTPUT_PROJECT_ID_KEY = "mapred.bq.output.project.id";

  /**
   * Configuration key for numeric ID of the output dataset accessed by the output format. This key
   * is stored as a {@link String}.
   */
  public static final String OUTPUT_DATASET_ID_KEY = "mapred.bq.output.dataset.id";

  /**
   * Configuration key for numeric ID of the output table written by the output format. This key is
   * stored as a {@link String}.
   */
  public static final String OUTPUT_TABLE_ID_KEY = "mapred.bq.output.table.id";

  /**
   * Configuration key for the output table schema used by the output format. This key is stored as
   * a {@link String}.
   */
  public static final String OUTPUT_TABLE_SCHEMA_KEY = "mapred.bq.output.table.schema";

  /**
   * Configuration key for the write disposition of the output table. This specifies the action that
   * occurs if the destination table already exists. This key is stored as a {@link String}.
   */
  public static final String OUTPUT_TABLE_WRITE_DISPOSITION_KEY =
      "mapred.bq.output.table.writedisposition";

  /**
   * The default write disposition for the output table. By default, if the table already exists,
   * BigQuery appends data to the output table.
   */
  public static final String OUTPUT_TABLE_WRITE_DISPOSITION_DEFAULT = "WRITE_APPEND";

  /**
   * Configuration key for the file format of the files outputted by the wrapped FileOutputFormat.
   * This key is stored as a serialized {@link BigQueryFileFormat}.
   */
  public static final String OUTPUT_FILE_FORMAT_KEY = "mapred.bq.output.gcs.fileformat";

  /**
   * Configuration key for the FileOutputFormat class that's going to be wrapped by the output
   * format. This key is stored as a {@link Class}.
   */
  public static final String OUTPUT_FORMAT_CLASS_KEY = "mapred.bq.output.gcs.outputformatclass";

  /**
   * Configuration key indicating whether temporary data stored in GCS should be deleted after the
   * output job is complete. This is true by default. This key is ignored when using federated
   * storage. This key is stored as a {@link Boolean}.
   */
  public static final String OUTPUT_CLEANUP_TEMP_KEY = "mapred.bq.output.gcs.cleanup";

  /** Size of the output buffer, in bytes, to use for BigQuery output. */
  public static final String OUTPUT_WRITE_BUFFER_SIZE_KEY = "mapred.bq.output.buffer.size";

  /** 64MB default write buffer size. */
  public static final int OUTPUT_WRITE_BUFFER_SIZE_DEFAULT = 64 * 1024 * 1024;

  /**
   * Configure the location of the temporary dataset.
   * Currently supported values are "US" and "EU".
   */
  public static final String DATA_LOCATION_KEY = "mapred.bq.output.location";

    /** The default dataset location is US */
  public static final String DATA_LOCATION_DEFAULT = "US";

  /** A list of all necessary Configuration keys. */
  public static final ImmutableList<String> MANDATORY_CONFIG_PROPERTIES_OUTPUT =
      ImmutableList.of(
          PROJECT_ID_KEY,
          OUTPUT_PROJECT_ID_KEY,
          OUTPUT_DATASET_ID_KEY,
          OUTPUT_TABLE_ID_KEY,
          OUTPUT_TABLE_SCHEMA_KEY);

  /**
   * Obsolete; no longer affects any behavior. A warning will be printed if the key is found to be
   * set to 'false', and then ignored. Outputs will now always occur in the "async" mode where
   * a pipe connects the writer thread with another request-executor thread sending data to
   * Google's "resumeable upload" service. This upload service is the same one used for uploads
   * using the Google Cloud Storage connector for Hadoop.
   */
  public static final String ENABLE_ASYNC_WRITE = "mapred.bq.output.async.write.enabled";
  public static final boolean ENABLE_ASYNC_WRITE_DEFAULT = true;

  // Logger.
  protected static final Logger LOG = LoggerFactory.getLogger(BigQueryConfiguration.class);

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
      LOG.info("Using specified project-id '{}' for input", projectId);
      config.set(INPUT_PROJECT_ID_KEY, projectId);

      // For user-friendliness, we'll helpfully backfill the input-specific projectId into the
      // "global" projectId for now.
      // TODO(user): Maybe don't try to be user-friendly here.
      if (Strings.isNullOrEmpty(config.get(PROJECT_ID_KEY))) {
        LOG.warn("No job-level projectId specified in '{}', using '{}' for it.",
            PROJECT_ID_KEY, projectId);
        config.set(PROJECT_ID_KEY, projectId);
      }
    } else {
      String defaultProjectId = ConfigurationUtil.getMandatoryConfig(config, PROJECT_ID_KEY);
      LOG.info("Using default project-id '{}' since none specified for input.", defaultProjectId);
      config.set(INPUT_PROJECT_ID_KEY, defaultProjectId);
    }
    config.set(INPUT_DATASET_ID_KEY, datasetId);
    config.set(INPUT_TABLE_ID_KEY, tableId);
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
      LOG.info("Using specified project-id '{}' for output", projectId);
      config.set(OUTPUT_PROJECT_ID_KEY, projectId);

      // For user-friendliness, we'll helpfully backfill the input-specific projectId into the
      // "global" projectId for now.
      // TODO(user): Maybe don't try to be user-friendly here.
      if (Strings.isNullOrEmpty(config.get(PROJECT_ID_KEY))) {
        LOG.warn("No job-level projectId specified in '{}', using '{}' for it.",
            PROJECT_ID_KEY, projectId);
        config.set(PROJECT_ID_KEY, projectId);
      }
    } else {
      String defaultProjectId = ConfigurationUtil.getMandatoryConfig(config, PROJECT_ID_KEY);
      LOG.info("Using default project-id '{}' since none specified for output.", defaultProjectId);
      config.set(OUTPUT_PROJECT_ID_KEY, defaultProjectId);
    }
    config.set(OUTPUT_DATASET_ID_KEY, datasetId);
    config.set(OUTPUT_TABLE_ID_KEY, tableId);
    config.set(OUTPUT_TABLE_SCHEMA_KEY, tableSchema);
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
   * Either resolves a temporary path based on GCS_BUCKET_KEY and JobID, or defers to a pre-provided
   * BigQueryConfiguration.TEMP_GCS_PATH_KEY.
   *
   * @param conf the configuration to fetch the keys from.
   * @param jobId the id of the job requesting a working path.
   * @return the temporary directory path.
   * @throws IOException if the file system of the derived working path isn't a derivative of
   *     GoogleHadoopFileSystemBase.
   */
  public static String getTemporaryPathRoot(Configuration conf, JobID jobId) throws IOException {
    // Try using the temporary gcs path.
    String pathRoot = conf.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY);

    if (Strings.isNullOrEmpty(pathRoot)) {
      LOG.info(
          "Fetching key '{}' since '{}' isn't set explicitly.",
          BigQueryConfiguration.GCS_BUCKET_KEY,
          BigQueryConfiguration.TEMP_GCS_PATH_KEY);
      String gcsBucket =
          conf.get(
              BigQueryConfiguration.GCS_BUCKET_KEY,
              "${" + GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY + "}");

      if (Strings.isNullOrEmpty(gcsBucket)) {
        throw new IOException("Must supply a value for configuration setting: " + GCS_BUCKET_KEY);
      }

      pathRoot = String.format("gs://%s/hadoop/tmp/bigquery/%s", gcsBucket, jobId);
    }

    LOG.info("Using working path: '{}'", pathRoot);
    Path workingPath = new Path(pathRoot);

    FileSystem fs = workingPath.getFileSystem(conf);
    Preconditions.checkState(
        fs instanceof GoogleHadoopFileSystemBase,
        "Export FS must derive from GoogleHadoopFileSystemBase.");
    return pathRoot;
  }
}
