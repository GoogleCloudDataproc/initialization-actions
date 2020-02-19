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
package com.google.cloud.hadoop.io.bigquery.output;

import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_CLEANUP_TEMP;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_DATASET_ID;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_FILE_FORMAT;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_FORMAT_CLASS;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_PROJECT_ID;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_TABLE_ID;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_TABLE_KMS_KEY_NAME;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_TABLE_PARTITIONING;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_TABLE_SCHEMA;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.PROJECT_ID;
import static com.google.cloud.hadoop.util.ConfigurationUtil.getMandatoryConfig;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings;
import com.google.cloud.hadoop.util.HadoopConfigurationProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A container for configuration keys related to BigQuery indirect output formats. Alternatively,
 * the properties can be set in the configuration xml files with proper values.
 */
@InterfaceStability.Unstable
public class BigQueryOutputConfiguration {

  /** A list of keys that are required for this output connector. */
  public static final ImmutableList<HadoopConfigurationProperty<?>> REQUIRED_PROPERTIES =
      ImmutableList.of(OUTPUT_DATASET_ID, OUTPUT_TABLE_ID, OUTPUT_FILE_FORMAT, OUTPUT_FORMAT_CLASS);

  /**
   * A helper function to set the required output keys in the given configuration.
   *
   * @param conf the configuration to set the keys on.
   * @param qualifiedOutputTableId the qualified id of the output table in the form: <code>(Optional
   *     ProjectId):[DatasetId].[TableId]</code>. If the project id is missing, the default project
   *     id is attempted {@link BigQueryConfiguration#PROJECT_ID}.
   * @param outputTableSchemaJson the schema of the BigQuery output table.
   * @param outputGcsPath the path in GCS to stage data in. Example: 'gs://bucket/job'.
   * @param outputFileFormat the formatting of the data being written by the output format class.
   * @param outputFormatClass the file output format that will write files to GCS.
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public static void configure(
      Configuration conf,
      String qualifiedOutputTableId,
      String outputTableSchemaJson,
      String outputGcsPath,
      BigQueryFileFormat outputFileFormat,
      Class<? extends FileOutputFormat> outputFormatClass)
      throws IOException {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(outputTableSchemaJson),
        "outputTableSchemaJson must not be null or empty.");
    TableReference outputTable = BigQueryStrings.parseTableReference(qualifiedOutputTableId);
    configure(
        conf,
        outputTable.getProjectId(),
        outputTable.getDatasetId(),
        outputTable.getTableId(),
        Optional.of(outputTableSchemaJson),
        outputGcsPath,
        outputFileFormat,
        outputFormatClass);
  }

  /**
   * A helper function to set the required output keys in the given configuration.
   *
   * @param conf the configuration to set the keys on.
   * @param outputProjectId the id of the output project. If the project id is null, the default
   *     project id is attempted {@link BigQueryConfiguration#PROJECT_ID}.
   * @param outputDatasetId the id of the output dataset.
   * @param outputTableId the id of the output table.
   * @param outputTableSchemaJson the schema of the BigQuery output table. If the schema is null,
   *     BigQuery will attempt to auto detect the schema. When using avro formatted data, a schema
   *     is not required as avro stores the schema in the file.
   * @param outputGcsPath the path in GCS to stage data in. Example: 'gs://bucket/job'.
   * @param outputFileFormat the formatting of the data being written by the output format class.
   * @param outputFormatClass the file output format that will write files to GCS.
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  private static void configure(
      Configuration conf,
      String outputProjectId,
      String outputDatasetId,
      String outputTableId,
      Optional<String> outputTableSchemaJson,
      String outputGcsPath,
      BigQueryFileFormat outputFileFormat,
      Class<? extends FileOutputFormat> outputFormatClass)
      throws IOException {

    // Use the default project ID as a backup.
    if (Strings.isNullOrEmpty(outputProjectId)) {
      outputProjectId = PROJECT_ID.get(conf, conf::get);
    }

    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(outputProjectId), "outputProjectId must not be null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(outputDatasetId), "outputDatasetId must not be null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(outputTableId), "outputTableId must not be null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(outputGcsPath), "outputGcsPath must not be null or empty.");
    Preconditions.checkNotNull(outputFileFormat, "outputFileFormat must not be null.");
    Preconditions.checkNotNull(outputFormatClass, "outputFormatClass must not be null.");

    conf.set(OUTPUT_PROJECT_ID.getKey(), outputProjectId);
    conf.set(OUTPUT_DATASET_ID.getKey(), outputDatasetId);
    conf.set(OUTPUT_TABLE_ID.getKey(), outputTableId);
    conf.set(OUTPUT_FILE_FORMAT.getKey(), outputFileFormat.name());
    conf.setClass(OUTPUT_FORMAT_CLASS.getKey(), outputFormatClass, FileOutputFormat.class);

    setFileOutputFormatOutputPath(conf, outputGcsPath);

    // If a schema is provided, serialize it.
    if (outputTableSchemaJson.isPresent()) {
      TableSchema tableSchema = BigQueryTableHelper.parseTableSchema(outputTableSchemaJson.get());
      String fieldsJson = BigQueryTableHelper.getTableFieldsJson(tableSchema);
      conf.set(OUTPUT_TABLE_SCHEMA.getKey(), fieldsJson);
    }
  }

  /**
   * A helper function to set the required output keys in the given configuration.
   *
   * @param conf the configuration to set the keys on.
   * @param qualifiedOutputTableId the qualified id of the output table in the form: <code>(Optional
   *     ProjectId):[DatasetId].[TableId]</code>. If the project id is missing, the default project
   *     id is attempted {@link BigQueryConfiguration#PROJECT_ID}.
   * @param outputTableSchema the schema of the BigQuery output table. If the schema is null,
   *     BigQuery will attempt to auto detect the schema. When using avro formatted data, a schema
   *     is not required as avro stores the schema in the file.
   * @param outputGcsPath the path in GCS to stage data in. Example: 'gs://bucket/job'.
   * @param outputFileFormat the formatting of the data being written by the output format class.
   * @param outputFormatClass the file output format that will write files to GCS.
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public static void configure(
      Configuration conf,
      String qualifiedOutputTableId,
      BigQueryTableSchema outputTableSchema,
      String outputGcsPath,
      BigQueryFileFormat outputFileFormat,
      Class<? extends FileOutputFormat> outputFormatClass)
      throws IOException {
    configure(
        conf,
        qualifiedOutputTableId,
        BigQueryTableHelper.getTableSchemaJson(outputTableSchema.get()),
        outputGcsPath,
        outputFileFormat,
        outputFormatClass);
  }

  /**
   * A helper function to set the required output keys in the given configuration.
   *
   * <p>This method will set the output table schema as auto-detected.
   *
   * @param conf the configuration to set the keys on.
   * @param qualifiedOutputTableId the qualified id of the output table in the form: <code>(Optional
   *     ProjectId):[DatasetId].[TableId]</code>. If the project id is missing, the default project
   *     id is attempted {@link BigQueryConfiguration#PROJECT_ID}.
   * @param outputGcsPath the path in GCS to stage data in. Example: 'gs://bucket/job'.
   * @param outputFileFormat the formatting of the data being written by the output format class.
   * @param outputFormatClass the file output format that will write files to GCS.
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public static void configureWithAutoSchema(
      Configuration conf,
      String qualifiedOutputTableId,
      String outputGcsPath,
      BigQueryFileFormat outputFileFormat,
      Class<? extends FileOutputFormat> outputFormatClass)
      throws IOException {
    TableReference outputTable = BigQueryStrings.parseTableReference(qualifiedOutputTableId);
    configure(
        conf,
        outputTable.getProjectId(),
        outputTable.getDatasetId(),
        outputTable.getTableId(),
        /* outputTableSchemaJson= */ Optional.empty(),
        outputGcsPath,
        outputFileFormat,
        outputFormatClass);
  }

  public static void setKmsKeyName(Configuration conf, String kmsKeyName) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(kmsKeyName), "kmsKeyName must not be null or empty.");
    conf.set(OUTPUT_TABLE_KMS_KEY_NAME.getKey(), kmsKeyName);
  }

  /**
   * Helper function that validates the output configuration. Ensures the project id, dataset id,
   * and table id exist in the configuration. This also ensures that if a schema is provided, that
   * it is properly formatted.
   *
   * @param conf the configuration to validate.
   * @throws IOException if the configuration is missing a key, or there's an issue while parsing
   *     the schema in the configuration.
   */
  public static void validateConfiguration(Configuration conf) throws IOException {
    // Ensure the BigQuery output information is valid.
    getMandatoryConfig(conf, REQUIRED_PROPERTIES);

    // Run through the individual getters as they manage error handling.
    getProjectId(conf);
    getJobProjectId(conf);
    getTableSchema(conf);
    getFileFormat(conf);
    getFileOutputFormat(conf);
    getGcsOutputPath(conf);
  }

  /**
   * Gets if the configuration flag to cleanup temporary data in GCS is enabled or not.
   *
   * @param conf the configuration to reference the key from.
   * @return true if the flag is enabled or missing, false otherwise.
   */
  public static boolean getCleanupTemporaryDataFlag(Configuration conf) {
    return OUTPUT_CLEANUP_TEMP.get(conf, conf::getBoolean);
  }

  /**
   * Gets the output dataset project id based on the given configuration.
   *
   * <p>If the {@link BigQueryConfiguration#OUTPUT_PROJECT_ID} is missing, this resolves to
   * referencing the {@link BigQueryConfiguration#PROJECT_ID} key.
   *
   * <p>The load job can be configured with two project identifiers. Configuration key {@link
   * BigQueryConfiguration#PROJECT_ID} can set the project on whose behalf to perform BigQuery load
   * operation, while {@link BigQueryConfiguration#OUTPUT_PROJECT_ID} can be used to name the
   * project that the target dataset belongs to.
   *
   * @param conf the configuration to reference the keys from.
   * @return the project id based on the given configuration.
   * @throws IOException if a required key is missing.
   */
  public static String getProjectId(Configuration conf) throws IOException {
    // Reference the default project ID as a backup.
    String projectId = OUTPUT_PROJECT_ID.get(conf, conf::get);
    if (Strings.isNullOrEmpty(projectId)) {
      projectId = PROJECT_ID.get(conf, conf::get);
    }
    if (Strings.isNullOrEmpty(projectId)) {
      throw new IOException(
          "Must supply a value for configuration setting: " + OUTPUT_PROJECT_ID.getKey());
    }
    return projectId;
  }

  /**
   * Gets the project id to be used to run BQ load job based on the given configuration.
   *
   * <p>If the {@link BigQueryConfiguration#PROJECT_ID} is missing, this resolves to referencing the
   * {@link BigQueryConfiguration#OUTPUT_PROJECT_ID} key.
   *
   * <p>The load job can be configured with two project identifiers. Configuration key {@link
   * BigQueryConfiguration#PROJECT_ID} can set the project on whose behalf to perform BigQuery load
   * operation, while {@link BigQueryConfiguration#OUTPUT_PROJECT_ID} can be used to name the
   * project that the target dataset belongs to.
   *
   * @param conf the configuration to reference the keys from.
   * @return the project id based on the given configuration.
   * @throws IOException if a required key is missing.
   */
  public static String getJobProjectId(Configuration conf) throws IOException {
    // Reference the default project ID as a backup.
    String projectId = PROJECT_ID.get(conf, conf::get);
    if (Strings.isNullOrEmpty(projectId)) {
      projectId = OUTPUT_PROJECT_ID.get(conf, conf::get);
    }
    if (Strings.isNullOrEmpty(projectId)) {
      throw new IOException(
          "Must supply a value for configuration setting: " + PROJECT_ID.getKey());
    }
    return projectId;
  }

  /**
   * Gets the output table reference based on the given configuration. If the {@link
   * BigQueryConfiguration#OUTPUT_PROJECT_ID} is missing, this resolves to referencing the
   * {@link BigQueryConfiguration#PROJECT_ID} key.
   *
   * @param conf the configuration to reference the keys from.
   * @return a reference to the derived output table in the format of "<project>:<dataset>.<table>".
   * @throws IOException if a required key is missing.
   */
  static TableReference getTableReference(Configuration conf) throws IOException {
    // Ensure the BigQuery output information is valid.
    String projectId = getProjectId(conf);
    String datasetId = getMandatoryConfig(conf, OUTPUT_DATASET_ID);
    String tableId = getMandatoryConfig(conf, OUTPUT_TABLE_ID);

    return new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId);
  }

  /**
   * Gets the output table schema based on the given configuration.
   *
   * @param conf the configuration to reference the keys from.
   * @return the derived table schema, absent value if no table schema exists in the configuration.
   * @throws IOException if a table schema was set in the configuration but couldn't be parsed.
   */
  static Optional<BigQueryTableSchema> getTableSchema(Configuration conf) throws IOException {
    String fieldsJson = OUTPUT_TABLE_SCHEMA.get(conf, conf::get);
    if (!Strings.isNullOrEmpty(fieldsJson)) {
      try {
        TableSchema tableSchema = BigQueryTableHelper.createTableSchemaFromFields(fieldsJson);
        return Optional.of(BigQueryTableSchema.wrap(tableSchema));
      } catch (IOException e) {
        throw new IOException("Unable to parse key '" + OUTPUT_TABLE_SCHEMA.getKey() + "'.", e);
      }
    }
    return Optional.empty();
  }

  /**
   * Gets the output table time partitioning based on the given configuration.
   *
   * @param conf the configuration to reference the keys from.
   * @return the derived table time partitioning, absent value if no table time partitioning exists
   *     in the configuration.
   * @throws IOException if a table time partitioning was set in the configuration but couldn't be
   *     parsed.
   */
  static Optional<BigQueryTimePartitioning> getTablePartitioning(Configuration conf)
      throws IOException {
    String fieldsJson = OUTPUT_TABLE_PARTITIONING.get(conf, conf::get);
    if (!Strings.isNullOrEmpty(fieldsJson)) {
      try {
        TimePartitioning tablePartitioning = BigQueryTimePartitioning.getFromJson(fieldsJson);
        return Optional.of(BigQueryTimePartitioning.wrap(tablePartitioning));
      } catch (IOException e) {
        throw new IOException(
            "Unable to parse key '" + OUTPUT_TABLE_PARTITIONING.getKey() + "'.", e);
      }
    }
    return Optional.empty();
  }

  /**
   * Gets the output table KMS key name based on the given configuration.
   *
   * @param conf the configuration to reference the keys from.
   * @return the KMS key name of the output table, null if no KMS key name exists in the
   *     configuration.
   */
  public static String getKmsKeyName(Configuration conf) throws IOException {
    return OUTPUT_TABLE_KMS_KEY_NAME.get(conf, conf::get);
  }

  /**
   * Gets the stored output {@link BigQueryFileFormat} in the configuration.
   *
   * @param conf the configuration to reference the keys from.
   * @return the stored output {@link BigQueryFileFormat} in the configuration.
   * @throws IOException if file format value is missing from the configuration.
   */
  public static BigQueryFileFormat getFileFormat(Configuration conf) throws IOException {
    // Ensure the BigQuery output information is valid.
    String fileFormatName = getMandatoryConfig(conf, OUTPUT_FILE_FORMAT);

    return BigQueryFileFormat.fromName(fileFormatName);
  }

  /**
   * Gets a configured instance of the stored {@link FileOutputFormat} in the configuration.
   *
   * @param conf the configuration to reference the keys from.
   * @return a configured instance of the stored {@link FileOutputFormat} in the configuration.
   * @throws IOException if there's an issue getting an instance of a FileOutputFormat from the
   *     configuration.
   */
  @SuppressWarnings("rawtypes")
  public static FileOutputFormat getFileOutputFormat(Configuration conf) throws IOException {
    // Ensure the BigQuery output information is valid.
    getMandatoryConfig(conf, OUTPUT_FORMAT_CLASS);

    Class<?> confClass = OUTPUT_FORMAT_CLASS.get(conf, conf::getClass);

    // Fail if the default value was used, or the class isn't a FileOutputFormat.
    if (confClass == null) {
      throw new IOException(
          "Unable to resolve value for the configuration key '"
              + OUTPUT_FORMAT_CLASS.getKey()
              + "'.");
    } else if (!FileOutputFormat.class.isAssignableFrom(confClass)) {
      throw new IOException("The class " + confClass.getName() + " is not a FileOutputFormat.");
    }

    Class<? extends FileOutputFormat> fileOutputClass =
        confClass.asSubclass(FileOutputFormat.class);

    // Create a new instance and configure it if it's configurable.
    return ReflectionUtils.newInstance(fileOutputClass, conf);
  }

  /**
   * Gets the stored GCS output path in the configuration.
   *
   * @param conf the configuration to reference the keys from.
   * @return the stored output path in the configuration.
   * @throws IOException if the output path isn't set in the configuration, or the output path's
   *     file system isn't GCS.
   */
  public static Path getGcsOutputPath(Configuration conf) throws IOException {
    Job tempJob = new JobConfigurationAdapter(conf);

    // Error if the output path is missing.
    Path outputPath = FileOutputFormat.getOutputPath(tempJob);
    if (outputPath == null) {
      throw new IOException("FileOutputFormat output path not set.");
    }

    // Error if the output file system isn't GCS.
    FileSystem fs = outputPath.getFileSystem(conf);
    if (!"gs".equals(fs.getScheme())) {
      throw new IOException("Output FileSystem must be GCS ('gs' scheme).");
    }

    return outputPath;
  }

  /**
   * Gets the create disposition of the output table. This specifies if the job should create a
   * table for loading data.
   *
   * @param conf the configuration to reference the keys from.
   * @return the create disposition of the output table.
   */
  public static String getCreateDisposition(Configuration conf) {
    return OUTPUT_TABLE_CREATE_DISPOSITION.get(conf, conf::get);
  }


  /**
   * Gets the write disposition of the output table. This specifies the action that occurs if the
   * destination table already exists. By default, if the table already exists, BigQuery appends
   * data to the output table.
   *
   * @param conf the configuration to reference the keys from.
   * @return the write disposition of the output table.
   */
  public static String getWriteDisposition(Configuration conf) {
    return OUTPUT_TABLE_WRITE_DISPOSITION.get(conf, conf::get);
  }

  /**
   * Sets the output path for FileOutputFormat.
   *
   * @param conf the configuration to pass to FileOutputFormat.
   * @param outputPath the path to set as the output path.
   * @throws IOException
   */
  @VisibleForTesting
  static void setFileOutputFormatOutputPath(Configuration conf, String outputPath)
      throws IOException {
    Job tempJob = new JobConfigurationAdapter(conf);
    FileOutputFormat.setOutputPath(tempJob, new Path(outputPath));
  }

  /**
   * This class provides a workaround for setting FileOutputFormat's output path. Creating a job
   * with a configuration creates a defensive copy of the configuration for the job, meaning changes
   * in either configuration will not be reflected in the other. Because FileOutputFormat requires a
   * job for the API to set an output path, this adapter is used to ensure changes are propagated
   * out to the wrapped configuration.
   */
  private static class JobConfigurationAdapter extends Job {

    private final Configuration config;

    public JobConfigurationAdapter(Configuration config) throws IOException {
      super();
      this.config = config;
    }

    @Override
    public Configuration getConfiguration() {
      return config;
    }
  }
}
