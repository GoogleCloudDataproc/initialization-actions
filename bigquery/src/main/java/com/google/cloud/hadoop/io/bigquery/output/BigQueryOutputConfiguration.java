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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings;
import com.google.cloud.hadoop.util.ConfigurationUtil;
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
  public static final ImmutableList<String> REQUIRED_KEYS =
      ImmutableList.of(
          BigQueryConfiguration.OUTPUT_DATASET_ID_KEY,
          BigQueryConfiguration.OUTPUT_TABLE_ID_KEY,
          BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY,
          BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY);

  /**
   * A helper function to set the required output keys in the given configuration.
   *
   * @param conf the configuration to set the keys on.
   * @param qualifiedOutputTableId the qualified id of the output table in the form: <code>(Optional
   *     ProjectId):[DatasetId].[TableId]</code>. If the project id is missing, the default project
   *     id is attempted {@link BigQueryConfiguration#PROJECT_ID_KEY}.
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
   *     project id is attempted {@link BigQueryConfiguration#PROJECT_ID_KEY}.
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
      outputProjectId = conf.get(BigQueryConfiguration.PROJECT_ID_KEY);
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

    conf.set(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY, outputProjectId);
    conf.set(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY, outputDatasetId);
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY, outputTableId);
    conf.set(BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY, outputFileFormat.name());
    conf.setClass(
        BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY, outputFormatClass, FileOutputFormat.class);

    setFileOutputFormatOutputPath(conf, outputGcsPath);

    // If a schema is provided, serialize it.
    if (outputTableSchemaJson.isPresent()) {
      TableSchema tableSchema = BigQueryTableHelper.parseTableSchema(outputTableSchemaJson.get());
      String fieldsJson = BigQueryTableHelper.getTableFieldsJson(tableSchema);
      conf.set(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY, fieldsJson);
    }
  }

  /**
   * A helper function to set the required output keys in the given configuration.
   *
   * @param conf the configuration to set the keys on.
   * @param qualifiedOutputTableId the qualified id of the output table in the form: <code>(Optional
   *     ProjectId):[DatasetId].[TableId]</code>. If the project id is missing, the default project
   *     id is attempted {@link BigQueryConfiguration#PROJECT_ID_KEY}.
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
   *     id is attempted {@link BigQueryConfiguration#PROJECT_ID_KEY}.
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
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_KMS_KEY_NAME_KEY, kmsKeyName);
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
    ConfigurationUtil.getMandatoryConfig(conf, REQUIRED_KEYS);

    // Run through the individual getters as they manage error handling.
    getProjectId(conf);
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
    return conf.getBoolean(BigQueryConfiguration.OUTPUT_CLEANUP_TEMP_KEY, true);
  }

  /**
   * Gets the project id based on the given configuration. If the {@link
   * BigQueryConfiguration#OUTPUT_PROJECT_ID_KEY} is missing, this resolves to referencing the
   * {@link BigQueryConfiguration#PROJECT_ID_KEY} key.
   *
   * @param conf the configuration to reference the keys from.
   * @return the project id based on the given configuration.
   * @throws IOException if a required key is missing.
   */
  public static String getProjectId(Configuration conf) throws IOException {
    // Reference the default project ID as a backup.
    String projectId = conf.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY);
    if (Strings.isNullOrEmpty(projectId)) {
      projectId = conf.get(BigQueryConfiguration.PROJECT_ID_KEY);
    }
    if (Strings.isNullOrEmpty(projectId)) {
      throw new IOException(
          "Must supply a value for configuration setting: "
              + BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY);
    }
    return projectId;
  }

  /**
   * Gets the output table reference based on the given configuration. If the {@link
   * BigQueryConfiguration#OUTPUT_PROJECT_ID_KEY} is missing, this resolves to referencing the
   * {@link BigQueryConfiguration#PROJECT_ID_KEY} key.
   *
   * @param conf the configuration to reference the keys from.
   * @return a reference to the derived output table in the format of "<project>:<dataset>.<table>".
   * @throws IOException if a required key is missing.
   */
  static TableReference getTableReference(Configuration conf) throws IOException {
    // Ensure the BigQuery output information is valid.
    String projectId = getProjectId(conf);
    String datasetId =
        ConfigurationUtil.getMandatoryConfig(conf, BigQueryConfiguration.OUTPUT_DATASET_ID_KEY);
    String tableId =
        ConfigurationUtil.getMandatoryConfig(conf, BigQueryConfiguration.OUTPUT_TABLE_ID_KEY);

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
    String fieldsJson = conf.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY);
    if (!Strings.isNullOrEmpty(fieldsJson)) {
      try {
        TableSchema tableSchema = BigQueryTableHelper.createTableSchemaFromFields(fieldsJson);
        return Optional.of(BigQueryTableSchema.wrap(tableSchema));
      } catch (IOException e) {
        throw new IOException(
            "Unable to parse key '" + BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY + "'.", e);
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
    String fieldsJson = conf.get(BigQueryConfiguration.OUTPUT_TABLE_PARTITIONING_KEY);
    if (!Strings.isNullOrEmpty(fieldsJson)) {
      try {
        TimePartitioning tablePartitioning = BigQueryTimePartitioning.getFromJson(fieldsJson);
        return Optional.of(BigQueryTimePartitioning.wrap(tablePartitioning));
      } catch (IOException e) {
        throw new IOException(
            "Unable to parse key '" + BigQueryConfiguration.OUTPUT_TABLE_PARTITIONING_KEY + "'.",
            e);
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
    return conf.get(BigQueryConfiguration.OUTPUT_TABLE_KMS_KEY_NAME_KEY);
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
    String fileFormatName =
        ConfigurationUtil.getMandatoryConfig(conf, BigQueryConfiguration.OUTPUT_FILE_FORMAT_KEY);

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
    ConfigurationUtil.getMandatoryConfig(conf, BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY);

    Class<?> confClass = conf.getClass(BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY, null);

    // Fail if the default value was used, or the class isn't a FileOutputFormat.
    if (confClass == null) {
      throw new IOException(
          "Unable to resolve value for the configuration key '"
              + BigQueryConfiguration.OUTPUT_FORMAT_CLASS_KEY
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
   * Gets the write disposition of the output table. This specifies the action that occurs if the
   * destination table already exists. By default, if the table already exists, BigQuery appends
   * data to the output table.
   *
   * @param conf the configuration to reference the keys from.
   * @return the write disposition of the output table.
   */
  public static String getWriteDisposition(Configuration conf) {
    return conf.get(
        BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY,
        BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_DEFAULT);
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
