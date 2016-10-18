package com.google.cloud.hadoop.io.bigquery.output;

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A container for configuration keys related to BigQuery indirect output formats. Alternatively,
 * the properties can be set in the configuration xml files with proper values.
 */
@InterfaceStability.Unstable
public class IndirectBigQueryOutputConfiguration {

  /**
   * Configuration key for project ID of the dataset accessed by this output connector. This key is
   * stored as a {@link String}.
   */
  public static final String PROJECT_ID = "mapreduce.bigquery.indirect.output.project.id";

  /**
   * Configuration key for numeric ID of the dataset accessed by this output connector. This key is
   * stored as a {@link String}.
   */
  public static final String DATASET_ID = "mapreduce.bigquery.indirect.output.dataset.id";

  /**
   * Configuration key for numeric ID of the table written by this output connector. This key is
   * stored as a {@link String}.
   */
  public static final String TABLE_ID = "mapreduce.bigquery.indirect.output.table.id";

  /**
   * Configuration key for the output table schema used by this output connector. If this key isn't
   * configured, the schema is auto detected. This key is stored as a serialized {@link
   * TableSchema}.
   */
  public static final String TABLE_SCHEMA = "mapreduce.bigquery.indirect.output.table.schema";

  /**
   * Configuration key for the file format of the files outputted by the wrapped FileOutputFormat.
   * This key is stored as a serialized {@link BigQueryFileFormat}.
   */
  public static final String FILE_FORMAT = "mapreduce.bigquery.indirect.output.fileformat";

  /**
   * Configuration key indicating whether temporary data stored in GCS should be deleted after the
   * job is complete. This is true by default. This key is stored as a {@link Boolean}.
   */
  public static final String DELETE_TEMPORARY_DATA =
      "mapreduce.bigquery.indirect.output.gcs.delete";

  /**
   * Configuration key for the FileOutputFormat class that's going to be wrapped. This key is stored
   * as a {@link Class}.
   */
  public static final String OUTPUT_FORMAT_CLASS =
      "mapreduce.bigquery.indirect.output.gcs.outputformatclass";

  /** A list of keys that are required for this output connector. */
  public static final List<String> REQUIRED_KEYS =
      ImmutableList.of(PROJECT_ID, DATASET_ID, TABLE_ID, FILE_FORMAT, OUTPUT_FORMAT_CLASS);

  /**
   * Sets the required output keys in the given configuration.
   *
   * @param conf the configuration to set the keys on.
   * @param fullTableId the fully qualified table id of the form: [projectId]:[datasetId].[tableId].
   * @param fileformat the file format that the wrapped FileOutputFormat will write files as.
   * @param outputFormatClass the FileOutputFormat to wrap and delegate functionality to.
   * @param tableSchema the output table schema for the table being written to. If this is null, the
   *     schema is auto-detected.
   */
  @SuppressWarnings("rawtypes")
  public static void configure(
      Configuration conf,
      String fullTableId,
      BigQueryFileFormat fileformat,
      Class<? extends FileOutputFormat> outputFormatClass,
      TableSchema tableSchema) {
    TableReference parsedTable = BigQueryStrings.parseTableReference(fullTableId);
    configure(
        conf,
        parsedTable.getProjectId(),
        parsedTable.getDatasetId(),
        parsedTable.getTableId(),
        fileformat,
        outputFormatClass,
        tableSchema);
  }

  /**
   * Sets the required output keys in the given configuration.
   *
   * @param conf the configuration to set the keys on.
   * @param projectId the project containing the table to write the results to.
   * @param datasetId the dataset to write the results to.
   * @param tableId the table to write the results to.
   * @param fileformat the file format that the wrapped FileOutputFormat will write files as.
   * @param outputFormatClass the FileOutputFormat to wrap and delegate functionality to.
   * @param tableSchema the output table schema for the table being written to. If this is null, the
   *     schema is auto-detected.
   */
  @SuppressWarnings("rawtypes")
  public static void configure(
      Configuration conf,
      String projectId,
      String datasetId,
      String tableId,
      BigQueryFileFormat fileformat,
      Class<? extends FileOutputFormat> outputFormatClass,
      TableSchema tableSchema) {

    // Check preconditions.
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId), "projectId must not be null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(datasetId), "datasetId must not be null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableId), "tableId must not be null or empty.");
    Preconditions.checkArgument(fileformat != null, "fileformat must not be null.");
    Preconditions.checkArgument(outputFormatClass != null, "outputFormatClass must not be null.");

    conf.set(PROJECT_ID, projectId);
    conf.set(DATASET_ID, datasetId);
    conf.set(TABLE_ID, tableId);
    conf.set(FILE_FORMAT, fileformat.name());
    conf.setClass(OUTPUT_FORMAT_CLASS, outputFormatClass, FileOutputFormat.class);

    // If a schema is provided, serialize it.
    if (tableSchema != null) {
      tableSchema.setFactory(JacksonFactory.getDefaultInstance());
      conf.set(TABLE_SCHEMA, tableSchema.toString());
    }
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
    getTableSchema(conf);
    getFileFormat(conf);
    getFileOutputFormat(conf);
  }

  /**
   * Gets the output table reference based on the given configuration.
   *
   * @param conf the configuration to reference the keys from.
   * @return a reference to the derived output table.
   * @throws IOException if a required key is missing.
   */
  protected static TableReference getTable(Configuration conf) throws IOException {
    // Ensure the BigQuery output information is valid.
    String projectId = ConfigurationUtil.getMandatoryConfig(conf, PROJECT_ID);
    String datasetId = ConfigurationUtil.getMandatoryConfig(conf, DATASET_ID);
    String tableId = ConfigurationUtil.getMandatoryConfig(conf, TABLE_ID);

    return new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId);
  }

  /**
   * Gets the output table schema based on the given configuration.
   *
   * @param conf the configuration to reference the keys from.
   * @return the derived table schema, null if no table schema exists in the configuration.
   * @throws IOException if a table schema was set in the configuration but couldn't be parsed.
   */
  protected static TableSchema getTableSchema(Configuration conf) throws IOException {
    String outputSchema = conf.get(TABLE_SCHEMA);
    if (!Strings.isNullOrEmpty(outputSchema)) {
      try {
        TableSchema schema = new TableSchema();
        JsonParser schemaParser =
            JacksonFactory.getDefaultInstance().createJsonParser(outputSchema);
        schemaParser.parseAndClose(schema);
        return schema;
      } catch (IOException e) {
        throw new IOException("Unable to parse key '" + TABLE_SCHEMA + "'.", e);
      }
    }
    return null;
  }

  protected static BigQueryFileFormat getFileFormat(Configuration conf) throws IOException {
    // Ensure the BigQuery output information is valid.
    String fileFormatName = ConfigurationUtil.getMandatoryConfig(conf, FILE_FORMAT);

    return BigQueryFileFormat.fromName(fileFormatName);
  }

  @SuppressWarnings("rawtypes")
  protected static FileOutputFormat getFileOutputFormat(Configuration conf) throws IOException {
    // Ensure the BigQuery output information is valid.
    ConfigurationUtil.getMandatoryConfig(conf, OUTPUT_FORMAT_CLASS);

    Class<?> confClass = conf.getClass(OUTPUT_FORMAT_CLASS, null);

    // Fail if the default value was used, or the class isn't a FileOutputFormat.
    if (confClass == null) {
      throw new IOException(
          "Unable to resolve value for the configuration key '" + OUTPUT_FORMAT_CLASS + "'.");
    } else if (!FileOutputFormat.class.isAssignableFrom(confClass)) {
      throw new IOException("The class " + confClass.getName() + " is not a FileOutputFormat.");
    }

    Class<? extends FileOutputFormat> fileOutputClass =
        confClass.asSubclass(FileOutputFormat.class);

    // Create a new instance and configure it if it's configurable.
    return ReflectionUtils.newInstance(fileOutputClass, conf);
  }
}
