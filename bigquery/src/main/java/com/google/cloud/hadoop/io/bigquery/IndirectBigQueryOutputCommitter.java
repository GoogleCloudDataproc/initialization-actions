package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OutputCommitter to load files from GCS into BigQuery. */
public class IndirectBigQueryOutputCommitter extends FileOutputCommitter {
  // Logger
  protected static final Logger LOG =
      LoggerFactory.getLogger(IndirectBigQueryOutputCommitter.class);

  private final FileSystem outputFileSystem;
  private final Path outputPath;
  private final String projectId;
  private final TableReference destinationTable;
  private final TableSchema destinationSchema;
  private final BigQueryFileFormat sourceFormat;

  public IndirectBigQueryOutputCommitter(
      Path path, TaskAttemptContext context, BigQueryFileFormat sourceFormat) throws IOException {
    super(path, context);

    // Get a copy of the output path as it cannot be retrieved from the FileOutputCommitter.
    if (path != null) {
      outputFileSystem = path.getFileSystem(context.getConfiguration());
      outputPath = outputFileSystem.makeQualified(path);
    } else {
      throw new IOException(
          "Unable to resolve output path and file system. Is the configuration correct?");
    }

    // Ensure the configuration options are present before they're retrieved.
    Configuration config = context.getConfiguration();
    ConfigurationUtil.getMandatoryConfig(
        config, BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_OUTPUT);

    // Pull keys from the configuration.
    String outputProjectId = config.get(BigQueryConfiguration.OUTPUT_PROJECT_ID_KEY);
    String outputDatasetId = config.get(BigQueryConfiguration.OUTPUT_DATASET_ID_KEY);
    String outputTableId = config.get(BigQueryConfiguration.OUTPUT_TABLE_ID_KEY);
    String outputSchema = config.get(BigQueryConfiguration.OUTPUT_TABLE_SCHEMA_KEY);

    // Store the output project id.
    projectId = outputProjectId;

    // Create the destination table.
    destinationTable =
        new TableReference()
            .setProjectId(outputProjectId)
            .setDatasetId(outputDatasetId)
            .setTableId(outputTableId);

    List<TableFieldSchema> fieldSchema = BigQueryUtils.getSchemaFromString(outputSchema);
    destinationSchema = new TableSchema();
    destinationSchema.setFields(fieldSchema);

    this.sourceFormat = sourceFormat;

    // Mark the output GCS path to be deleted on exit in case of a failure.
    outputFileSystem.deleteOnExit(outputPath);
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    super.commitJob(context);

    // Enumerate over all files in the output path to import them.
    // TODO(user): Ensure only files created by the job are added.
    FileStatus[] outputFiles = outputFileSystem.listStatus(outputPath);
    ArrayList<String> sourceUris = new ArrayList<String>(outputFiles.length);
    for (int i = 0; i < outputFiles.length; i++) {
      FileStatus fileStatus = outputFiles[i];
      sourceUris.add(fileStatus.getPath().toString());
    }

    BigQueryHelper helper = null;
    try {
      helper = new BigQueryFactory().getBigQueryHelper(context.getConfiguration());
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }
    try {
      helper.importBigQueryFromGcs(
          projectId, destinationTable, destinationSchema, sourceFormat, sourceUris, true);
    } catch (InterruptedException e) {
      throw new IOException("Failed to import GCS into BigQuery", e);
    }

    cleanup();
  }

  @Override
  public void abortJob(JobContext context, State state) throws IOException {
    super.abortJob(context, state);

    cleanup();
  }

  /**
   * Attempts to manually delete data in the temporary output path. If this fails, another delete
   * attempt is made on JVM shutdown.
   *
   * @throws IOException if a FileSystem exception is encountered.
   */
  public void cleanup() throws IOException {
    if (outputFileSystem.exists(outputPath)) {
      if (outputFileSystem.delete(outputPath, true)) {
        LOG.info("Successfully deleted temporary GCS output path '{}'", outputPath);
      } else {
        LOG.warn(
            "Failed to delete temporary GCS output at '{}', retrying on shutdown.", outputPath);
      }
    }
  }
}
