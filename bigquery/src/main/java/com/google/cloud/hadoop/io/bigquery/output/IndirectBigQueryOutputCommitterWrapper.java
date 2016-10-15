package com.google.cloud.hadoop.io.bigquery.output;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OutputCommitter to load files from GCS into BigQuery. This acts as a wrapper which delegates
 * calls to another OutputCommitter whose responsibility is to generate files in the defined output
 * path. This class will ensure that those file are imported into BigQuery and cleaned up locally.
 */
@InterfaceStability.Unstable
public class IndirectBigQueryOutputCommitterWrapper extends OutputCommitter {

  /** Logger. */
  protected static final Logger LOG =
      LoggerFactory.getLogger(IndirectBigQueryOutputCommitterWrapper.class);

  /** The delegate OutputCommitter being wrapped. */
  private final OutputCommitter delegate;

  /** The output file system for the intermediary data in GCS. */
  private final FileSystem outputFileSystem;

  /** The output path for the intermediary data in GCS. */
  private final Path outputPath;

  /** The format of the file in GCS. */
  private final BigQueryFileFormat outputFileFormat;

  /** The project id for the destination data in BigQuer. */
  private final String destProjectId;

  /** The destination table for the data in BigQuery. */
  private final TableReference destTable;

  /** The schema of the destination table. This may be null. */
  private final TableSchema destSchema;

  private BigQueryHelper bigQueryHelper;

  /**
   * Creates a new IndirectBigQueryOutputCommitter. This acts as a wrapper which delegates calls to
   * another OutputCommitter whose responsibility is to generate files in the defined output path.
   * This class will ensure that those file are imported into BigQuery and cleaned up locally.
   *
   * @param delegate the OutputCommitter that this will delegate functionality to.
   * @param path the GCS path where the files are committed to before being loaded into BigQuery.
   * @param context the context of the task.
   * @throws IOException if an IOError is thrown.
   */
  public IndirectBigQueryOutputCommitterWrapper(
      OutputCommitter delegate, Path path, TaskAttemptContext context) throws IOException {

    this.delegate = delegate;

    // Ensure the BigQuery output information is valid.
    Configuration conf = context.getConfiguration();
    IndirectBigQueryOutputConfiguration.validateConfiguration(conf);

    // Get a defensive copy of the output path as it cannot be retrieved from the OutputCommitter.
    if (path != null) {
      outputFileSystem = path.getFileSystem(conf);
      outputPath = outputFileSystem.makeQualified(path);
    } else {
      throw new IOException("Unable to resolve output path and file system.");
    }

    // Create a big query reference
    try {
      bigQueryHelper = new BigQueryFactory().getBigQueryHelper(conf);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }

    // Get the destination configuration information.
    outputFileFormat = IndirectBigQueryOutputConfiguration.getFileFormat(conf);
    destSchema = IndirectBigQueryOutputConfiguration.getTableSchema(conf);
    destTable = IndirectBigQueryOutputConfiguration.getTable(conf);
    destProjectId = destTable.getProjectId();
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    delegate.commitJob(context);

    // Enumerate over all files in the output path to import them.
    FileStatus[] outputFiles = outputFileSystem.listStatus(outputPath);
    ArrayList<String> sourceUris = new ArrayList<String>(outputFiles.length);
    for (int i = 0; i < outputFiles.length; i++) {
      FileStatus fileStatus = outputFiles[i];
      // Skip the success file and directories as they're not relevant to BigQuery.
      if (!fileStatus.isDir()
          && !fileStatus.getPath().getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME)) {
        sourceUris.add(fileStatus.getPath().toString());
      }
    }

    try {
      bigQueryHelper.importBigQueryFromGcs(
          destProjectId, destTable, destSchema, outputFileFormat, sourceUris, true);
    } catch (InterruptedException e) {
      throw new IOException("Failed to import GCS into BigQuery", e);
    }

    // Make an explicit call to cleanup.
    cleanup();
  }

  @Override
  public void abortJob(JobContext context, State state) throws IOException {
    delegate.abortJob(context, state);

    // Make an explicit call to cleanup.
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
      LOG.info("Found temporary GCS output data at '{}', attempting to clean up.", outputPath);
      if (outputFileSystem.delete(outputPath, true)) {
        LOG.info("Successfully deleted temporary GCS output path '{}'.", outputPath);
      } else {
        LOG.warn(
            "Failed to delete temporary GCS output at '{}', retrying on shutdown.", outputPath);
        outputFileSystem.deleteOnExit(outputPath);
      }
    }
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    delegate.abortTask(context);
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    delegate.commitTask(context);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return delegate.needsTaskCommit(context);
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    delegate.setupJob(context);
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    delegate.setupTask(context);
  }

  /**
   * Sets helper for testing purposes.
   *
   * @param bigQueryHelper the bigQueryHelper to set.
   */
  @VisibleForTesting
  void setBigQueryHelper(BigQueryHelper bigQueryHelper) {
    this.bigQueryHelper = bigQueryHelper;
  }

  /** Sets gets the delegate for testing purposes. */
  @VisibleForTesting
  OutputCommitter getDelegate() {
    return delegate;
  }
}
