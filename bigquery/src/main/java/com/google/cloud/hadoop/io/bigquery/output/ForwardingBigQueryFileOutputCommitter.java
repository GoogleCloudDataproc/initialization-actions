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

import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
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
 * This class acts as a wrapper which delegates calls to another OutputCommitter whose
 * responsibility is to generate files in the defined output path.
 */
@InterfaceStability.Unstable
public class ForwardingBigQueryFileOutputCommitter extends OutputCommitter {

  /** Logger. */
  private static final Logger LOG =
      LoggerFactory.getLogger(ForwardingBigQueryFileOutputCommitter.class);

  /** The delegate OutputCommitter being wrapped. */
  private final OutputCommitter delegate;

  /** The output file system for the intermediary data in GCS. */
  private final FileSystem outputFileSystem;

  /** The output path for the intermediary data in GCS. */
  private final Path outputPath;

  /** The helper used to interact with BigQuery. Non-final for unit testing. */
  private BigQueryHelper bigQueryHelper;

  /**
   * This acts as a wrapper which delegates calls to another OutputCommitter whose responsibility is
   * to generate files in the defined output path.
   *
   * @param context the context of the task.
   * @param delegate the OutputCommitter that this will delegate functionality to.
   * @throws IOException if there's an exception while validating the output path or getting the
   *     BigQueryHelper.
   */
  public ForwardingBigQueryFileOutputCommitter(TaskAttemptContext context, OutputCommitter delegate)
      throws IOException {
    this.delegate = delegate;

    // Get the configuration and ensure it's valid.
    Configuration conf = context.getConfiguration();
    BigQueryOutputConfiguration.validateConfiguration(conf);

    // Resolve the output path.
    Path path = BigQueryOutputConfiguration.getGcsOutputPath(conf);
    outputFileSystem = path.getFileSystem(conf);
    outputPath = outputFileSystem.makeQualified(path);

    // Create a big query reference
    try {
      bigQueryHelper = BigQueryFactory.INSTANCE.getBigQueryHelper(conf);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }
  }

  /** Calls the delegate's {@link OutputCommitter#commitJob(JobContext)}. */
  @Override
  public void commitJob(JobContext context) throws IOException {
    delegate.commitJob(context);
  }

  /** Calls the delegate's {@link OutputCommitter#abortJob(JobContext, State)}. */
  @Override
  public void abortJob(JobContext context, State state) throws IOException {
    delegate.abortJob(context, state);
  }

  /** Calls the delegate's {@link OutputCommitter#abortTask(TaskAttemptContext)}. */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    delegate.abortTask(context);
  }

  /** Calls the delegate's {@link OutputCommitter#commitTask(TaskAttemptContext)}. */
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    delegate.commitTask(context);
  }

  /** Calls the delegate's {@link OutputCommitter#needsTaskCommit(TaskAttemptContext)}. */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return delegate.needsTaskCommit(context);
  }

  /** Calls the delegate's {@link OutputCommitter#setupJob(JobContext)}. */
  @Override
  public void setupJob(JobContext context) throws IOException {
    delegate.setupJob(context);
  }

  /** Calls the delegate's {@link OutputCommitter#setupTask(TaskAttemptContext)}. */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    delegate.setupTask(context);
  }

  /**
   * Queries the file system for the URIs of all files in the base output directory that are not
   * directories and whose name isn't {@link FileOutputCommitter#SUCCEEDED_FILE_NAME}.
   *
   * @return a list of all URIs in the form of strings.
   * @throws IOException if unable to query for the files in the base output directory.
   */
  protected List<String> getOutputFileURIs() throws IOException {
    // Enumerate over all files in the output path.
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

    return sourceUris;
  }

  /**
   * Attempts to manually delete data in the output path. If this fails, another delete attempt is
   * made on JVM shutdown.
   *
   * @param context the job content to cleanup.
   * @throws IOException if a FileSystem exception is encountered.
   */
  protected void cleanup(JobContext context) throws IOException {
    boolean delete =
        BigQueryOutputConfiguration.getCleanupTemporaryDataFlag(context.getConfiguration());

    if (delete && outputFileSystem.exists(outputPath)) {
      LOG.info("Found GCS output data at '{}', attempting to clean up.", outputPath);
      if (outputFileSystem.delete(outputPath, true)) {
        LOG.info("Successfully deleted GCS output path '{}'.", outputPath);
      } else {
        LOG.warn("Failed to delete GCS output at '{}', retrying on shutdown.", outputPath);
        outputFileSystem.deleteOnExit(outputPath);
      }
    }
  }

  /**
   * Gets the delegate OutputCommitter being wrapped.
   *
   * @return the delegate OutputCommitter being wrapped.
   */
  protected OutputCommitter getDelegate() {
    return delegate;
  }

  /**
   * Gets the helper used to interact with BigQuery.
   *
   * @return the helper used to interact with BigQuery.
   */
  protected BigQueryHelper getBigQueryHelper() {
    return bigQueryHelper;
  }

  /**
   * Sets the {@link BigQueryHelper} used by this class. This is exposed for testing purposes.
   *
   * @param bigQueryHelper the BigQueryHelper to use.
   */
  @VisibleForTesting
  void setBigQueryHelper(BigQueryHelper bigQueryHelper) {
    this.bigQueryHelper = bigQueryHelper;
  }
}
