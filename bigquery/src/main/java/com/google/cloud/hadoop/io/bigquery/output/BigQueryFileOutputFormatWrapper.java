package com.google.cloud.hadoop.io.bigquery.output;

import com.google.cloud.hadoop.io.bigquery.BigQueryFactory;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OutputFormat to interact with Google Cloud Storage and BigQuery. This acts as a wrapper around
 * an existing FileOutputFormat.
 */
@InterfaceStability.Unstable
public class BigQueryFileOutputFormatWrapper<K, V> extends OutputFormat<K, V> {

  /** Logger. */
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryFileOutputFormatWrapper.class);

  /**
   * Cached reference to the delegate, this may be null at any time. Use getDelegate to get a
   * non-null reference.
   */
  private FileOutputFormat<K, V> delegate = null;

  /**
   * Cached reference to the committer, this may be null at any time. Use getOutputCommitter to get
   * a non-null reference.
   */
  private OutputCommitter committer = null;

  /**
   * Checks to make sure the configuration is valid, the output path doesn't already exist, and that
   * a connection to BigQuery can be established.
   */
  @Override
  public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
    Configuration conf = job.getConfiguration();

    // Validate the output configuration.
    BigQueryOutputConfiguration.validateConfiguration(conf);

    // Error if the output path is missing.
    Path outputPath = FileOutputFormat.getOutputPath(job);
    if (outputPath == null) {
      throw new IOException("FileOutputFormat output path not set.");
    } else {
      LOG.info("Using output path '{}'.", outputPath);
    }

    // Error if the output path already exists.
    FileSystem outputFileSystem = outputPath.getFileSystem(conf);
    if (outputFileSystem.exists(outputPath)) {
      throw new IOException("The output path '" + outputPath + "' already exists.");
    }

    // Error if compression is set as there's mixed support in BigQuery.
    if (FileOutputFormat.getCompressOutput(job)) {
      throw new IOException("Compression isn't supported for this OutputFormat.");
    }

    // Error if unable to create a BigQuery helper.
    try {
      new BigQueryFactory().getBigQueryHelper(conf);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }

    // Let delegate process its checks.
    getDelegate(conf).checkOutputSpecs(job);
  }

  /** Gets the cached OutputCommitter, creating a new one if it doesn't exist. */
  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    // Cache the committer.
    if (committer == null) {
      committer = createCommitter(context);
    }
    return committer;
  }

  /** Gets the RecordWriter from the wrapped FileOutputFormat. */
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return getDelegate(conf).getRecordWriter(context);
  }

  /**
   * Create a new OutputCommitter for this OutputFormat.
   *
   * @param context the context to create the OutputCommitter from.
   * @return the new OutputCommitter for this format.
   * @throws IOException if there's an issue while creating the OutputCommitter.
   */
  protected OutputCommitter createCommitter(TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    return getDelegate(conf).getOutputCommitter(context);
  }

  /**
   * Gets a reference to the underlying delegate used by this OutputFormat.
   *
   * @param conf the configuration to derive the delegate from.
   * @return the underlying wrapped delegate.
   * @throws IOException if unable to get the delegate.
   */
  @SuppressWarnings("unchecked")
  protected synchronized FileOutputFormat<K, V> getDelegate(Configuration conf) throws IOException {
    if (delegate == null) {
      delegate = BigQueryOutputConfiguration.getFileOutputFormat(conf);
      LOG.info("Delegating functionality to '{}'.", delegate.getClass().getSimpleName());
    }
    return delegate;
  }

  /**
   * Sets delegate that this OutputFormat will wrap. This is exposed for testing purposes.
   *
   * @param delegate that this OutputFormat will wrap.
   */
  @VisibleForTesting
  void setDelegate(FileOutputFormat<K, V> delegate) {
    this.delegate = delegate;
  }
}
