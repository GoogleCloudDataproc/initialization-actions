package com.google.cloud.hadoop.io.bigquery.output;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
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
 * An output format to write to Google Cloud Storage and then load that into BigQuery. This acts as
 * a wrapper around a FileOutputFormat, ensuring data is imported into BigQuery and cleaned up
 * locally.
 */
@InterfaceStability.Unstable
public class IndirectBigQueryOutputFormatWrapper<K, V> extends OutputFormat<K, V> {

  /** Logger. */
  private static final Logger LOG =
      LoggerFactory.getLogger(IndirectBigQueryOutputFormatWrapper.class);

  /**
   * Cached reference to the delegate, this may be null at any time. Use getDelegate to get a
   * non-null reference.
   */
  private FileOutputFormat<K, V> delegate = null;

  /**
   * Cached reference to the committer, this may be null at any time. Use getOutputCommitter to get
   * a non-null reference.
   */
  private IndirectBigQueryOutputCommitterWrapper committer = null;

  @Override
  public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
    Configuration conf = job.getConfiguration();

    // Validate the output configuration.
    IndirectBigQueryOutputConfiguration.validateConfiguration(conf);

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

    // Let delegate process its checks.
    getDelegate(conf).checkOutputSpecs(job);
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (committer == null) {
      Configuration conf = context.getConfiguration();
      OutputCommitter delegateCommitter = getDelegate(conf).getOutputCommitter(context);
      Path output = FileOutputFormat.getOutputPath(context);

      // Wrap the delegate's OutputCommitter.
      committer = new IndirectBigQueryOutputCommitterWrapper(delegateCommitter, output, context);
    }
    return committer;
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    return getDelegate(conf).getRecordWriter(context);
  }

  /**
   * Gets a reference to the underlying wrapped delegate used by this output format.
   *
   * @param conf the configuration to derive the delegate from.
   * @return the underlying wrapped delegate.
   * @throws IOException if unable to get the delegate.
   */
  @SuppressWarnings("unchecked")
  protected synchronized FileOutputFormat<K, V> getDelegate(Configuration conf) throws IOException {
    if (delegate == null) {
      delegate = IndirectBigQueryOutputConfiguration.getFileOutputFormat(conf);
      LOG.info("Delegating functionality to '{}'.", delegate.getClass().getSimpleName());
    }
    return delegate;
  }

  /**
   * Sets delegate for testing purposes.
   *
   * @param delegate the delegate to set.
   */
  @VisibleForTesting
  void setDelegate(FileOutputFormat<K, V> delegate) {
    this.delegate = delegate;
  }
}
