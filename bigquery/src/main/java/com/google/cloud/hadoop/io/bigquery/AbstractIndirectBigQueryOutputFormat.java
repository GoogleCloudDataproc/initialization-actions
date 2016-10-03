package com.google.cloud.hadoop.io.bigquery;

import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Experimental, API subject to change.<br>
 * Abstract output format to write to Google Cloud Storage and then load that into Bigquery.
 */
@InterfaceStability.Unstable
public abstract class AbstractIndirectBigQueryOutputFormat<T extends FileOutputFormat<K, V>, K, V>
    extends FileOutputFormat<K, V> {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractIndirectBigQueryOutputFormat.class);

  private final T delegate;
  private IndirectBigQueryOutputCommitter committer = null;

  public AbstractIndirectBigQueryOutputFormat(T delegate) {
    this.delegate = delegate;
  }

  /**
   * Set the {@link Path} of the output directory for the map-reduce job.
   *
   * @param context the context to modify
   * @param outputDir the {@link Path} of the output directory for the map-reduce job.
   */
  protected static void setOutputPath(JobContext context, String outputDir) throws IOException {
    // TODO(user): Fix, turns out the temp job maintains its own config reference.
    Job tempJob = new Job(context.getConfiguration(), context.getJobName());
    FileOutputFormat.setOutputPath(tempJob, new Path(outputDir));
  }

  /**
   * Gets the format of the files that this OutputFormat will generate. This is used for loading
   * into Bigquery.
   *
   * @return the format of the files that this OutputFormat will generate.
   */
  public abstract BigQueryFileFormat getSourceFormat();

  @Override
  public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
    Configuration config = job.getConfiguration();

    // Ensure the BigQuery output information is valid.
    ConfigurationUtil.getMandatoryConfig(
        config, BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_OUTPUT);

    // Check if a path is provided in FileOutputFormat.
    if (FileOutputFormat.getOutputPath(job) == null) {
      // If it isn't provided, attempt to get the default working path.
      String path = BigQueryConfiguration.getTemporaryPathRoot(config, job.getJobID());
      if (!Strings.isNullOrEmpty(path)) {
        LOG.info("Setting path to {}", path);
        config.set("mapreduce.output.fileoutputformat.outputdir", path);
        // TODO(user): (See above) Fix, turns out the temp job maintains its own config reference.
        // setOutputPath(job, path);
      } else {
        throw new IOException("Unable to resolve GCS working path.");
      }
    }

    // Error on compression as its support is spotty in BigQuery.
    if (FileOutputFormat.getCompressOutput(job)) {
      throw new IOException(
          "Compression isn't supported in FileOutputFormat when using "
              + "IndirectBigQueryOutputFormat.");
    }

    // Let FileOutputFormat process its checks.
    super.checkOutputSpecs(job);
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (committer == null) {
      Path output = FileOutputFormat.getOutputPath(context);
      committer = new IndirectBigQueryOutputCommitter(output, context, getSourceFormat());
    }
    return committer;
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return delegate.getRecordWriter(context);
  }
}
