package com.google.cloud.hadoop.io.bigquery.output;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An output format to write to Google Cloud Storage and then load that into BigQuery. This acts as
 * a wrapper around a FileOutputFormat, ensuring data is imported into BigQuery and cleaned up
 * locally.
 */
@InterfaceStability.Unstable
public class IndirectBigQueryOutputFormat<K, V>
    extends ForwardingBigQueryFileOutputFormat<K, V> {

  /** Wraps the delegate's committer in a {@link IndirectBigQueryOutputCommitter}. */
  @Override
  public OutputCommitter createCommitter(TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    OutputCommitter delegateCommitter = getDelegate(conf).getOutputCommitter(context);
    OutputCommitter committer = new IndirectBigQueryOutputCommitter(context, delegateCommitter);
    return committer;
  }
}
