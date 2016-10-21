package com.google.cloud.hadoop.io.bigquery.output;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An output format to write to Google Cloud Storage and then load that into BigQuery. This acts as
 * a wrapper around a FileOutputFormat, ensuring that a federated BigQuery table is created linking
 * to the generated data.
 */
@InterfaceStability.Unstable
public class FederatedBigQueryOutputFormat<K, V> extends ForwardingBigQueryFileOutputFormat<K, V> {

  /** Wraps the delegate's committer in a {@link IndirectBigQueryOutputCommitter}. */
  @Override
  public OutputCommitter createCommitter(TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    OutputCommitter delegateCommitter = getDelegate(conf).getOutputCommitter(context);
    OutputCommitter committer = new FederatedBigQueryOutputCommitter(context, delegateCommitter);
    return committer;
  }
}
