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

  /** Wraps the delegate's committer in a {@link FederatedBigQueryOutputCommitter}. */
  @Override
  public OutputCommitter createCommitter(TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    OutputCommitter delegateCommitter = getDelegate(conf).getOutputCommitter(context);
    OutputCommitter committer = new FederatedBigQueryOutputCommitter(context, delegateCommitter);
    return committer;
  }
}
