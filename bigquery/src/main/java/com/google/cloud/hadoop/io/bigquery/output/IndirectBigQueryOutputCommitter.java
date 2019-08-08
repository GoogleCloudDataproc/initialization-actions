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

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class acts as a wrapper which delegates calls to another OutputCommitter whose
 * responsibility is to generate files in the defined output path. This class will ensure that those
 * file are imported into BigQuery and cleaned up locally.
 */
@InterfaceStability.Unstable
public class IndirectBigQueryOutputCommitter extends ForwardingBigQueryFileOutputCommitter {

  /**
   * This class acts as a wrapper which delegates calls to another OutputCommitter whose
   * responsibility is to generate files in the defined output path. This class will ensure that
   * those file are imported into BigQuery and cleaned up locally.
   *
   * @param context the context of the task.
   * @param delegate the OutputCommitter that this will delegate functionality to.
   * @throws IOException if there's an exception while validating the output path or getting the
   *     BigQueryHelper.
   */
  public IndirectBigQueryOutputCommitter(TaskAttemptContext context, OutputCommitter delegate)
      throws IOException {
    super(context, delegate);
  }

  /**
   * Runs an import job on BigQuery for the data in the output path in addition to calling the
   * delegate's commitJob.
   */
  @Override
  public void commitJob(JobContext context) throws IOException {
    super.commitJob(context);

    // Get the destination configuration information.
    Configuration conf = context.getConfiguration();
    TableReference destTable = BigQueryOutputConfiguration.getTableReference(conf);
    String jobProjectId = BigQueryOutputConfiguration.getJobProjectId(conf);
    String writeDisposition = BigQueryOutputConfiguration.getWriteDisposition(conf);
    String createDisposition = BigQueryOutputConfiguration.getCreateDisposition(conf);
    Optional<BigQueryTableSchema> destSchema = BigQueryOutputConfiguration.getTableSchema(conf);
    Optional<BigQueryTimePartitioning> timePartitioning =
        BigQueryOutputConfiguration.getTablePartitioning(conf);
    String kmsKeyName = BigQueryOutputConfiguration.getKmsKeyName(conf);
    BigQueryFileFormat outputFileFormat = BigQueryOutputConfiguration.getFileFormat(conf);
    List<String> sourceUris = getOutputFileURIs();

    try {
      getBigQueryHelper()
          .importFromGcs(
              jobProjectId,
              destTable,
              destSchema.isPresent() ? destSchema.get().get() : null,
              timePartitioning.isPresent() ? timePartitioning.get().get() : null,
              kmsKeyName,
              outputFileFormat,
              createDisposition,
              writeDisposition,
              sourceUris,
              true);
    } catch (InterruptedException e) {
      throw new IOException("Failed to import GCS into BigQuery", e);
    }

    cleanup(context);
  }

  /**
   * Performs a cleanup of the output path in addition to delegating the call to the wrapped
   * OutputCommitter.
   */
  @Override
  public void abortJob(JobContext context, State state) throws IOException {
    super.abortJob(context, state);
    cleanup(context);
  }
}
