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
package com.google.cloud.hadoop.io.bigquery;

import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.INPUT_DATASET_ID;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.INPUT_PROJECT_ID;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.INPUT_TABLE_ID;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_INPUT;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.PROJECT_ID;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.TEMP_GCS_PATH;
import static com.google.cloud.hadoop.util.ConfigurationUtil.getMandatoryConfig;
import static com.google.common.flogger.LazyArgs.lazy;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.util.HadoopToStringUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Abstract base class for BigQuery input formats. This class is expected to take care of performing
 * BigQuery exports to temporary tables, BigQuery exports to GCS and cleaning up any files or tables
 * that either of those processes create.
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class AbstractBigQueryInputFormat<K, V>
    extends InputFormat<K, V> implements DelegateRecordReaderFactory<K, V> {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** Configuration key for InputFormat class name. */
  public static final HadoopConfigurationProperty<Class<?>> INPUT_FORMAT_CLASS =
      new HadoopConfigurationProperty<>(
          "mapreduce.inputformat.class", AbstractBigQueryInputFormat.class);

  /**
   * The keyword for the type of BigQueryTable store externally.
   */
  public static final String EXTERNAL_TABLE_TYPE = "EXTERNAL";

  // Used by UnshardedExportToCloudStorage
  private InputFormat<LongWritable, Text> delegateInputFormat;

  /**
   * Configure the BigQuery input table for a job
   */
  public static void setInputTable(
      Configuration configuration, String projectId, String datasetId, String tableId)
      throws IOException {
    BigQueryConfiguration.configureBigQueryInput(configuration, projectId, datasetId, tableId);
  }

  /**
   * Configure the BigQuery input table for a job
   */
  public static void setInputTable(Configuration configuration, TableReference tableReference)
      throws IOException {
    setInputTable(
        configuration,
        tableReference.getProjectId(),
        tableReference.getDatasetId(),
        tableReference.getTableId());
  }

  /**
   * Configure a directory to which we will export BigQuery data
   */
  public static void setTemporaryCloudStorageDirectory(Configuration configuration, String path) {
    configuration.set(TEMP_GCS_PATH.getKey(), path);
  }

  /** Get the ExportFileFormat that this input format supports. */
  public abstract ExportFileFormat getExportFileFormat();

  @SuppressWarnings("unchecked")
  protected static ExportFileFormat getExportFileFormat(Configuration configuration) {
    Class<? extends AbstractBigQueryInputFormat<?, ?>> clazz =
        (Class<? extends AbstractBigQueryInputFormat<?, ?>>)
            INPUT_FORMAT_CLASS.get(configuration, configuration::getClass);
    Preconditions.checkState(
        AbstractBigQueryInputFormat.class.isAssignableFrom(clazz),
        "Expected input format to derive from AbstractBigQueryInputFormat");
    return getExportFileFormat(clazz);
  }

  protected static ExportFileFormat getExportFileFormat(
      Class<? extends AbstractBigQueryInputFormat<?, ?>> clazz) {
    try {
      AbstractBigQueryInputFormat<?, ?> format = clazz.getConstructor().newInstance();
      return format.getExportFileFormat();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    logger.atFine().log("getSplits(%s)", lazy(() -> HadoopToStringUtil.toString(context)));

    final Configuration configuration = context.getConfiguration();
    BigQueryHelper bigQueryHelper;
    try {
      bigQueryHelper = getBigQueryHelper(configuration);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }

    String exportPath =
        BigQueryConfiguration.getTemporaryPathRoot(configuration, context.getJobID());
    configuration.set(TEMP_GCS_PATH.getKey(), exportPath);

    Export export = constructExport(
        configuration,
        getExportFileFormat(),
        exportPath,
        bigQueryHelper,
        delegateInputFormat);
    export.prepare();

    // Invoke the export, maybe wait for it to complete.
    try {
      export.beginExport();
      export.waitForUsableMapReduceInput();
    } catch (IOException ie) {
      throw new IOException("Error while exporting: " + HadoopToStringUtil.toString(context), ie);
    }

    List<InputSplit> splits = export.getSplits(context);

    if (logger.atFine().isEnabled()) {
      // Stringifying a really big list of splits can be expensive, so we guard with
      // isDebugEnabled().
      logger.atFine().log("getSplits -> %s", HadoopToStringUtil.toString(splits));
    }
    return splits;
  }

  @Override
  public RecordReader<K, V> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return createRecordReader(inputSplit, taskAttemptContext.getConfiguration());
  }

  public RecordReader<K, V> createRecordReader(
      InputSplit inputSplit, Configuration configuration)
      throws IOException, InterruptedException {
    Preconditions.checkArgument(
        inputSplit instanceof UnshardedInputSplit,
        "Split should be instance of UnshardedInputSplit.");
      logger.atFine().log("createRecordReader -> createDelegateRecordReader()");
      return createDelegateRecordReader(inputSplit, configuration);
  }

  private static Export constructExport(
      Configuration configuration,
      ExportFileFormat format,
      String exportPath,
      BigQueryHelper bigQueryHelper,
      InputFormat<LongWritable, Text> delegateInputFormat)
      throws IOException {
    logger.atFine().log("constructExport() with export path %s", exportPath);

    // Extract relevant configuration settings.
    Map<String, String> mandatoryConfig =
        getMandatoryConfig(configuration, MANDATORY_CONFIG_PROPERTIES_INPUT);
    String jobProjectId = mandatoryConfig.get(PROJECT_ID.getKey());
    String inputProjectId = mandatoryConfig.get(INPUT_PROJECT_ID.getKey());
    String datasetId = mandatoryConfig.get(INPUT_DATASET_ID.getKey());
    String tableName = mandatoryConfig.get(INPUT_TABLE_ID.getKey());

    TableReference exportTableReference = new TableReference()
        .setDatasetId(datasetId)
        .setProjectId(inputProjectId)
        .setTableId(tableName);
    Table table = bigQueryHelper.getTable(exportTableReference);

    if (EXTERNAL_TABLE_TYPE.equals(table.getType())) {
        logger.atInfo().log("Table is already external, so skipping export");
        return new NoopFederatedExportToCloudStorage(
            configuration, format, bigQueryHelper, jobProjectId, table, delegateInputFormat);
    }

    return new UnshardedExportToCloudStorage(
        configuration,
        exportPath,
        format,
        bigQueryHelper,
        jobProjectId,
        table,
        delegateInputFormat);
  }

  /**
   * Cleans up relevant temporary resources associated with a job which used the
   * GsonBigQueryInputFormat; this should be called explicitly after the completion of the entire
   * job. Possibly cleans up intermediate export tables if configured to use one due to
   * specifying a BigQuery "query" for the input. Cleans up the GCS directoriy where BigQuery
   * exported its files for reading.
   */
  public static void cleanupJob(Configuration configuration, JobID jobId) throws IOException {
    String exportPathRoot = BigQueryConfiguration.getTemporaryPathRoot(configuration, jobId);
    configuration.set(TEMP_GCS_PATH.getKey(), exportPathRoot);
    Bigquery bigquery;
    try {
      bigquery = new BigQueryFactory().getBigQuery(configuration);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create Bigquery client", gse);
    }
    cleanupJob(new BigQueryHelper(bigquery), configuration);
  }

  /**
   * Similar to {@link #cleanupJob(Configuration, JobID)}, but allows specifying the Bigquery
   * instance to use.
   *
   * @param bigQueryHelper The Bigquery API-client helper instance to use.
   * @param config The job Configuration object which contains settings such as whether sharded
   *     export was enabled, which GCS directory the export was performed in, etc.
   */
  public static void cleanupJob(BigQueryHelper bigQueryHelper, Configuration config)
      throws IOException {
    logger.atFine().log("cleanupJob(Bigquery, Configuration)");

    String gcsPath = getMandatoryConfig(config, TEMP_GCS_PATH);

    Export export = constructExport(
        config, getExportFileFormat(config), gcsPath, bigQueryHelper, null);

    try {
      export.cleanupExport();
    } catch (IOException ioe) {
      // Error is swallowed as job has completed successfully and the only failure is deleting
      // temporary data.
      // This matches the FileOutputCommitter pattern.
      logger.atWarning().withCause(ioe).log(
          "Could not delete intermediate data from BigQuery export");
    }
  }

  /**
   * Helper method to override for testing.
   *
   * @return Bigquery.
   * @throws IOException on IO Error.
   * @throws GeneralSecurityException on security exception.
   */
  protected Bigquery getBigQuery(Configuration config)
      throws GeneralSecurityException, IOException {
    BigQueryFactory factory = new BigQueryFactory();
    return factory.getBigQuery(config);
  }

  /**
   * Helper method to override for testing.
   */
  protected BigQueryHelper getBigQueryHelper(Configuration config)
      throws GeneralSecurityException, IOException {
    BigQueryFactory factory = new BigQueryFactory();
    return factory.getBigQueryHelper(config);
  }

  @VisibleForTesting
  void setDelegateInputFormat(InputFormat inputFormat) {
    delegateInputFormat = inputFormat;
  }
}
