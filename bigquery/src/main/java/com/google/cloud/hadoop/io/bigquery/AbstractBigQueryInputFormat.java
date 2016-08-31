package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.cloud.hadoop.util.HadoopToStringUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for BigQuery input formats. This class is expected to take care of performing
 * BigQuery exports to temporary tables, BigQuery exports to GCS and cleaning up any files or tables
 * that either of those processes create.
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class AbstractBigQueryInputFormat<K, V>
    extends InputFormat<K, V> implements DelegateRecordReaderFactory<K, V> {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractBigQueryInputFormat.class);
  /**
   * Configuration key for InputFormat class name.
   */
  public static final String INPUT_FORMAT_CLASS_KEY = "mapreduce.inputformat.class";

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
    configuration.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, path);
  }

  /**
   * Enable or disable BigQuery sharded output.
   */
  public static void setEnableShardedExport(Configuration configuration, boolean enabled) {
    configuration.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, enabled);
  }

  protected static boolean isShardedExportEnabled(Configuration configuration) {
    return configuration.getBoolean(
        BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY,
        BigQueryConfiguration.ENABLE_SHARDED_EXPORT_DEFAULT);
  }

  /**
   * Get the ExportFileFormat that this input format supports.
   */
  public abstract ExportFileFormat getExportFileFormat();

  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException {
    LOG.debug("getSplits({})", HadoopToStringUtil.toString(context));
    Preconditions.checkNotNull(context.getJobID(), "getSplits requires a jobID");

    final Configuration configuration = context.getConfiguration();
    final JobID jobId = context.getJobID();
    BigQueryHelper bigQueryHelper = null;
    try {
      bigQueryHelper = getBigQueryHelper(configuration);
    } catch (GeneralSecurityException gse) {
      LOG.error("Failed to create BigQuery client", gse);
      throw new IOException("Failed to create BigQuery client", gse);
    }

    String exportPath = extractExportPathRoot(configuration, jobId);
    configuration.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, exportPath);

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
    } catch (IOException | InterruptedException ie) {
      LOG.error("Error while exporting", ie);
      throw new IOException("Error while exporting", ie);
    }

    List<InputSplit> splits = export.getSplits(context);

    if (LOG.isDebugEnabled()) {
      try {
        // Stringifying a really big list of splits can be expensive, so we guard with
        // isDebugEnabled().
        LOG.debug("getSplits -> {}", HadoopToStringUtil.toString(splits));
      } catch (InterruptedException e) {
        LOG.debug("getSplits -> {}", "*exception on toString()*");
      }
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
    if (isShardedExportEnabled(configuration)) {
      Preconditions.checkArgument(
          inputSplit instanceof ShardedInputSplit,
          "Split should be instance of ShardedInputSplit.");
      LOG.debug("createRecordReader -> DynamicFileListRecordReader");
      return new DynamicFileListRecordReader<>(this);
    } else {
      Preconditions.checkArgument(
          inputSplit instanceof UnshardedInputSplit,
          "Split should be instance of UnshardedInputSplit.");
      LOG.debug("createRecordReader -> createDelegateRecordReader()");
      return createDelegateRecordReader(inputSplit, configuration);
    }
  }

  private static Export constructExport(
      Configuration configuration, ExportFileFormat format, String exportPath,
      BigQueryHelper bigQueryHelper, InputFormat delegateInputFormat)
      throws IOException {
    LOG.debug("contructExport() with export path {}", exportPath);

    // Extract relevant configuration settings.
    Map<String, String> mandatoryConfig = ConfigurationUtil.getMandatoryConfig(
        configuration, BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_INPUT);
    String jobProjectId = mandatoryConfig.get(BigQueryConfiguration.PROJECT_ID_KEY);
    String inputProjectId = mandatoryConfig.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY);
    String datasetId = mandatoryConfig.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY);
    String tableName = mandatoryConfig.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY);

    TableReference exportTableReference = new TableReference()
        .setDatasetId(datasetId)
        .setProjectId(inputProjectId)
        .setTableId(tableName);
    Table table = bigQueryHelper.getTable(exportTableReference);

    String query = configuration.get(BigQueryConfiguration.INPUT_QUERY_KEY);

    if (EXTERNAL_TABLE_TYPE.equals(table.getType())) {
      if (Strings.isNullOrEmpty(query)) {
        LOG.info("Table is already external, so skipping export");
        // Otherwise getSplits gets confused.
        setEnableShardedExport(configuration, false);
        return new NoopFederatedExportToCloudStorage(
            configuration, format, bigQueryHelper, jobProjectId, table, delegateInputFormat);
      } else {
        LOG.info("Ignoring use of federated data source, because a query was specified.");
      }
    }

    boolean enableShardedExport = isShardedExportEnabled(configuration);
    boolean deleteTableOnExit = configuration.getBoolean(
        BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY,
        BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_DEFAULT);

    LOG.debug(
        "isShardedExportEnabled = %s, deleteTableOnExit = %s, tableReference = %s, query = %s",
        enableShardedExport,
        deleteTableOnExit,
        BigQueryStrings.toString(exportTableReference),
        query);

    Export export;
    if (enableShardedExport) {
      export = new ShardedExportToCloudStorage(
          configuration,
          exportPath,
          format,
          bigQueryHelper,
          jobProjectId,
          table);
    } else {
      export = new UnshardedExportToCloudStorage(
          configuration,
          exportPath,
          format,
          bigQueryHelper,
          jobProjectId,
          table,
          delegateInputFormat);
    }

    if (!Strings.isNullOrEmpty(query)) {
      // A query was specified. In this case we want to add add prepare and cleanup steps
      // via the QueryBasedExport.
      export = new QueryBasedExport(
          export, query, jobProjectId, bigQueryHelper, exportTableReference, deleteTableOnExit);
    }

    return export;
  }

  /**
   * Either resolves an export path based on GCS_BUCKET_KEY and JobID, or defers to a pre-provided
   * BigQueryConfiguration.TEMP_GCS_PATH_KEY.
   */
  protected static String extractExportPathRoot(Configuration configuration, JobID jobId)
      throws IOException {
    String exportPathRoot = configuration.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY);
    if (Strings.isNullOrEmpty(exportPathRoot)) {
      LOG.info("Fetching mandatory field '{}' since '{}' isn't set explicitly",
          BigQueryConfiguration.GCS_BUCKET_KEY, BigQueryConfiguration.TEMP_GCS_PATH_KEY);
      String gcsBucket = ConfigurationUtil.getMandatoryConfig(
          configuration, BigQueryConfiguration.GCS_BUCKET_KEY);
      exportPathRoot = String.format(
          "gs://%s/hadoop/tmp/bigquery/%s", gcsBucket, jobId);
      LOG.info("Resolved GCS export path: '{}'", exportPathRoot);
    } else {
      LOG.info("Using user-provided custom export path: '{}'", exportPathRoot);
    }

    Path exportPath = new Path(exportPathRoot);

    FileSystem fs = exportPath.getFileSystem(configuration);
    Preconditions.checkState(
        fs instanceof GoogleHadoopFileSystemBase,
        "Export FS must derive from GoogleHadoopFileSystemBase.");
    return exportPathRoot;
  }

  /**
   * Cleans up relevant temporary resources associated with a job which used the
   * GsonBigQueryInputFormat; this should be called explicitly after the completion of the entire
   * job. Possibly cleans up intermediate export tables if configured to use one due to
   * specifying a BigQuery "query" for the input. Cleans up the GCS directoriy where BigQuery
   * exported its files for reading.
   *
   * @param context The JobContext which contains the full configuration plus JobID which matches
   *     the JobContext seen in the corresponding BigQueryInptuFormat.getSplits() setup.
   *
   * @deprecated Use {@link #cleanupJob(Configuration, JobID)}
   */
  @Deprecated
  public static void cleanupJob(JobContext context)
      throws IOException {
    // Since cleanupJob may be called from a place where the actual runtime Configuration isn't
    // available, we must re-walk the same logic for generating the export path based on settings
    // and JobID in the context.
    cleanupJob(context.getConfiguration(), context.getJobID());
  }

  /**
   * Cleans up relevant temporary resources associated with a job which used the
   * GsonBigQueryInputFormat; this should be called explicitly after the completion of the entire
   * job. Possibly cleans up intermediate export tables if configured to use one due to
   * specifying a BigQuery "query" for the input. Cleans up the GCS directoriy where BigQuery
   * exported its files for reading.
   */
  public static void cleanupJob(Configuration configuration, JobID jobId) throws IOException {
    String exportPathRoot = extractExportPathRoot(configuration, jobId);
    configuration.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, exportPathRoot);
    Bigquery bigquery = null;
    try {
      bigquery = new BigQueryFactory().getBigQuery(configuration);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create Bigquery client", gse);
    }
    cleanupJob(new BigQueryHelper(bigquery), configuration);
  }

  /**
   * Similar to {@link #cleanupJob(JobContext)}, but allows specifying the Bigquery instance to use.
   *
   * @param bigQueryHelper The Bigquery API-client helper instance to use.
   * @param config The job Configuration object which contains settings such as whether sharded
   *     export was enabled, which GCS directory the export was performed in, etc.
   */
  public static void cleanupJob(BigQueryHelper bigQueryHelper, Configuration config)
      throws IOException {
    LOG.debug("cleanupJob(Bigquery, Configuration)");

    String gcsPath = ConfigurationUtil.getMandatoryConfig(
        config, BigQueryConfiguration.TEMP_GCS_PATH_KEY);

    Export export = constructExport(
        config, getExportFileFormat(config), gcsPath, bigQueryHelper, null);

    try {
      export.cleanupExport();
    } catch (IOException ioe) {
      // Error is swallowed as job has completed successfully and the only failure is deleting
      // temporary data.
      // This matches the FileOutputCommitter pattern.
      LOG.warn(
          "Could not delete intermediate data from BigQuery export", ioe);
    }
  }

  @SuppressWarnings("unchecked")
  protected static ExportFileFormat getExportFileFormat(Configuration configuration) {
    Class<? extends AbstractBigQueryInputFormat<?, ?>> clazz =
        (Class<? extends AbstractBigQueryInputFormat<?, ?>>) configuration.getClass(
            INPUT_FORMAT_CLASS_KEY, AbstractBigQueryInputFormat.class);
    Preconditions.checkState(
        AbstractBigQueryInputFormat.class.isAssignableFrom(clazz),
        "Expected input format to derive from AbstractBigQueryInputFormat");
    return getExportFileFormat(clazz);
  }

  protected static ExportFileFormat getExportFileFormat(
      Class<? extends AbstractBigQueryInputFormat<?, ?>> clazz) {
    try {
      AbstractBigQueryInputFormat<?, ?> format = clazz.newInstance();
      return format.getExportFileFormat();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
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
