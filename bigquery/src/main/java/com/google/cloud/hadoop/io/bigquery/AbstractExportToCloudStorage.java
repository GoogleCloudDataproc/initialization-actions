package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.cloud.hadoop.util.LogUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * An abstract base class for BigQuery exports to GCS for use by MapReduce job setup
 */
public abstract class AbstractExportToCloudStorage implements Export {

  public static final String DESTINATION_FORMAT_KEY = "destinationFormat";

  private static final LogUtil log = new LogUtil(AbstractExportToCloudStorage.class);

  protected final Configuration configuration;
  protected final String gcsPath;
  protected final ExportFileFormat fileFormat;
  protected final Bigquery bigqueryClient;
  protected final String projectId;
  protected final TableReference tableToExport;
  protected JobReference exportJobReference;

  public AbstractExportToCloudStorage(
      Configuration configuration,
      String gcsPath,
      ExportFileFormat fileFormat,
      Bigquery bigQueryClient,
      String projectId,
      TableReference tableToExport) {
    this.configuration = configuration;
    this.gcsPath = gcsPath;
    this.fileFormat = fileFormat;
    this.bigqueryClient = bigQueryClient;
    this.projectId = projectId;
    this.tableToExport = tableToExport;
  }

  @Override
  public void prepare() throws IOException {
    log.debug("Preparing export path %s", gcsPath);
    Path hadoopPath = new Path(gcsPath);
    FileSystem fs = hadoopPath.getFileSystem(configuration);
    if (fs.exists(hadoopPath)) {
      throw new IOException(
          String.format(
              "Conflict occurred creating export directory. Path %s already exists", gcsPath));
    }
    // Now create the directory.
    fs.mkdirs(hadoopPath);
  }

  @Override
  public void beginExport() throws IOException {
    // Create job and configuration.
    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationExtract extractConfig = new JobConfigurationExtract();

    // Set source.
    extractConfig.setSourceTable(tableToExport);

    // Set destination.
    extractConfig.setDestinationUris(getExportPaths());
    extractConfig.set(DESTINATION_FORMAT_KEY, fileFormat.getFormatIdentifier());
    config.setExtract(extractConfig);
    job.setConfiguration(config);

    // Insert and run job.
    Bigquery.Jobs.Insert insert = null;
    try {
      insert = bigqueryClient.jobs().insert(projectId, job);
      exportJobReference = insert.execute().getJobReference();
    } catch (IOException e) {
      String error = String.format(
          "Error while exporting table %s", BigQueryStrings.toString(tableToExport));
      log.error(error, e);
      throw new IOException(error, e);
    }
  }

  @Override
  public void cleanupExport() throws IOException {
    // Delete temporary GCS directory.
    if (configuration.getBoolean(
        BigQueryConfiguration.DELETE_EXPORT_FILES_FROM_GCS_KEY,
        BigQueryConfiguration.DELETE_EXPORT_FILES_FROM_GCS_DEFAULT)) {
      Path tempPath = new Path(
          ConfigurationUtil.getMandatoryConfig(
              configuration, BigQueryConfiguration.TEMP_GCS_PATH_KEY));
      try {
        FileSystem fs = tempPath.getFileSystem(configuration);
        if (fs.exists(tempPath)) {
          log.info("Deleting temp GCS input path '%s'", tempPath);
          fs.delete(tempPath, true);
        }
      } catch (IOException e) {
        // Error is swallowed as job has completed successfully and the only failure is deleting
        // temporary data.
        // This matches the FileOutputCommitter pattern.
        log.warn("Could not delete intermediate GCS files. Temporary data not cleaned up.", e);
      }
    }
  }
}
