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

import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.DELETE_EXPORT_FILES_FROM_GCS;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.TEMP_GCS_PATH;
import static com.google.cloud.hadoop.util.ConfigurationUtil.getMandatoryConfig;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * An abstract base class for BigQuery exports to GCS for use by MapReduce job setup
 */
public abstract class AbstractExportToCloudStorage implements Export {

  public static final String DESTINATION_FORMAT_KEY = "destinationFormat";

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  protected final Configuration configuration;
  protected final String gcsPath;
  protected final ExportFileFormat fileFormat;
  protected final BigQueryHelper bigQueryHelper;
  protected final String projectId;
  protected final Table tableToExport;
  protected JobReference exportJobReference;

  public AbstractExportToCloudStorage(
      Configuration configuration,
      String gcsPath,
      ExportFileFormat fileFormat,
      BigQueryHelper bigQueryHelper,
      String projectId,
      Table tableToExport) {
    this.configuration = configuration;
    this.gcsPath = gcsPath;
    this.fileFormat = fileFormat;
    this.bigQueryHelper = bigQueryHelper;
    this.projectId = projectId;
    this.tableToExport = tableToExport;
  }

  @Override
  public void prepare() throws IOException {
    logger.atFine().log("Preparing export path %s", gcsPath);
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
    JobConfigurationExtract extractConfig = new JobConfigurationExtract();

    // Set source.
    extractConfig.setSourceTable(tableToExport.getTableReference());

    // Set destination.
    extractConfig.setDestinationUris(getExportPaths());
    extractConfig.set(DESTINATION_FORMAT_KEY, fileFormat.getFormatIdentifier());

    JobConfiguration config = new JobConfiguration();
    config.setExtract(extractConfig);

    JobReference jobReference =
        bigQueryHelper.createJobReference(
            projectId, "exporttocloudstorage", tableToExport.getLocation());

    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Insert and run job.
    try {
      Job response = bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);
      logger.atFine().log("Got response '%s'", response);
      exportJobReference = response.getJobReference();
    } catch (IOException e) {
      String error = String.format(
          "Error while exporting table %s",
          BigQueryStrings.toString(tableToExport.getTableReference()));
      throw new IOException(error, e);
    }
  }

  @Override
  public void cleanupExport() throws IOException {
    // Delete temporary GCS directory.
    if (DELETE_EXPORT_FILES_FROM_GCS.get(configuration, configuration::getBoolean)) {
      Path tempPath = new Path(getMandatoryConfig(configuration, TEMP_GCS_PATH));
      try {
        FileSystem fs = tempPath.getFileSystem(configuration);
        if (fs.exists(tempPath)) {
          logger.atInfo().log("Deleting temp GCS input path '%s'", tempPath);
          fs.delete(tempPath, true);
        }
      } catch (IOException e) {
        // Error is swallowed as job has completed successfully and the only failure is deleting
        // temporary data.
        // This matches the FileOutputCommitter pattern.
        logger.atWarning().withCause(e).log(
            "Could not delete intermediate GCS files. Temporary data not cleaned up.");
      }
    }
  }
}
