/**
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

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract base class for BigQuery exports to GCS for use by MapReduce job setup
 */
public abstract class AbstractExportToCloudStorage implements Export {

  public static final String DESTINATION_FORMAT_KEY = "destinationFormat";

  private static final Logger LOG = LoggerFactory.getLogger(AbstractExportToCloudStorage.class);

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
    LOG.debug("Preparing export path {}", gcsPath);
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
      LOG.debug("Got response '{}'", response);
      exportJobReference = response.getJobReference();
    } catch (IOException e) {
      String error = String.format(
          "Error while exporting table %s",
          BigQueryStrings.toString(tableToExport.getTableReference()));
      LOG.error(error, e);
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
          LOG.info("Deleting temp GCS input path '{}'", tempPath);
          fs.delete(tempPath, true);
        }
      } catch (IOException e) {
        // Error is swallowed as job has completed successfully and the only failure is deleting
        // temporary data.
        // This matches the FileOutputCommitter pattern.
        LOG.warn("Could not delete intermediate GCS files. Temporary data not cleaned up.", e);
      }
    }
  }
}
