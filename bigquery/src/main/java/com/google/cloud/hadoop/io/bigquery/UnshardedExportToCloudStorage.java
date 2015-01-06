package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.List;

/**
 * An Export to GCS that provides a single directory for BigQuery to export to and requires
 * all content to be written (the export complete) before we begin execution of the MapReduce.
 */
public class UnshardedExportToCloudStorage extends AbstractExportToCloudStorage {
  private static final LogUtil log = new LogUtil(UnshardedExportToCloudStorage.class);
  private final InputFormat<LongWritable, Text> delegateInputFormat;

  public UnshardedExportToCloudStorage(
      Configuration configuration,
      String gcsPath,
      ExportFileFormat fileFormat,
      Bigquery bigqueryClient,
      String projectId,
      TableReference tableToExport) {
    this(configuration, gcsPath, fileFormat, bigqueryClient, projectId, tableToExport,
        new TextInputFormat());
  }

  public UnshardedExportToCloudStorage(
      Configuration configuration,
      String gcsPath,
      ExportFileFormat fileFormat,
      Bigquery bigqueryClient,
      String projectId,
      TableReference tableToExport,
      InputFormat delegateInputFormat) {
    super(configuration, gcsPath, fileFormat, bigqueryClient, projectId, tableToExport);
    this.delegateInputFormat = delegateInputFormat;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    log.info("Setting FileInputFormat's inputPath to '%s'", gcsPath);
    configuration.set("mapred.input.dir", gcsPath);

    // Now that the FileInputFormat's path is pointed to the export directory, construct splits
    // using a TextInputFormat instance.
    return delegateInputFormat.getSplits(context);
  }

  @Override
  public List<String> getExportPaths() throws IOException {
    log.debug("Using unsharded splits");
    String exportPattern = gcsPath + "/" + fileFormat.getFilePattern();
    return ImmutableList.of(exportPattern);
  }

  @Override
  public void waitForUsableMapReduceInput() throws IOException, InterruptedException {
      Preconditions.checkState(
          exportJobReference != null,
          "beginExport() must be called before waitForUsableMapReduceInput()");

      BigQueryUtils.waitForJobCompletion(
          bigqueryClient,
          projectId,
          exportJobReference,
          new Progressable() {
            @Override
            public void progress() {
            }
          });
    }

}
