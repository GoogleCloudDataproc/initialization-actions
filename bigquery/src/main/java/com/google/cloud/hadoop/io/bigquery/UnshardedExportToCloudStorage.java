package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.model.TableReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An Export to GCS that provides a single directory for BigQuery to export to and requires
 * all content to be written (the export complete) before we begin execution of the MapReduce.
 */
public class UnshardedExportToCloudStorage extends AbstractExportToCloudStorage {
  private static final Logger LOG = LoggerFactory.getLogger(UnshardedExportToCloudStorage.class);
  private final InputFormat<LongWritable, Text> delegateInputFormat;

  public UnshardedExportToCloudStorage(
      Configuration configuration,
      String gcsPath,
      ExportFileFormat fileFormat,
      BigQueryHelper bigQueryHelper,
      String projectId,
      TableReference tableToExport) {
    this(configuration, gcsPath, fileFormat, bigQueryHelper, projectId, tableToExport,
        new TextInputFormat());
  }

  public UnshardedExportToCloudStorage(
      Configuration configuration,
      String gcsPath,
      ExportFileFormat fileFormat,
      BigQueryHelper bigQueryHelper,
      String projectId,
      TableReference tableToExport,
      InputFormat delegateInputFormat) {
    super(configuration, gcsPath, fileFormat, bigQueryHelper, projectId, tableToExport);
    this.delegateInputFormat = delegateInputFormat;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    LOG.info("Setting FileInputFormat's inputPath to '{}'", gcsPath);
    configuration.set("mapred.input.dir", gcsPath);

    // Now that the FileInputFormat's path is pointed to the export directory, construct splits
    // using a TextInputFormat instance.
    List<InputSplit> splits = new ArrayList<>();
    for (InputSplit split : delegateInputFormat.getSplits(context)) {
      FileSplit fileSplit = (FileSplit) split;
      splits.add(
          new UnshardedInputSplit(
              fileSplit.getPath(),
              fileSplit.getStart(),
              fileSplit.getLength(),
              fileSplit.getLocations()));
    }
    return splits;
  }

  @Override
  public List<String> getExportPaths() throws IOException {
    LOG.debug("Using unsharded splits");
    String exportPattern = gcsPath + "/" + fileFormat.getFilePattern();
    return ImmutableList.of(exportPattern);
  }

  @Override
  public void waitForUsableMapReduceInput() throws IOException, InterruptedException {
      Preconditions.checkState(
          exportJobReference != null,
          "beginExport() must be called before waitForUsableMapReduceInput()");

      BigQueryUtils.waitForJobCompletion(
          bigQueryHelper.getRawBigquery(),
          projectId,
          exportJobReference,
          new Progressable() {
            @Override
            public void progress() {
            }
          });
    }

}
