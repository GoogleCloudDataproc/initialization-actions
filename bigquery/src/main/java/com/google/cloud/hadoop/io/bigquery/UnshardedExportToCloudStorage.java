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

import com.google.api.services.bigquery.model.Table;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * An Export to GCS that provides a single directory for BigQuery to export to and requires
 * all content to be written (the export complete) before we begin execution of the MapReduce.
 */
public class UnshardedExportToCloudStorage extends AbstractExportToCloudStorage {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private InputFormat<LongWritable, Text> delegateInputFormat;

  public UnshardedExportToCloudStorage(
      Configuration configuration,
      String gcsPath,
      ExportFileFormat fileFormat,
      BigQueryHelper bigQueryHelper,
      String projectId,
      Table tableToExport,
      @Nullable InputFormat<LongWritable, Text> delegateInputFormat) {
    super(configuration, gcsPath, fileFormat, bigQueryHelper, projectId, tableToExport);
    if (delegateInputFormat == null) {
      this.delegateInputFormat = new TextInputFormat();
    } else {
      this.delegateInputFormat = delegateInputFormat;
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    logger.atInfo().log("Setting FileInputFormat's inputPath to '%s'", gcsPath);
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
    logger.atFine().log("Using unsharded splits");
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
