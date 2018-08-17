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
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * A BigQueryExport that makes use of BigQuery's multiple export path feature and
 * allows us to begin MapReducing using the DynamicFileListingRecord RecordReader
 * as soon as the export begins.
 */
public class ShardedExportToCloudStorage extends AbstractExportToCloudStorage {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Configuration key for 'hint' of number of desired map tasks.
  public static final String NUM_MAP_TASKS_HINT_KEY = "mapred.map.tasks";

  // Default desired num map tasks.
  public static final int NUM_MAP_TASKS_HINT_DEFAULT = 2;

  // Configuration key for service-specified maxium number of export shards (not necessarily
  // equal to the maximum number of export files).
  public static final String MAX_EXPORT_SHARDS_KEY = "mapred.bq.input.sharded.export.shards.max";

  // Default maximum number of export shards.
  public static final int MAX_EXPORT_SHARDS_DEFAULT = 500;

  // Estimated size of export files, used to estimate the total number of files that will be
  // exported.
  public static final long APPROXIMATE_EXPORT_FILE_SIZE = 256L * 1024 * 1024;

  // Approximate maximum number of export files the backend is willing to create; larger files
  // will be generated if the total amount of data otherwise causes the number of files to
  // exceed this limit.
  public static final int APPROXIMATE_MAX_EXPORT_FILES = 10000;

  // If we try 'sharded export' with only one shard, then BigQuery doesn't recognize it as a
  // sharded export and thus doesn't generate an end-marker file. Even if BigQuery will only
  // export one file, as long as we specify two shards, BigQuery will create both end-marker files
  // while only populating actual data into one of the two shards.
  public static final int MIN_SHARDS_FOR_SHARDED_EXPORT = 2;

  public ShardedExportToCloudStorage(
      Configuration configuration,
      String gcsPath,
      ExportFileFormat fileFormat,
      BigQueryHelper bigQueryHelper,
      String projectId,
      Table tableToExport) throws IOException {
    super(configuration, gcsPath, fileFormat, bigQueryHelper, projectId, tableToExport);
  }

  @Override
  public void waitForUsableMapReduceInput() throws IOException, InterruptedException {
    logger.atFine().log("Using sharded input. waitForUsableMapReduceInput is a no-op.");
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    long numTableRows = tableToExport.getNumRows().longValue();
    List<String> paths = getExportPaths();
    int pathCount = paths.size();

    List<InputSplit> splits = new ArrayList<>();
    for (String exportPattern : paths) {
      splits.add(
          new ShardedInputSplit(
              new Path(exportPattern), Math.max(1, numTableRows / pathCount)));
    }
    return splits;
  }

  @Override
  public List<String> getExportPaths() throws IOException {
    List<String> paths = new ArrayList<>();

    long numTableRows = tableToExport.getNumRows().longValue();
    long numTableBytes = tableToExport.getNumBytes();

    int numShards = computeNumShards(numTableBytes);
    logger.atInfo().log("Computed '%s' shards for sharded BigQuery export.", numShards);
    for (int i = 0; i < numShards; ++i) {
      String exportPattern = String.format(
          "%s/shard-%d/%s", gcsPath, i, fileFormat.getFilePattern());
      paths.add(exportPattern);
    }

    logger.atInfo().log(
        "Table '%s' to be exported has %s rows and %s bytes",
        BigQueryStrings.toString(tableToExport.getTableReference()), numTableRows, numTableBytes);

    return paths;
  }

  /**
   * Helper to use a mixture of Hadoop settings and Bigquery table properties to determine the
   * number of shards to use in a sharded export.
   */
  private int computeNumShards(long numTableBytes) {
    int desiredNumMaps = configuration.getInt(NUM_MAP_TASKS_HINT_KEY, NUM_MAP_TASKS_HINT_DEFAULT);
    logger.atFine().log(
        "Fetched desiredNumMaps from '%s': %s", NUM_MAP_TASKS_HINT_KEY, desiredNumMaps);

    int estimatedNumFiles =
        (int) Math.min(numTableBytes / APPROXIMATE_EXPORT_FILE_SIZE, APPROXIMATE_MAX_EXPORT_FILES);
    logger.atFine().log("estimatedNumFiles: %s", estimatedNumFiles);

    // Maximum number of shards is either equal to the number of files (such that each shard has
    // exactly one file) or the service-enforced maximum.
    int serviceMaxShards = configuration.getInt(MAX_EXPORT_SHARDS_KEY, MAX_EXPORT_SHARDS_DEFAULT);
    logger.atFine().log(
        "Fetched serviceMaxShards from '%s': %s", MAX_EXPORT_SHARDS_KEY, serviceMaxShards);

    int numShards = Math.min(estimatedNumFiles, serviceMaxShards);
    if (numShards < desiredNumMaps) {
      logger.atWarning().log(
          "Estimated number of shards < desired num maps (%s < %s); clipping to %s.",
          numShards, desiredNumMaps, numShards);
      // TODO(user): Add config settings for whether to clip or not.
    } else {
      numShards = desiredNumMaps;
    }
    return Math.max(numShards, MIN_SHARDS_FOR_SHARDED_EXPORT);
  }
}
