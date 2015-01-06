package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.util.LogUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A BigQueryExport that makes use of BigQuery's multiple export path feature and
 * allows us to begin MapReducing using the DynamicFileListingRecord RecordReader
 * as soon as the export begins.
 */
public class ShardedExportToCloudStorage extends AbstractExportToCloudStorage {
  private static final LogUtil log = new LogUtil(ShardedExportToCloudStorage.class);

  // Configuration key for 'hint' of number of desired map tasks.
  public static final String NUM_MAP_TASKS_HINT_KEY = "mapred.map.tasks";

  // Default desired num map tasks.
  public static final int NUM_MAP_TASKS_HINT_DEFAULT = 2;

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

  protected final Table tableMetadata;

  public ShardedExportToCloudStorage(Configuration configuration, String gcsPath,
      ExportFileFormat fileFormat, Bigquery bigqueryClient, String projectId,
      TableReference tableToExport) throws IOException {
    super(configuration, gcsPath, fileFormat, bigqueryClient, projectId, tableToExport);
    // Fetch some metadata about the table we plan to export.
    tableMetadata = bigqueryClient.tables().get(
        tableToExport.getProjectId(),
        tableToExport.getDatasetId(),
        tableToExport.getTableId()).execute();
  }

  @Override
  public void waitForUsableMapReduceInput() throws IOException, InterruptedException {
    log.debug("Using sharded input. waitForUsableMapReduceInput is a no-op.");
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    long numTableRows = tableMetadata.getNumRows().longValue();
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

    long numTableRows = tableMetadata.getNumRows().longValue();
    long numTableBytes = tableMetadata.getNumBytes();

    int numShards = computeNumShards(numTableBytes);
    log.info("Computed '%d' shards for sharded BigQuery export.", numShards);
    for (int i = 0; i < numShards; ++i) {
      String exportPattern = String.format(
          "%s/shard-%d/%s", gcsPath, i, fileFormat.getFilePattern());
      paths.add(exportPattern);
    }

    log.info("Table '%s' to be exported has %d rows and %d bytes",
        BigQueryStrings.toString(tableToExport), numTableRows, numTableBytes);

    return paths;
  }

  /**
   * Helper to use a mixture of Hadoop settings and Bigquery table properties to determine the
   * number of shards to use in a sharded export.
   */
  private int computeNumShards(long numTableBytes) {
    int desiredNumMaps = configuration.getInt(NUM_MAP_TASKS_HINT_KEY, NUM_MAP_TASKS_HINT_DEFAULT);
    int estimatedNumFiles = (int) Math.min(
        Math.max(MIN_SHARDS_FOR_SHARDED_EXPORT, numTableBytes / APPROXIMATE_EXPORT_FILE_SIZE),
        APPROXIMATE_MAX_EXPORT_FILES);

    if (estimatedNumFiles < desiredNumMaps) {
      log.warn("Estimated num files < desired num maps (%d < %d); clipping to %d.",
          estimatedNumFiles, desiredNumMaps, estimatedNumFiles);
      // TODO(user): Add config settings for whether to clip or not.
      return estimatedNumFiles;
    } else {
      return desiredNumMaps;
    }
  }
}
