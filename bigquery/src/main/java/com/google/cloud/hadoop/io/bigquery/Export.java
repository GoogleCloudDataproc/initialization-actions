package com.google.cloud.hadoop.io.bigquery;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

/**
 * A single BigQuery export for the purpose of running a Hadoop MapReduce.
 */
public interface Export {

  /**
   * Create any temporary directories, tables, etc
   */
  void prepare() throws IOException;

  /**
   * Start exporting data
   */
  void beginExport() throws IOException;

  /**
   * Wait for enough data to be available for us to start a MapReduce. This may be all data
   * or no data.
   */
  void waitForUsableMapReduceInput() throws IOException, InterruptedException;

  /**
   * Get input splits that should be passed to Hadoop.
   */
  List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException;

  /**
   * Get a list of export paths to provide to BigQuery
   */
  List<String> getExportPaths() throws IOException;

  /**
   * Delete any temp tables or temporary data locations.
   */
  void cleanupExport() throws IOException;
}
