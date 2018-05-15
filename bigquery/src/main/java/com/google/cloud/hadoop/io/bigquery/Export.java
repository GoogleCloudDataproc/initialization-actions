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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

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
