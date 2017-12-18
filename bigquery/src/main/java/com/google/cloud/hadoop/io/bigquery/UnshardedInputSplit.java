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


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A class to tag BigQuery input splits
 */
// TODO(user): Consider removing this class.
public class UnshardedInputSplit extends FileSplit {

  // Used for Serialization
  public UnshardedInputSplit() {
    this(null, 0, 0, null);
  }

  public UnshardedInputSplit(Path path, long start, long length, String[] hosts) {
    super(path, start, length, hosts);
  }
}
