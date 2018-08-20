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

import com.google.common.flogger.GoogleLogger;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * ShardedInputSplit implements Hadoop InputSplit by storing path information for a
 * logical "shard" corresponding to a single Hadoop task. This shard will own a single subdirectory
 * of GCS so read in dynamic ordering as a BigQuery export proceeds simultaneously.
 */
public class ShardedInputSplit
    extends InputSplit implements Writable {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Directory/file-pattern whose files will be read by the reader created from this split.
  // The file-pattern portion is the glob expression of the file basename, e.g. data-*.json.
  private Path shardDirectoryAndPattern;

  // Estimated number of records in this particular split.
  private long estimatedNumRecords;

  /**
   * Default constructor for dynamic-classloading deserialization.
   */
  public ShardedInputSplit() {
  }

  /**
   * For creation of splits in JobClient.
   */
  public ShardedInputSplit(Path shardDirectoryAndPattern, long estimatedNumRecords) {
    this.shardDirectoryAndPattern = shardDirectoryAndPattern;
    this.estimatedNumRecords = estimatedNumRecords;
  }

  /**
   * Accessor for shardDirectoryAndPattern.
   */
  public Path getShardDirectoryAndPattern() {
    return shardDirectoryAndPattern;
  }

  /**
   * Estimated number of records to read, *not* the number of bytes.
   */
  @Override
  public long getLength() {
    return estimatedNumRecords;
  }

  @Override
  public String[] getLocations()
      throws IOException {
    return new String[0];
  }

  @Override
  public String toString() {
    return String.format("%s[%d estimated records]", shardDirectoryAndPattern, estimatedNumRecords);
  }

  @Override
  public void write(DataOutput out)
      throws IOException {
    Text.writeString(out, shardDirectoryAndPattern.toString());
    out.writeLong(estimatedNumRecords);
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    shardDirectoryAndPattern = new Path(Text.readString(in));
    estimatedNumRecords = in.readLong();
  }
}
