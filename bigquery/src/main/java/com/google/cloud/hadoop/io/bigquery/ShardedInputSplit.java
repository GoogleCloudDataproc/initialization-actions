package com.google.cloud.hadoop.io.bigquery;

import com.google.cloud.hadoop.util.LogUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * ShardedInputSplit implements Hadoop InputSplit by storing path information for a
 * logical "shard" corresponding to a single Hadoop task. This shard will own a single subdirectory
 * of GCS so read in dynamic ordering as a BigQuery export proceeds simultaneously.
 */
public class ShardedInputSplit
    extends InputSplit implements Writable {
  // Logger.
  protected static final LogUtil log = new LogUtil(ShardedInputSplit.class);

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
