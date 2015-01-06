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
