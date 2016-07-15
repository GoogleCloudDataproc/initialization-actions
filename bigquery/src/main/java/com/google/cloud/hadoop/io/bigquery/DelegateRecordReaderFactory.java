package com.google.cloud.hadoop.io.bigquery;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * Interface to produce delegate RecordReader instances.
 */
public interface DelegateRecordReaderFactory<K, V> {

  /**
   * Create a new record reader for a single input split.
   */
  public RecordReader<K, V> createDelegateRecordReader(
      InputSplit split, Configuration configuration) throws IOException, InterruptedException;
}
