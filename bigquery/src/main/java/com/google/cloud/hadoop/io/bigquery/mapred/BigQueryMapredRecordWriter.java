package com.google.cloud.hadoop.io.bigquery.mapred;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wrap our mapreduce RecordWriter so it can be called
 * from streaming hadoop.
 */
public class BigQueryMapredRecordWriter<K, V> implements RecordWriter<K, V> {

  protected static final Logger LOG =
      LoggerFactory.getLogger(BigQueryMapredRecordWriter.class);

  private org.apache.hadoop.mapreduce.RecordWriter<K, JsonObject>
      mapreduceRecordWriter;
  private TaskAttemptContext context;
  private JsonParser jsonParser = new JsonParser();
  private int writeCount = 0;

  /**
   * @param mapreduceRecordWriter A mapreduce-based RecordWriter.
   */
  public BigQueryMapredRecordWriter(
      org.apache.hadoop.mapreduce.RecordWriter<K, JsonObject>
      mapreduceRecordWriter, TaskAttemptContext context) {
    this.mapreduceRecordWriter = mapreduceRecordWriter;
    this.context = context;
    LOG.debug("BigQueryMapredRecordWriter created");
  }

  public void close(Reporter reporter) throws IOException {
    LOG.debug("close");
    try {
      mapreduceRecordWriter.close(context);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  public void write(K key, V value) throws IOException {
    if (writeCount < 5) {
      // TODO(user): perhaps figure out how to make a reusable log_first_n
      LOG.debug("convertToJson from type {}",
            (value == null) ? "null" : value.getClass().getName());
      writeCount++;
    }
    try {
      JsonObject jsonValue = convertToJson(value);
      mapreduceRecordWriter.write(key, jsonValue);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }

  private JsonObject convertToJson(V value) {
    if (value == null) {
      return null;
    }
    String s = value.toString();
    return jsonParser.parse(s).getAsJsonObject();
  }
}
