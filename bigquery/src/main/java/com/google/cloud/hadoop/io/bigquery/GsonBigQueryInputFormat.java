package com.google.cloud.hadoop.io.bigquery;

import com.google.cloud.hadoop.util.LogUtil;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;

/**
 * GsonBigQueryInputFormat provides access to BigQuery tables via exports to GCS in the form of
 * gson JsonObjects as mapper values.
 */
public class GsonBigQueryInputFormat
    extends AbstractBigQueryInputFormat<LongWritable, JsonObject> {
  protected static final LogUtil log = new LogUtil(GsonBigQueryInputFormat.class);

  @Override
  public RecordReader<LongWritable, JsonObject> createDelegateRecordReader(
      InputSplit split, Configuration configuration) throws IOException, InterruptedException {
    log.debug("createDelegateRecordReader -> new GsonRecordReader");
    return new GsonRecordReader();
  }

  @Override
  public ExportFileFormat getExportFileFormat() {
    return ExportFileFormat.LINE_DELIMITED_JSON;
  }
}
