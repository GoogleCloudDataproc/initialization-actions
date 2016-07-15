package com.google.cloud.hadoop.io.bigquery;

import com.google.gson.JsonObject;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GsonBigQueryInputFormat provides access to BigQuery tables via exports to GCS in the form of
 * gson JsonObjects as mapper values.
 */
public class GsonBigQueryInputFormat
    extends AbstractBigQueryInputFormat<LongWritable, JsonObject> {
  protected static final Logger LOG = LoggerFactory.getLogger(GsonBigQueryInputFormat.class);

  @Override
  public RecordReader<LongWritable, JsonObject> createDelegateRecordReader(
      InputSplit split, Configuration configuration) throws IOException, InterruptedException {
    LOG.debug("createDelegateRecordReader -> new GsonRecordReader");
    return new GsonRecordReader();
  }

  @Override
  public ExportFileFormat getExportFileFormat() {
    return ExportFileFormat.LINE_DELIMITED_JSON;
  }
}
