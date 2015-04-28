package com.google.cloud.hadoop.io.bigquery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An input format that provides BigQuery JSON as lines of text as they are written to the export
 * by BigQuery.
 */
public class JsonTextBigQueryInputFormat extends AbstractBigQueryInputFormat<LongWritable, Text> {
  protected static final Logger LOG = LoggerFactory.getLogger(JsonTextBigQueryInputFormat.class);

  @Override
  public RecordReader<LongWritable, Text> createDelegateRecordReader(
      InputSplit split, Configuration configuration) throws IOException, InterruptedException {
    LOG.debug("createDelegateRecordReader -> new LineRecordReader");
    return new LineRecordReader();
  }

  @Override
  public ExportFileFormat getExportFileFormat() {
    return ExportFileFormat.LINE_DELIMITED_JSON;
  }
}
