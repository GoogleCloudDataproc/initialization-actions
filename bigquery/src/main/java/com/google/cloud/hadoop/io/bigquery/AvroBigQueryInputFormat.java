package com.google.cloud.hadoop.io.bigquery;

import com.google.common.base.Preconditions;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * InputFormat to generate and ingest Avro-based BigQuery exports.
 */
public class AvroBigQueryInputFormat
    extends AbstractBigQueryInputFormat<LongWritable, GenericData.Record> {
  @Override
  public RecordReader<LongWritable, GenericData.Record> createDelegateRecordReader(
      InputSplit split, Configuration configuration) throws IOException, InterruptedException {
    Preconditions.checkState(
        split instanceof FileSplit, "AvroBigQueryInputFormat requires FileSplit input splits");
    return new AvroRecordReader();
  }

  @Override
  public ExportFileFormat getExportFileFormat() {
    return ExportFileFormat.AVRO;
  }
}
