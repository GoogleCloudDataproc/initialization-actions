package com.google.cloud.hadoop.io.bigquery;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * OutputFormat that writes new line delimited CSV into plain text files. This is processed as CSV
 * when being imported into BigQuery.
 */
public class CsvIndirectBigQueryOutputFormat<K extends NullWritable, V extends Text>
    extends AbstractIndirectBigQueryOutputFormat<TextOutputFormat<K, V>, K, V> {

  public CsvIndirectBigQueryOutputFormat() {
    super(new TextOutputFormat<K, V>());
  }

  @Override
  public BigQueryFileFormat getSourceFormat() {
    return BigQueryFileFormat.CSV;
  }
}
