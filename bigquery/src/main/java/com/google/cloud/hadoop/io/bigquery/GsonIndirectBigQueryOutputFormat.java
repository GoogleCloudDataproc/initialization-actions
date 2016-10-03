package com.google.cloud.hadoop.io.bigquery;

import com.google.gson.JsonObject;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Experimental, API subject to change.<br>
 * OutputFormat that writes new line delimited JSON into plain text files. This is processed as
 * LINE_DELIMITED_JSON when being imported into BigQuery.
 */
@InterfaceStability.Unstable
public class GsonIndirectBigQueryOutputFormat<K extends NullWritable, V extends JsonObject>
    extends AbstractIndirectBigQueryOutputFormat<TextOutputFormat<K, V>, K, V> {

  public GsonIndirectBigQueryOutputFormat() {
    super(new TextOutputFormat<K, V>());
  }

  @Override
  public BigQueryFileFormat getSourceFormat() {
    return BigQueryFileFormat.LINE_DELIMITED_JSON;
  }
}
