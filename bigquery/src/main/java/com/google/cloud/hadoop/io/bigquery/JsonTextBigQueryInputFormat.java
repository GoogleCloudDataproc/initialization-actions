/*
 * Copyright 2017 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.io.bigquery;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
