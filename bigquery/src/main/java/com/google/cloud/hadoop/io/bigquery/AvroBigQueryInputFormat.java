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

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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
