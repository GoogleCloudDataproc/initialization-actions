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
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * RecordReader for avro BigQuery exports.
 */
public class AvroRecordReader extends RecordReader<LongWritable, GenericData.Record> {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  final LongWritable currentKey = new LongWritable();
  FileReader<GenericData.Record> dataFileReader;
  Schema schema;
  GenericData.Record currentRecord;
  long inputFileLength;
  long splitStart;
  long splitLength;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    initializeInternal(inputSplit, taskAttemptContext.getConfiguration());
  }

  protected void initializeInternal(InputSplit inputSplit, Configuration conf)
      throws IOException, InterruptedException {
    Preconditions.checkState(
        inputSplit instanceof FileSplit, "AvroRecordReader requires FileSplit input splits.");
    FileSplit fileSplit = (FileSplit) inputSplit;
    splitStart = fileSplit.getStart();
    splitLength = fileSplit.getLength();

    Path filePath = fileSplit.getPath();
    FileSystem fs = filePath.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(filePath);
    inputFileLength = status.getLen();

    final FSDataInputStream stream = fs.open(filePath);
    dataFileReader = DataFileReader.openReader(
        new SeekableInput() {
          @Override
          public void seek(long offset) throws IOException {
            stream.seek(offset);
          }

          @Override
          public long tell() throws IOException {
            return stream.getPos();
          }
          @Override
          public long length() throws IOException {
            return inputFileLength;
          }

          @Override
          public int read(byte[] bytes, int offset, int length) throws IOException {
            return stream.read(bytes, offset, length);
          }

          @Override
          public void close() throws IOException {
            stream.close();
          }
        }, new GenericDatumReader<GenericData.Record>());
    // Sync to the first sync point after the start of the split:
    dataFileReader.sync(fileSplit.getStart());
    schema = dataFileReader.getSchema();
    currentRecord = new GenericData.Record(schema);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    Preconditions.checkState(currentRecord != null);
    // Stop reading as soon as we hit a sync point out of our split:
    if (dataFileReader.hasNext() && !dataFileReader.pastSync(splitStart + splitLength)) {
      currentKey.set(currentKey.get() + 1);
      dataFileReader.next(currentRecord);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return currentKey;
  }

  @Override
  public GenericData.Record getCurrentValue() throws IOException, InterruptedException {
    return currentRecord;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    Preconditions.checkState(dataFileReader != null);
    if (splitLength == 0) {
      return 1.0f;
    }
    float splitRelativeLocation = dataFileReader.tell() - splitStart;
    return splitRelativeLocation / splitLength;
  }

  @Override
  public void close() throws IOException {
    Preconditions.checkState(dataFileReader != null);
    dataFileReader.close();
  }
}
