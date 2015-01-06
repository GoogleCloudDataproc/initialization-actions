package com.google.cloud.hadoop.io.bigquery;

import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.base.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * RecordReader for avro BigQuery exports.
 */
public class AvroRecordReader extends RecordReader<LongWritable, GenericData.Record> {
  protected static final LogUtil LOG = new LogUtil(AvroRecordReader.class);

  final LongWritable currentKey = new LongWritable();
  FileReader<GenericData.Record> dataFileReader;
  Schema schema;
  GenericData.Record currentRecord;
  long inputFileLength;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Preconditions.checkState(
        inputSplit instanceof FileSplit, "AvroRecordReader requires FileSplit input splits.");

    FileSplit fileSplit = (FileSplit) inputSplit;
    Path filePath = fileSplit.getPath();
    FileSystem fs = filePath.getFileSystem(taskAttemptContext.getConfiguration());
    FileStatus status = fs.getFileStatus(filePath);
    final FSDataInputStream stream = fs.open(filePath);
    inputFileLength = status.getLen();
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

    schema = dataFileReader.getSchema();
    currentRecord = new GenericData.Record(schema);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    Preconditions.checkState(currentRecord != null);
    if (dataFileReader.hasNext()) {
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
    return inputFileLength / (float) dataFileReader.tell();
  }

  @Override
  public void close() throws IOException {
    Preconditions.checkState(dataFileReader != null);
    dataFileReader.close();
  }
}
