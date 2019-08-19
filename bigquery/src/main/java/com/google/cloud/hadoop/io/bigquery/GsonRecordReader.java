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

import com.google.cloud.hadoop.util.HadoopToStringUtil;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * The GsonRecordReader reads records from GCS through GHFS. It takes newline-delimited Json files
 * in GCS and reads them through LineRecordReader. It parses each line in the file split into
 * key/value pairs with the line number as the key and the jsonObject represented by the line as the
 * value. These pairs are passed as input to the Mapper.
 */
public class GsonRecordReader extends RecordReader<LongWritable, JsonObject> {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // A LineRecordReader which handles most calls. The GsonRecordReader just provides a wrapper which
  // translates the results of LineRecordReader into Json objects.
  private LineRecordReader lineReader;

  // Current key.
  private LongWritable currentKey = new LongWritable(0L);

  // Current value.
  private JsonObject currentValue;

  // Total key, value pairs read.
  private int count;

  // Used to parse the JsonObject from the LineRecordReader output.
  private JsonParser jsonParser;

  /**
   * Called once at initialization to initialize the RecordReader.
   *
   * @param genericSplit the split that defines the range of records to read.
   * @param context the information about the task.
   * @throws IOException on IO Error.
   */
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (logger.atFine().isEnabled()) {
        logger.atFine().log(
            "initialize('%s', '%s')",
            HadoopToStringUtil.toString(genericSplit), HadoopToStringUtil.toString(context));
    }
    Preconditions.checkArgument(genericSplit instanceof FileSplit,
        "InputSplit genericSplit should be an instance of FileSplit.");
    // Get FileSplit.
    FileSplit fileSplit = (FileSplit) genericSplit;
    // Create the JsonParser.
    jsonParser = new JsonParser();
    // Initialize the LineRecordReader.
    lineReader = new LineRecordReader();
    lineReader.initialize(fileSplit, context);
  }

  /**
   * Reads the next key, value pair. Gets next line and parses Json object.
   *
   * @return true if a key/value pair was read.
   * @throws IOException on IO Error.
   */
  @Override
  public boolean nextKeyValue()
      throws IOException {
    // If there is no next value, return false. Set current key and value to null.
    // Different Hadoop recordreaders have different behavior for calling current key and value
    // after nextKeyValue returns false.
    if (!lineReader.nextKeyValue()) {
      logger.atFine().log("All values read: record reader read %s key, value pairs.", count);
      return false;
    }
    // Get the next line.
    currentKey.set(lineReader.getCurrentKey().get());
    Text lineValue = lineReader.getCurrentValue();
    currentValue = jsonParser.parse(lineValue.toString()).getAsJsonObject();
    // Increment count of key, value pairs.
    count++;
    return true;
  }

  /**
   * Gets the current key.
   *
   * @return the current key or null if there is no current key.
   */
  @Override
  public LongWritable getCurrentKey() {
    return currentKey;
  }

  /**
   * Gets the current value.
   *
   * @return the current value or null if there is no current value.
   */
  @Override
  public JsonObject getCurrentValue() {
    return currentValue;
  }

  /**
   * Returns the current progress of the record reader through its data.
   *
   * @return a number between 0.0 and 1.0 that is the fraction of the data read.
   * @throws IOException on IO Error.
   */
  @Override
  public float getProgress()
      throws IOException {
    return lineReader.getProgress();
  }

  /**
   * Closes the record reader.
   *
   * @throws IOException on IO Error.
   */
  @Override
  public void close()
      throws IOException {
    lineReader.close();
  }
}
