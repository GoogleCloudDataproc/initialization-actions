/**
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
package com.google.cloud.hadoop.io.bigquery.mapred;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Wrap our mapreduce RecordReader so it can be called
 * from streaming hadoop.
 */
public class BigQueryMapredRecordReader
    implements RecordReader<LongWritable, JsonObject> {

  private org.apache.hadoop.mapreduce.RecordReader<LongWritable, JsonObject>
      mapreduceRecordReader;
  private long splitLength;

  /**
   * @param mapreduceRecordReader A mapreduce-based RecordReader.
   */
  public BigQueryMapredRecordReader(
      org.apache.hadoop.mapreduce.RecordReader<LongWritable, JsonObject>
          mapreduceRecordReader,
      long splitLength) {
    this.mapreduceRecordReader = mapreduceRecordReader;
    this.splitLength = splitLength;
  }

  public void close() throws IOException {
    mapreduceRecordReader.close();
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public JsonObject createValue() {
    return new JsonObject();
  }

  public long getPos() throws IOException {
    return splitLength * (long) getProgress();
  }

  public float getProgress() throws IOException {
    try {
      return mapreduceRecordReader.getProgress();
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
  }

  public boolean next(LongWritable key, JsonObject value) throws IOException {
    try {
      boolean hasNext = mapreduceRecordReader.nextKeyValue();
      if (!hasNext) {
          return false;
      }
      LongWritable nextKey = mapreduceRecordReader.getCurrentKey();
      JsonObject nextValue = mapreduceRecordReader.getCurrentValue();
      key.set(nextKey.get());
      copyJsonObject(nextValue, value);
      return true;
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
  }

  /**
   * Clears out the destination object, then copies the contents of
   * the source object into the destination object.
   */
  @VisibleForTesting
  void copyJsonObject(JsonObject source, JsonObject destination) {
    // Get a list of all the keys in the destination so that we can
    // remove all of the old values. We can't just use the entrySet()
    // directly because we can get a ConcurrentModificationException.
    ArrayList<String> keys = new ArrayList<>();
    for (Map.Entry<String, JsonElement> oldEntry : destination.entrySet()) {
        keys.add(oldEntry.getKey());
    }
    // Clear out all old values.
    for (String key : keys) {
      destination.remove(key);
    }
    // Then copy new values in.
    for (Map.Entry<String, JsonElement> newEntry : source.entrySet()) {
      destination.add(newEntry.getKey(), newEntry.getValue());
    }
  }
}
