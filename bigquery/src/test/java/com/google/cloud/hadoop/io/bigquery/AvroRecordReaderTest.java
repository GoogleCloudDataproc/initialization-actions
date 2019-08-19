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

import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AvroRecordReaderTest {

  private static final int RECORD_COUNT = 50;
  private static final int AUTO_SYNC_INTERVAL = 32; /* Auto sync every 32 bytes */
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File testAvroFile;
  private List<String> allAddedKeys;

  @Before
  public void setup() throws IOException {
    Schema schema =
        SchemaBuilder.record("BigQueryRecord").fields()
            .name("key").type().stringBuilder().endString().noDefault()
            .name("value1").type().stringBuilder().endString().noDefault()
            .name("value2").type().intBuilder().endInt().noDefault()
            .endRecord();

    GenericDatumWriter<GenericData.Record> recordWriter = new GenericDatumWriter<>(schema);
    testAvroFile = temporaryFolder.newFile("TestAvroFile");
    if (testAvroFile.exists()) {
      testAvroFile.delete();
    }
    DataFileWriter<GenericData.Record> dataFileWriter =
        new DataFileWriter<>(recordWriter).create(schema, testAvroFile);
    dataFileWriter.setSyncInterval(AUTO_SYNC_INTERVAL);
    ImmutableList.Builder<String> addedKeysBuilder = ImmutableList.builder();
    for (int idx = 0; idx < RECORD_COUNT; idx++) {
      GenericData.Record record = new GenericData.Record(schema);
      String key = String.format("key-%s", idx);
      record.put("key", key);
      record.put("value1", String.format("value-%s", idx));
      record.put("value2", idx * RECORD_COUNT);
      dataFileWriter.append(record);
      addedKeysBuilder.add(key);
    }
    dataFileWriter.close();
    allAddedKeys = addedKeysBuilder.build();
  }

  /** Collect all values contained in the field "key" within all records in the given reader. */
  private List<String> collectRecordKeys(AvroRecordReader recordReader) throws IOException {
    List<String> result = new ArrayList<>();
    while (recordReader.nextKeyValue()) {
      result.add(recordReader.currentRecord.get("key").toString());
    }
    return result;
  }

  /** Count all records available until the reader reports that no more are available. */
  private int remainingRecordCount(AvroRecordReader recordReader) throws IOException {
    return collectRecordKeys(recordReader).size();
  }

  @Test
  public void testSingleSplit() throws IOException {
    FileSplit fileSplit =
        new FileSplit(
            new Path("file", null,  testAvroFile.getAbsolutePath()),
            0,
            testAvroFile.length(),
            new String[0]);
    AvroRecordReader recordReader = new AvroRecordReader();
    recordReader.initializeInternal(fileSplit, new Configuration());
    Truth.assertThat(remainingRecordCount(recordReader)).isEqualTo(RECORD_COUNT);
    recordReader.close();
  }

  @Test
  public void testMultipleSplits() throws IOException {
    long fileLength = testAvroFile.length();
    List<FileSplit> splits = new ArrayList<>();
    Path hadoopPath = new Path("file", null,  testAvroFile.getAbsolutePath());

    for (int blockStart = 0; blockStart < fileLength; blockStart += AUTO_SYNC_INTERVAL) {
      splits.add(new FileSplit(hadoopPath, blockStart, AUTO_SYNC_INTERVAL, new String[0]));
    }

    List<String> allRecordKeys = new ArrayList<>();
    long totalFileRecords = 0;
    for (FileSplit split : splits) {
      try (AvroRecordReader reader = new AvroRecordReader()) {
        reader.initializeInternal(split, new Configuration());
        List<String> keysInSplit = collectRecordKeys(reader);
        allRecordKeys.addAll(keysInSplit);
        int recordsInSplit = keysInSplit.size();
        totalFileRecords += recordsInSplit;
        // Not all 'blocks' contain records, but none should have all records
        Truth.assertThat(recordsInSplit)
            .isLessThan(RECORD_COUNT);
      }
    }

    Truth.assertThat(allRecordKeys).containsExactlyElementsIn(allAddedKeys);
    Truth.assertThat(totalFileRecords).isEqualTo(RECORD_COUNT);
  }
}
