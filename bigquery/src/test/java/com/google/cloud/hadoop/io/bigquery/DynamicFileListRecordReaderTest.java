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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Sleeper;
import com.google.cloud.hadoop.fs.gcs.InMemoryGoogleHadoopFileSystem;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.LoggerConfig;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for DynamicFileListRecordReader using an in-memory GHFS. */
@RunWith(JUnit4.class)
public class DynamicFileListRecordReaderTest {

  // Data we will write and read back.
  private static final String RECORD_0 = "{'day':'Sunday','letters':'6'}";
  private static final String RECORD_1 = "{'day':'Monday','letters':'6'}";
  private static final String RECORD_2 = "{'day':'Tuesday','letters':'7'}";

  private static final String SLEEP_ID = "test-sleep-id-12345";

  // Used to parse text lines into JsonObjects.
  private JsonParser jsonParser = new JsonParser();

  // Configuration object we use for specifying parameters to the record reader.
  private Configuration config;

  // Mock used simply to deliver the Configuration object to the record reader.
  @Mock private TaskAttemptContext mockTaskContext;

  // Mock Sleeper to use instead of actually blocking the thread in realtime.
  @Mock private Sleeper mockSleeper;

  // Parent directory of the shardPath.
  private Path basePath;

  // GCS Path owned by the recordReader being tested.
  private Path shardPath;

  // Number of total records we will tell the recordReader to expect.
  private long estimatedNumRecords;

  // InputSplit used to specify path and estimated number of records to the record reader.
  private ShardedInputSplit inputSplit;

  // The instance being tested.
  private DynamicFileListRecordReader<LongWritable, JsonObject> recordReader;

  // A FileSystem handle for populating files or cleaning up.
  private FileSystem fileSystem;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    LoggerConfig.getConfig(DynamicFileListRecordReader.class).setLevel(Level.FINE);

    // Set up a Configuration which will case "gs://" to grab an InMemoryGoogleHadoopFileSystem.
    config = InMemoryGoogleHadoopFileSystem.getSampleConfiguration();
    when(mockTaskContext.getConfiguration()).thenReturn(config);

    basePath = new Path("gs://foo-bucket/");
    shardPath = new Path(basePath, "shard0/data-*.json");
    estimatedNumRecords = 2;

    fileSystem = basePath.getFileSystem(config);
    fileSystem.mkdirs(shardPath.getParent());

    // Instead of actually blocking, make our mockSleeper throw an exception that we can catch
    // whenever the reader would otherwise be blocking.
    doThrow(new RuntimeException(SLEEP_ID)).when(mockSleeper).sleep(anyLong());

    resetRecordReader();
  }

  @After
  public void tearDown() throws IOException {
    // Delete everything in basePath.
    fileSystem.delete(basePath, true);
    recordReader.close();
  }

  private DynamicFileListRecordReader<LongWritable, JsonObject> createReader() {
    return new DynamicFileListRecordReader<>((split, configuration) -> new GsonRecordReader());
  }

  /** Returns the recordReader to clean state. */
  private void resetRecordReader() throws Exception {
    inputSplit = new ShardedInputSplit(shardPath, estimatedNumRecords);
    recordReader = createReader();
    recordReader.initialize(inputSplit, mockTaskContext);
    recordReader.setSleeper(mockSleeper);
  }

  /**
   * Since we set up the mockSleeper to throw an exception with a test-identifiable string, this
   * helper method checks that invoking nextKeyValue would've blocked but threw the fake exception
   * instead.
   */
  private void checkNextKeyValueWouldBlock() {
    RuntimeException e = assertThrows(RuntimeException.class, () -> recordReader.nextKeyValue());
    assertThat(e).hasMessageThat().contains(SLEEP_ID);
  }

  /** Creates file {@code outfile} adding a newline between each element of {@code lines}. */
  private void writeFile(Path outfile, List<String> lines) throws IOException {
    Text lineText = new Text();
    Text newLine = new Text("\n");
    try (FSDataOutputStream dataOut = fileSystem.create(outfile)) {
      for (String line : lines) {
        lineText.set(line);
        dataOut.write(lineText.getBytes(), 0, lineText.getLength());
        dataOut.write(newLine.getBytes(), 0, newLine.getLength());
      }
    }
  }

  @Test
  public void testInitializeCreatesShardDirectory() throws Exception {
    fileSystem.delete(shardPath.getParent(), true);
    assertThat(fileSystem.exists(shardPath.getParent())).isFalse();
    resetRecordReader();
    assertThat(fileSystem.exists(shardPath.getParent())).isTrue();
  }

  @Test
  public void testGetCurrentBeforeFirstRecord() {
    assertThat(recordReader.getCurrentKey()).isNull();
    assertThat(recordReader.getCurrentValue()).isNull();
    assertThat(recordReader.getProgress()).isZero();
  }

  @Test
  public void testGetProgressZeroEstimatedRecords() throws Exception {
    inputSplit = new ShardedInputSplit(shardPath, 0);
    recordReader = createReader();
    recordReader.initialize(inputSplit, mockTaskContext);
    assertThat(recordReader.getProgress()).isZero();
  }

  @Test
  public void testEmptyFileIsOnlyFileAndZeroIndex() throws Exception {
    checkNextKeyValueWouldBlock();
    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-000.json"));
    assertThat(recordReader.nextKeyValue()).isFalse();
    assertThat(recordReader.getCurrentKey()).isNull();
    assertThat(recordReader.getCurrentValue()).isNull();
  }

  @Test
  public void nextKeyValue_whenNoFilesAndMaxAttemptsReached_throwsException() throws Exception {
    config.setInt(BigQueryConfiguration.DYNAMIC_FILE_LIST_RECORD_READER_POLL_MAX_ATTEMPTS_KEY, 1);
    resetRecordReader();
    recordReader.setSleeper(Sleeper.DEFAULT);

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> recordReader.nextKeyValue());

    assertThat(e).hasMessageThat().doesNotContain(SLEEP_ID);
    assertThat(e).hasMessageThat().contains("Couldn't obtain any files after 1 attempt(s).");
  }

  @Test
  public void testEmptyFileIsOnlyFileAndNotZeroIndex() throws IOException {
    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-001.json"));
    checkNextKeyValueWouldBlock();
    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-002.json"));
    // Second file-marker with different index causes IllegalStateException.
    assertThrows(IllegalStateException.class, () -> recordReader.nextKeyValue());
  }

  @Test
  public void testEmptyFileThenDataFile() throws Exception {
    checkNextKeyValueWouldBlock();

    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-001.json"));
    checkNextKeyValueWouldBlock();

    writeFile(new Path(shardPath.getParent(), "data-000.json"), ImmutableList.of(RECORD_0));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_0));

    assertThat(recordReader.nextKeyValue()).isFalse();
  }

  @Test
  public void testEmptyFileIndexLessThanOtherFileBadKnownFile() throws Exception {
    writeFile(new Path(shardPath.getParent(), "data-000.json"), ImmutableList.of(RECORD_0));
    writeFile(new Path(shardPath.getParent(), "data-002.json"), ImmutableList.of(RECORD_1));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_0));

    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-001.json"));

    // We will successfully read the remaining available file before discovering the bad one.
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_1));

    assertThrows(IllegalStateException.class, () -> recordReader.nextKeyValue());
  }

  @Test
  public void testEmptyFileIndexLessThanOtherFileBadNewFile() throws Exception {
    writeFile(new Path(shardPath.getParent(), "data-000.json"), ImmutableList.of(RECORD_0));
    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-002.json"));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_0));

    writeFile(new Path(shardPath.getParent(), "data-003.json"), ImmutableList.of(RECORD_1));

    assertThrows(IllegalStateException.class, () -> recordReader.nextKeyValue());
  }

  @Test
  public void testSingleDataFile() throws Exception {
    writeFile(
        new Path(shardPath.getParent(), "data-000.json"),
        ImmutableList.of(RECORD_0, RECORD_1, RECORD_2));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_0));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(RECORD_0.length() + 1));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_1));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey())
        .isEqualTo(new LongWritable(RECORD_0.length() + RECORD_1.length() + 2));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_2));

    checkNextKeyValueWouldBlock();
    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-001.json"));
    assertThat(recordReader.nextKeyValue()).isFalse();
  }

  @Test
  public void testMultipleDataFilesInSingleList() throws Exception {
    writeFile(new Path(shardPath.getParent(), "data-000.json"), ImmutableList.of(RECORD_0));
    writeFile(
        new Path(shardPath.getParent(), "data-001.json"), ImmutableList.of(RECORD_1, RECORD_2));
    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-002.json"));

    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_0));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_1));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(RECORD_1.length() + 1));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_2));
    assertThat(recordReader.nextKeyValue()).isFalse();
  }

  @Test
  public void testMultipleFilesThenHangBeforeEmptyFileAppears() throws Exception {
    writeFile(new Path(shardPath.getParent(), "data-000.json"), ImmutableList.of(RECORD_0));
    writeFile(new Path(shardPath.getParent(), "data-001.json"), ImmutableList.of(RECORD_1));

    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_0));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_1));

    checkNextKeyValueWouldBlock();
    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-002.json"));
    assertThat(recordReader.nextKeyValue()).isFalse();
  }

  @Test
  public void testCloseBeforeEnd() throws Exception {
    writeFile(
        new Path(shardPath.getParent(), "data-000.json"), ImmutableList.of(RECORD_0, RECORD_1));

    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_0));

    recordReader.close();
  }

  @Test
  public void testThreeBatchesEndFileInMiddleBatch() throws Exception {
    writeFile(new Path(shardPath.getParent(), "data-000.json"), ImmutableList.of(RECORD_0));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_0));
    checkNextKeyValueWouldBlock();

    writeFile(new Path(shardPath.getParent(), "data-001.json"), ImmutableList.of(RECORD_1));
    fileSystem.createNewFile(new Path(shardPath.getParent(), "data-003.json"));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_1));
    checkNextKeyValueWouldBlock();
    checkNextKeyValueWouldBlock();

    writeFile(new Path(shardPath.getParent(), "data-002.json"), ImmutableList.of(RECORD_2));
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.getCurrentKey()).isEqualTo(new LongWritable(0));
    assertThat(recordReader.getCurrentValue()).isEqualTo(jsonParser.parse(RECORD_2));
    assertThat(recordReader.nextKeyValue()).isFalse();
  }

  @Test
  public void testBadFilename() throws Exception {
    String outOfBounds = String.format("data-%d.json", 1L + Integer.MAX_VALUE);
    fileSystem.createNewFile(new Path(shardPath.getParent(), outOfBounds));
    assertThrows(IndexOutOfBoundsException.class, () -> recordReader.nextKeyValue());
  }
}
