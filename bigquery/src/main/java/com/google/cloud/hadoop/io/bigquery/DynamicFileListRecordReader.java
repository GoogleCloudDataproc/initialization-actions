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

import com.google.api.client.util.Sleeper;
import com.google.cloud.hadoop.util.HadoopToStringUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * DynamicFileListRecordReader implements hadoop RecordReader by exposing a single logical
 * record stream despite being made up of multiple files which are still newly appearing
 * while this RecordReader is being used. Requires a single zero-record file to mark the end of the
 * series of files, and all files must appear under a single directory. Note that with some
 * file encodings a 0-record file may not be the same as a 0-length file. Filenames must follow
 * the naming convention specified in the static final members of this class; files will be
 * read in whatever order they appear, so multiple uses of this RecordReader may result in
 * very different orderings of data being read.
 */
public class DynamicFileListRecordReader<K, V>
    extends RecordReader<K, V> {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Directory/file-pattern which will contain all the files we read with this reader.
  private Path inputDirectoryAndPattern;

  // The estimated number of records we will read in total.
  private long estimatedNumRecords;

  // The interval we will poll listStatus/globStatus inside nextKeyValue() if we don't already
  // have a file ready for reading.
  private int pollIntervalMs;

  // Stashed away context for use with delegate readers.
  private TaskAttemptContext context;

  // The filesystem we will poll for files, based on initial inputDirectoryAndPattern Path.
  private FileSystem fileSystem;

  // The Pattern for matching export files, set up at initialization time.
  private Pattern exportPattern;

  // Counter for the number of records read so far.
  private long recordsRead = 0;

  // Factory for creating the underlying reader for iterating over records within a single file.
  private DelegateRecordReaderFactory<K, V> delegateRecordReaderFactory;

  // Underlying reader for iterating over the records within a single file.
  private RecordReader<K, V> delegateReader = null;

  // Set of all files we've successfully listed so far. Doesn't include end-indicator empty file.
  private Set<String> knownFileSet = new HashSet<>();

  // Queue of ready-but-not-yet-processed files whose filenames are also saved in knownFileSet,
  // in the order we discovered them.
  private Queue<FileStatus> fileQueue = new ArrayDeque<>();

  // Becomes positive once we've discovered the end-indicator file for the first time.
  private int endFileNumber = -1;

  // Sleeper used to sleep when polling listStatus.
  private Sleeper sleeper = Sleeper.DEFAULT;

  // Stored current key/value.
  private K currentKey = null;
  private V currentValue = null;

  public DynamicFileListRecordReader(
      DelegateRecordReaderFactory<K, V> delegateRecordReaderFactory) {
    this.delegateRecordReaderFactory = delegateRecordReaderFactory;
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
    try {
      logger.atInfo().log(
          "Initializing DynamicFileListRecordReader with split '%s', task context '%s'",
          HadoopToStringUtil.toString(genericSplit), HadoopToStringUtil.toString(context));
    } catch (InterruptedException ie) {
      logger.atWarning().withCause(ie).log("InterruptedException when logging InputSplit.");
    }
    Preconditions.checkArgument(genericSplit instanceof ShardedInputSplit,
        "InputSplit genericSplit should be an instance of ShardedInputSplit.");

    this.context = context;

    // Get inputDirectoryAndPattern from the split.
    ShardedInputSplit shardedSplit = (ShardedInputSplit) genericSplit;
    inputDirectoryAndPattern = shardedSplit.getShardDirectoryAndPattern();
    estimatedNumRecords = shardedSplit.getLength();
    if (estimatedNumRecords <= 0) {
      logger.atWarning().log(
          "Non-positive estimatedNumRecords '%s'; clipping to 1.", estimatedNumRecords);
      estimatedNumRecords = 1;
    }

    // Grab pollIntervalMs out of the config.
    pollIntervalMs = context.getConfiguration().getInt(
        BigQueryConfiguration.DYNAMIC_FILE_LIST_RECORD_READER_POLL_INTERVAL_MS_KEY,
        BigQueryConfiguration.DYNAMIC_FILE_LIST_RECORD_READER_POLL_INTERVAL_MS_DEFAULT);

    fileSystem = inputDirectoryAndPattern.getFileSystem(context.getConfiguration());

    // TODO(user): Make the base export pattern configurable.
    String exportPatternRegex = inputDirectoryAndPattern.getName().replace("*", "(\\d+)");
    exportPattern = Pattern.compile(exportPatternRegex);

    fileSystem.mkdirs(inputDirectoryAndPattern.getParent());
  }

  /**
   * Reads the next key, value pair. Gets next line and parses Json object. May hang for a long
   * time waiting for more files to appear in this reader's directory.
   *
   * @return true if a key/value pair was read.
   * @throws IOException on IO Error.
   */
  @Override
  public boolean nextKeyValue()
      throws IOException, InterruptedException {
    currentValue = null;

    // Check if we already have a reader in-progress.
    if (delegateReader != null) {
      if (delegateReader.nextKeyValue()) {
        populateCurrentKeyValue();
        return true;
      } else {
        delegateReader.close();
        delegateReader = null;
      }
    }

    boolean needRefresh = !isNextFileReady() && shouldExpectMoreFiles();
    while (needRefresh) {
      logger.atFine().log("No files available, but more are expected; refreshing...");
      refreshFileList();
      needRefresh = !isNextFileReady() && shouldExpectMoreFiles();
      if (needRefresh) {
        logger.atFine().log("No new files found, sleeping before trying again...");
        try {
          sleeper.sleep(pollIntervalMs);
          context.progress();
        } catch (InterruptedException ie) {
          logger.atWarning().withCause(ie).log("Interrupted while sleeping.");
        }
      }
    }

    if (isNextFileReady()) {
      // Open the file and see if it's the 0-record end of dataset marker:
      FileStatus newFile = moveToNextFile();
      logger.atInfo().log(
          "Moving to next file '%s' which has %s bytes. Records read so far: %s",
          newFile.getPath(), newFile.getLen(), recordsRead);

      InputSplit split = new FileSplit(newFile.getPath(), 0, newFile.getLen(), new String[0]);
      delegateReader = delegateRecordReaderFactory.createDelegateRecordReader(
          split, context.getConfiguration());
      delegateReader.initialize(split, context);
      if (!delegateReader.nextKeyValue()) {
        // we found the end of dataset marker.
        setEndFileMarkerFile(newFile.getPath().getName());
        return nextKeyValue();
      } else {
        populateCurrentKeyValue();
        return true;
      }
    }

    Preconditions.checkState(
        !shouldExpectMoreFiles(),
        "Should not have exited the refresh loop shouldExpectMoreFiles = true "
            + "and no files ready to read.");

    // No files ready and we shouldn't expect any more.
    return false;
  }

  /**
   * Gets the current key as reported by the delegate record reader. This will generally be the
   * byte position within the current file.
   *
   * @return the current key or null if there is no current key.
   */
  @Override
  public K getCurrentKey() {
    return currentKey;
  }

  /**
   * Gets the current value.
   *
   * @return the current value or null if there is no current value.
   */
  @Override
  public V getCurrentValue() {
    return currentValue;
  }

  /**
   * Returns the current progress based on the number of records read compared to the *estimated*
   * total number of records planned to be read; this number may be inexact, but will not
   * report a number greater than 1.
   *
   * @return a number between 0.0 and 1.0 that is the fraction of the data read.
   */
  @Override
  public float getProgress() {
    return Math.min(1.0f, recordsRead / (float) estimatedNumRecords);
  }

  /**
   * Closes the record reader.
   *
   * @throws IOException on IO Error.
   */
  @Override
  public void close()
      throws IOException {
    if (delegateReader != null) {
      logger.atWarning().log(
          "Got non-null delegateReader during close(); possible premature close() call.");
      delegateReader.close();
      delegateReader = null;
    }
  }

  /**
   * Allows setting a mock Sleeper for tests to not have to wait in realtime for polling.
   */
  @VisibleForTesting
  void setSleeper(Sleeper sleeper) {
    this.sleeper = sleeper;
  }

  /**
   * Helper for populating currentKey and currentValue from delegateReader. Should only be called
   * once per new key/value from the delegateReader; this method is also responsible for tracking
   * the number of records read so far.
   */
  private void populateCurrentKeyValue() throws IOException, InterruptedException {
    currentKey = delegateReader.getCurrentKey();
    currentValue = delegateReader.getCurrentValue();
    ++recordsRead;
  }

  /**
   * @return true if the next file is available for immediate usage.
   */
  private boolean isNextFileReady() {
    return !fileQueue.isEmpty();
  }

  /**
   * Moves to the next file; must have checked to make sure isNextFileReady() returns true prior
   * to calling this.
   */
  private FileStatus moveToNextFile() {
    return fileQueue.remove();
  }

  /**
   * @return true if we haven't found the end-indicator file yet, or if the number of known files
   *     is less than the total number of files as indicated by the endFileNumber. Note that
   *     returning false does *not* mean this RecordReader is done, just that we know of all the
   *     files we plan to read.
   */
  private boolean shouldExpectMoreFiles() {
    if (endFileNumber == -1 || knownFileSet.size() <= endFileNumber) {
      return true;
    }
    return false;
  }

  /**
   * Parses the numerical index out of a String which matches exportPattern; the exportPattern
   * should have been compiled from a regex that looks like "data-(\d+).json".
   *
   * @throws IndexOutOfBoundsException if the parsed value is greater than Integer.MAX_VALUE.
   */
  private int parseFileIndex(String fileName) {
    Matcher match = null;
    String indexString = null;
    try {
      match = exportPattern.matcher(fileName);
      match.find();
      indexString = match.group(1);
    } catch (Exception e) {
      throw new IllegalStateException(String.format("Failed to parse file '%s'", fileName), e);
    }
    long longValue = Long.parseLong(indexString);
    if (longValue > Integer.MAX_VALUE) {
      throw new IndexOutOfBoundsException(String.format(
          "Invalid fileName '%s'; max allowable index is %d, got %d instead",
          fileName, Integer.MAX_VALUE, longValue));
    }
    return (int) longValue;
  }

  /**
   * Record a specific file as being the 0-record end of stream marker.
   */
  private void setEndFileMarkerFile(String fileName) {
    int fileIndex = parseFileIndex(fileName);
    if (endFileNumber == -1) {
      // First time finding the end-marker file.
      endFileNumber = fileIndex;
      logger.atInfo().log("Found end-marker file '%s' with index %s", fileName, endFileNumber);

      // Sanity-check known filenames against the endFileNumber.
      for (String knownFile : knownFileSet) {
        int knownFileIndex = parseFileIndex(knownFile);
        Preconditions.checkState(
            knownFileIndex <= endFileNumber,
            "Found known file '%s' with index %s, which isn't less than or "
                + "equal to than endFileNumber %s!",
            knownFile, knownFileIndex, endFileNumber);
      }
    } else {
      // If we found it before, make sure the file we're looking at has the same index.
      Preconditions.checkState(fileIndex == endFileNumber,
          "Found new end-marker file '%s' with index %s but already have endFileNumber %s!",
          fileName, fileIndex, endFileNumber);
    }
  }

  /**
   * Lists files, and sifts through the results for any new files we haven't found before.
   * If a file of size 0 is found, we mark the 'endFileNumber' from it.
   */
  private void refreshFileList()
      throws IOException {
    FileStatus[] files = fileSystem.globStatus(inputDirectoryAndPattern);
    for (FileStatus file : files) {
      String fileName = file.getPath().getName();
      if (!knownFileSet.contains(fileName)) {
        if (endFileNumber != -1) {
          // Sanity check against endFileNumber.
          int newFileIndex = parseFileIndex(fileName);
          Preconditions.checkState(newFileIndex < endFileNumber,
              "Found new file '%s' with index %s, which isn't less than endFileNumber %s!",
              fileName, newFileIndex, endFileNumber);
        }

        logger.atInfo().log(
            "Adding new file '%s' of size %s to knownFileSet.", fileName, file.getLen());
        knownFileSet.add(fileName);
        fileQueue.add(file);
      }
    }
  }
}
