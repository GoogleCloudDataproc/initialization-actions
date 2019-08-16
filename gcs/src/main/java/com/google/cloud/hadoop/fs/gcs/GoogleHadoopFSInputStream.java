/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

/** A seekable and positionable FSInputStream that provides read access to a file. */
class GoogleHadoopFSInputStream extends FSInputStream {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Instance of GoogleHadoopFileSystemBase.
  private GoogleHadoopFileSystemBase ghfs;

  // All store IO access goes through this.
  private final SeekableByteChannel channel;

  // Path of the file to read.
  private URI gcsPath;

  // Number of bytes read through this channel.
  private long totalBytesRead;

  // Statistics tracker provided by the parent GoogleHadoopFileSystemBase for recording
  // numbers of bytes read.
  private final FileSystem.Statistics statistics;

  // Time of initialization
  private long initTime;

  // Used for single-byte reads.
  private final byte[] singleReadBuf = new byte[1];

  /**
   * Constructs an instance of GoogleHadoopFSInputStream object.
   *
   * @param ghfs Instance of GoogleHadoopFileSystemBase.
   * @param gcsPath Path of the file to read from.
   * @param statistics File system statistics object.
   * @throws IOException if an IO error occurs.
   */
  GoogleHadoopFSInputStream(
      GoogleHadoopFileSystemBase ghfs,
      URI gcsPath,
      GoogleCloudStorageReadOptions readOptions,
      FileSystem.Statistics statistics)
      throws IOException {
    logger.atFine().log(
        "GoogleHadoopFSInputStream(gcsPath: %s, readOptions: %s)", gcsPath, readOptions);
    this.ghfs = ghfs;
    this.gcsPath = gcsPath;
    this.statistics = statistics;
    this.initTime = System.nanoTime();
    this.totalBytesRead = 0;
    this.channel = ghfs.getGcsFs().open(gcsPath, readOptions);
  }

  /**
   * Reads a single byte from the underlying store.
   *
   * @return A single byte from the underlying store or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized int read() throws IOException {
    long startTime = System.nanoTime();

    // TODO(user): Wrap this in a while-loop if we ever introduce a non-blocking mode for the
    // underlying channel.
    int numRead = channel.read(ByteBuffer.wrap(singleReadBuf));
    if (numRead == -1) {
      return -1;
    }
    if (numRead != 1) {
      throw new IOException(
          String.format(
              "Somehow read %d bytes using single-byte buffer for path %s ending in position %d!",
              numRead, gcsPath, channel.position()));
    }
    byte b = singleReadBuf[0];

    totalBytesRead++;
    statistics.incrementBytesRead(1);
    long duration = System.nanoTime() - startTime;
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ1);
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ1_TIME, duration);
    return (b & 0xff);
  }

  /**
   * Reads up to length bytes from the underlying store and stores them starting at the specified
   * offset in the given buffer. Less than length bytes may be returned.
   *
   * @param buf The buffer into which data is returned.
   * @param offset The offset at which data is written.
   * @param length Maximum number of bytes to read.
   * @return Number of bytes read or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized int read(byte[] buf, int offset, int length) throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkNotNull(buf, "buf must not be null");
    if (offset < 0 || length < 0 || length > buf.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    int numRead = channel.read(ByteBuffer.wrap(buf, offset, length));

    if (numRead > 0) {
      // -1 means we actually read 0 bytes, but requested at least one byte.
      statistics.incrementBytesRead(numRead);
      totalBytesRead += numRead;
    }

    long duration = System.nanoTime() - startTime;
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ);
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_TIME, duration);
    return numRead;
  }

  /**
   * Reads up to length bytes from the underlying store and stores them starting at the specified
   * offset in the given buffer. Less than length bytes may be returned. Reading starts at the given
   * position.
   *
   * @param position Data is read from the stream starting at this position.
   * @param buf The buffer into which data is returned.
   * @param offset The offset at which data is written.
   * @param length Maximum number of bytes to read.
   * @return Number of bytes read or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized int read(long position, byte[] buf, int offset, int length)
      throws IOException {
    long startTime = System.nanoTime();
    int result = super.read(position, buf, offset, length);

    if (result > 0) {
      // -1 means we actually read 0 bytes, but requested at least one byte.
      statistics.incrementBytesRead(result);
      totalBytesRead += result;
    }

    long duration = System.nanoTime() - startTime;
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_POS);
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_POS_TIME, duration);
    return result;
  }

  /**
   * Gets the current position within the file being read.
   *
   * @return The current position within the file being read.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized long getPos() throws IOException {
    long pos = channel.position();
    logger.atFine().log("getPos(): %d", pos);
    return pos;
  }

  /**
   * Sets the current position within the file being read.
   *
   * @param pos The position to seek to.
   * @throws IOException if an IO error occurs or if the target position is invalid.
   */
  @Override
  public synchronized void seek(long pos) throws IOException {
    long startTime = System.nanoTime();
    logger.atFine().log("seek(%d)", pos);
    try {
      channel.position(pos);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }
    long duration = System.nanoTime() - startTime;
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.SEEK);
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.SEEK_TIME, duration);
  }

  /**
   * Seeks a different copy of the data. Not supported.
   *
   * @return true if a new source is found, false otherwise.
   */
  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  /**
   * Closes the current stream.
   *
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized void close() throws IOException {
    logger.atFinest().log("close(): %s", gcsPath);
    if (channel != null) {
      long startTime = System.nanoTime();
      logger.atFine().log("Closing '%s' file with %d total bytes read", gcsPath, totalBytesRead);
      channel.close();
      long duration = System.nanoTime() - startTime;
      ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_CLOSE);
      ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_CLOSE_TIME, duration);
      long streamDuration = System.nanoTime() - initTime;
      ghfs.increment(GoogleHadoopFileSystemBase.Counter.INPUT_STREAM);
      ghfs.increment(GoogleHadoopFileSystemBase.Counter.INPUT_STREAM_TIME, streamDuration);
    }
  }

  /**
   * Indicates whether this stream supports the 'mark' functionality.
   *
   * @return false (functionality not supported).
   */
  @Override
  public boolean markSupported() {
    // HDFS does not support it either and most Hadoop tools do not expect it.
    return false;
  }

  @Override
  public int available() throws IOException {
    if (!channel.isOpen()) {
      throw new ClosedChannelException();
    }
    return super.available();
  }
}
