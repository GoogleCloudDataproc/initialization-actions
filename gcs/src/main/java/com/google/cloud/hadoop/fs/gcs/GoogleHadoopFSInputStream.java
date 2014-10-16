/**
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

import com.google.cloud.hadoop.gcsio.SeekableReadableByteChannel;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * A seekable and positionable FSInputStream that provides read access to a file.
 */
class GoogleHadoopFSInputStream
    extends FSInputStream {

  // Logging helper.
  private static LogUtil log = new LogUtil(GoogleHadoopFSInputStream.class);

  // Instance of GoogleHadoopFileSystemBase.
  private GoogleHadoopFileSystemBase ghfs;

  // All store IO access goes through this.
  private SeekableReadableByteChannel channel;

  // Internal buffer.
  private ByteBuffer buffer;

  // Path of the file to read.
  private URI gcsPath;

  // Number of bytes read through this channel.
  private long totalBytesRead;

  // Statistics tracker provided by the parent GoogleHadoopFileSystemBase for recording
  // numbers of bytes read.
  private final FileSystem.Statistics statistics;

  // Time of initialization
  private long initTime;

  /**
   * Constructs an instance of GoogleHadoopFSInputStream object.
   *
   * @param ghfs Instance of GoogleHadoopFileSystemBase.
   * @param gcsPath Path of the file to read from.
   * @param bufferSize Size of the buffer to use.
   * @param statistics File system statistics object.
   * @throws IOException if an IO error occurs.
   */
  GoogleHadoopFSInputStream(
      GoogleHadoopFileSystemBase ghfs, URI gcsPath, int bufferSize,
      FileSystem.Statistics statistics)
      throws IOException {
    log.debug("GoogleHadoopFSInputStream(%s, %d)", gcsPath, bufferSize);
    this.ghfs = ghfs;
    this.gcsPath = gcsPath;
    this.statistics = statistics;
    initTime = System.nanoTime();
    totalBytesRead = 0;
    channel = ghfs.getGcsFs().open(gcsPath);
    buffer = ByteBuffer.allocate(bufferSize);
    buffer.limit(0);
    buffer.rewind();
  }

  /**
   * Reads a single byte from the underlying store.
   *
   * @return A single byte from the underlying store or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized int read()
      throws IOException {
    long startTime = System.nanoTime();

    // Refill the internal buffer if necessary.
    if (!buffer.hasRemaining()) {
      buffer.clear();
      int numBytesRead = channel.read(buffer);
      if (numBytesRead <= 0) {
        buffer.limit(0);
        buffer.rewind();
        return -1;
      }

      buffer.flip();
    }

    byte b = buffer.get();
    totalBytesRead++;
    statistics.incrementBytesRead(1);
    long duration = System.nanoTime() - startTime;
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ1);
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ1_TIME, duration);
    return (b & 0xff);
  }

  /**
   * Reads up to length bytes from the underlying store and stores
   * them starting at the specified offset in the given buffer.
   * Less than length bytes may be returned.
   *
   * @param buf The buffer into which data is returned.
   * @param offset The offset at which data is written.
   * @param length Maximum number of bytes to read.
   *
   * @return Number of bytes read or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public int read(byte[] buf, int offset, int length)
      throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkNotNull(buf, "buf must not be null");
    if (offset < 0 || length < 0 || length > buf.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    int numRead = 0;
    while (numRead < length) {
      int needToRead = length - numRead;
      if (buffer.remaining() >= needToRead) {
        // There are sufficient bytes, we'll only read a (not-necessarily-proper) subset of the
        // internal buffer.
        buffer.get(buf, offset + numRead, needToRead);
        numRead += needToRead;
      } else if (buffer.hasRemaining()) {
        // We must take everything from the buffer and loop again.
        int singleRead = buffer.remaining();
        buffer.get(buf, offset + numRead, singleRead);
        numRead += singleRead;
      } else {
        // Buffer is empty AND we still need more bytes to be read.
        long channelTime = System.nanoTime();
        buffer.clear();
        int numNewBytes = channel.read(buffer);
        long channelDuration = System.nanoTime() - channelTime;
        ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_FROM_CHANNEL);
        ghfs.increment(
            GoogleHadoopFileSystemBase.Counter.READ_FROM_CHANNEL_TIME, channelDuration);
        if (numNewBytes <= 0) {
          // Ran out of underlying channel bytes.
          buffer.limit(0);
          buffer.rewind();

          if (numRead == 0) {
            // Never read anything at all; return -1 to indicate EOF. Otherwise, we'll leave
            // numRead untouched and return the number of bytes we did manage to retrieve.
            numRead = -1;
          }
          break;
        } else {
          // Successfully got some new bytes from the channel; keep looping.
          buffer.flip();
        }
      }
    }

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
   * Reads up to length bytes from the underlying store and stores
   * them starting at the specified offset in the given buffer.
   * Less than length bytes may be returned. Reading starts at the
   * given position.
   *
   * @param position Data is read from the stream starting at this position.
   * @param buf The buffer into which data is returned.
   * @param offset The offset at which data is written.
   * @param length Maximum number of bytes to read.
   *
   * @return Number of bytes read or -1 on EOF.
   * @throws IOException if an IO error occurs.
   */
  @Override
  public int read(long position, byte[] buf, int offset, int length)
    throws IOException {
    long startTime = System.nanoTime();
    int result = super.read(position, buf, offset, length);
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
  public synchronized long getPos()
      throws IOException {
    long pos = channel.position() - buffer.remaining();
    log.debug("getPos: %d", pos);
    return pos;
  }

  /**
   * Sets the current position within the file being read.
   *
   * @param pos The position to seek to.
   * @throws IOException if an IO error occurs or if the target position is invalid.
   */
  @Override
  public synchronized void seek(long pos)
      throws IOException {
    long startTime = System.nanoTime();
    log.debug("seek: %d", pos);
    long curPos = getPos();
    if (curPos == pos) {
      log.debug("Skipping no-op seek.");
    } else if (pos < curPos && curPos - pos <= buffer.position()) {
      // Skip backwards few enough bytes that our current buffer still has those bytes around
      // so that we simply need to reposition the buffer backwards a bit.
      long skipBack = curPos - pos;

      // Guaranteed safe to cast as an (int) because curPos - pos is <= buffer.position(), and
      // position() is itself an int.
      int newBufferPosition = buffer.position() - (int) skipBack;
      log.debug("Skipping backward %d bytes in-place from buffer pos %s to new pos %s",
          skipBack, buffer.position(), newBufferPosition);
      buffer.position(newBufferPosition);
    } else if (curPos < pos && pos < channel.position()) {
      // Skip forwards--between curPos and channel.position() are the bytes we already have
      // available in the buffer.
      long skipBytes = pos - curPos;
      Preconditions.checkState(skipBytes < buffer.remaining(),
          "skipBytes (%s) must be less than buffer.remaining() (%s)",
          skipBytes, buffer.remaining());

      // We know skipBytes is castable as (int) even if the top-level position is capable of
      // overflowing an int, since we at least assert that skipBytes < buffer.remaining(),
      // which is itself less than Integer.MAX_VALUE.
      int newBufferPosition = buffer.position() + (int) skipBytes;
      log.debug("Skipping %d bytes in-place from buffer pos %d to new pos %d",
          skipBytes, buffer.position(), newBufferPosition);
      buffer.position(newBufferPosition);
    } else {
      log.debug("New position '%d' out of range of inplace buffer, with curPos (%d), "
         + "buffer.position() (%d) and buffer.remaining() (%d).",
          pos, curPos, buffer.position(), buffer.remaining());
      try {
        channel.position(pos);
      } catch (IllegalArgumentException e) {
        throw new IOException(e);
      }
      buffer.limit(0);
      buffer.rewind();
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
  public synchronized boolean seekToNewSource(long targetPos)
      throws IOException {
    return false;
  }

  /**
   * Closes the current stream.
   *
   * @throws IOException if an IO error occurs.
   */
  @Override
  public synchronized void close()
      throws IOException {
    if (channel != null) {
    long startTime = System.nanoTime();
      try {
        log.debug("close: file: %s, totalBytesRead: %d", gcsPath, totalBytesRead);
        channel.close();
        long duration = System.nanoTime() - startTime;
        ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_CLOSE);
        ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_CLOSE_TIME, duration);
        long streamDuration = System.nanoTime() - initTime;
        ghfs.increment(GoogleHadoopFileSystemBase.Counter.INPUT_STREAM);
        ghfs.increment(
            GoogleHadoopFileSystemBase.Counter.INPUT_STREAM_TIME, streamDuration);
      } finally {
        channel = null;
      }
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
}
