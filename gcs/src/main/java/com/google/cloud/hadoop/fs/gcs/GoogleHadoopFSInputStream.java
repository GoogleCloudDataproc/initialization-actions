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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A seekable and positionable FSInputStream that provides read access to a file.
 */
class GoogleHadoopFSInputStream
    extends FSInputStream {

  // Logging helper.
  private static final Logger LOG = LoggerFactory.getLogger(GoogleHadoopFSInputStream.class);

  // Instance of GoogleHadoopFileSystemBase.
  private GoogleHadoopFileSystemBase ghfs;

  // All store IO access goes through this.
  private final SeekableByteChannel channel;

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

  // Used for single-byte reads.
  private final byte[] singleReadBuf = new byte[1];

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
    LOG.debug("GoogleHadoopFSInputStream({}, {})", gcsPath, bufferSize);
    this.ghfs = ghfs;
    this.gcsPath = gcsPath;
    this.statistics = statistics;
    initTime = System.nanoTime();
    totalBytesRead = 0;

    boolean enableInternalBuffer = ghfs.getConf().getBoolean(
        GoogleHadoopFileSystemBase.GCS_INPUTSTREAM_INTERNALBUFFER_ENABLE_KEY,
        GoogleHadoopFileSystemBase.GCS_INPUTSTREAM_INTERNALBUFFER_ENABLE_DEFAULT);
    LOG.debug("enableInternalBuffer: {}", enableInternalBuffer);

    boolean supportContentEncoding = ghfs.getConf().getBoolean(
        GoogleHadoopFileSystemBase.GCS_INPUTSTREAM_SUPPORT_CONTENT_ENCODING_ENABLE_KEY,
        GoogleHadoopFileSystemBase.GCS_INPUTSTREAM_SUPPORT_CONTENT_ENCODING_ENABLE_DEFAULT);
    LOG.debug("supportContentEncoding: {}", supportContentEncoding);

    boolean fastFailOnNotFound = ghfs.getConf().getBoolean(
        GoogleHadoopFileSystemBase.GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_KEY,
        GoogleHadoopFileSystemBase.GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_DEFAULT);
    LOG.debug("fastFailOnNotFound: {}", fastFailOnNotFound);

    long inplaceSeekLimit = ghfs.getConf().getLong(
        GoogleHadoopFileSystemBase.GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_KEY,
        GoogleHadoopFileSystemBase.GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_DEFAULT);
    LOG.debug("inplaceSeekLimit: {}", inplaceSeekLimit);

    GoogleCloudStorageReadOptions.Builder readOptions = new GoogleCloudStorageReadOptions.Builder()
        .setSupportContentEncoding(supportContentEncoding)
        .setFastFailOnNotFound(fastFailOnNotFound)
        .setInplaceSeekLimit(inplaceSeekLimit);
    if (enableInternalBuffer) {
      buffer = ByteBuffer.allocate(bufferSize);
      buffer.limit(0);
      buffer.rewind();
      // If we're using a buffer in this layer, skip the lower-level buffer.
      readOptions.setBufferSize(0);
    } else {
      buffer = null;
      // If not using internal buffer, let the lower-level channel figure out how to do buffering.
      readOptions.setBufferSize(bufferSize);
    }

    channel = ghfs.getGcsFs().open(gcsPath, readOptions.build());
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

    byte b;
    if (buffer == null) {
      // TODO(user): Wrap this in a while-loop if we ever introduce a non-blocking mode for the
      // underlying channel.
      int numRead = channel.read(ByteBuffer.wrap(singleReadBuf));
      if (numRead == -1) {
        return -1;
      } else if (numRead != 1) {
        throw new IOException(String.format(
            "Somehow read %d bytes using single-byte buffer for path %s ending in position %d!",
            numRead, gcsPath, channel.position()));
      }
      b = singleReadBuf[0];
    } else {
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

      b = buffer.get();
    }
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
  public synchronized int read(byte[] buf, int offset, int length)
      throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkNotNull(buf, "buf must not be null");
    if (offset < 0 || length < 0 || length > buf.length - offset) {
      throw new IndexOutOfBoundsException();
    }

    int numRead = 0;
    if (buffer == null) {
      numRead = channel.read(ByteBuffer.wrap(buf, offset, length));
    } else {
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
  public synchronized long getPos()
      throws IOException {
    int bufRemaining = (buffer == null ? 0 : buffer.remaining());
    long pos = channel.position() - bufRemaining;
    LOG.debug("getPos: {}", pos);
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
    LOG.debug("seek: {}", pos);
    if (buffer == null) {
      try {
        channel.position(pos);
      } catch (IllegalArgumentException e) {
        throw new IOException(e);
      }
    } else {
      long curPos = getPos();
      if (curPos == pos) {
        LOG.debug("Skipping no-op seek.");
      } else if (pos < curPos && curPos - pos <= buffer.position()) {
        // Skip backwards few enough bytes that our current buffer still has those bytes around
        // so that we simply need to reposition the buffer backwards a bit.
        long skipBack = curPos - pos;

        // Guaranteed safe to cast as an (int) because curPos - pos is <= buffer.position(), and
        // position() is itself an int.
        int newBufferPosition = buffer.position() - (int) skipBack;
        LOG.debug("Skipping backward {} bytes in-place from buffer pos {} to new pos {}",
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
        LOG.debug("Skipping {} bytes in-place from buffer pos {} to new pos {}",
            skipBytes, buffer.position(), newBufferPosition);
        buffer.position(newBufferPosition);
      } else {
        LOG.debug("New position '{}' out of range of inplace buffer, with curPos ({}), "
            + "buffer.position() ({}) and buffer.remaining() ({}).",
            pos, curPos, buffer.position(), buffer.remaining());
        try {
          channel.position(pos);
        } catch (IllegalArgumentException e) {
          throw new IOException(e);
        }
        buffer.limit(0);
        buffer.rewind();
      }
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
      LOG.debug("close: file: {}, totalBytesRead: {}", gcsPath, totalBytesRead);
      channel.close();
      long duration = System.nanoTime() - startTime;
      ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_CLOSE);
      ghfs.increment(GoogleHadoopFileSystemBase.Counter.READ_CLOSE_TIME, duration);
      long streamDuration = System.nanoTime() - initTime;
      ghfs.increment(GoogleHadoopFileSystemBase.Counter.INPUT_STREAM);
      ghfs.increment(
          GoogleHadoopFileSystemBase.Counter.INPUT_STREAM_TIME, streamDuration);
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
