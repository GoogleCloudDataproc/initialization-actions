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

import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.common.flogger.GoogleLogger;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;

/**
 * A buffered output stream that allows writing to a GCS object.
 */
class GoogleHadoopOutputStream
    extends OutputStream {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Instance of GoogleHadoopFileSystemBase.
  private GoogleHadoopFileSystemBase ghfs;

  // All store IO access goes through this.
  private WritableByteChannel channel;

  // Output stream corresponding to channel.
  private OutputStream out;

  // Path of the file to write to.
  private URI gcsPath;

  // Statistics tracker provided by the parent GoogleHadoopFileSystemBase for recording
  // numbers of bytes written.
  private final FileSystem.Statistics statistics;
  
  // Time of initialization
  private long initTime;

  /**
   * Constructs an instance of GoogleHadoopOutputStream object.
   *
   * @param ghfs Instance of GoogleHadoopFileSystemBase.
   * @param gcsPath Path of the file to write to.
   * @param bufferSize Size of the buffer to use.
   * @param statistics File system statistics object.
   * @throws IOException if an IO error occurs.
   */
  GoogleHadoopOutputStream(
      GoogleHadoopFileSystemBase ghfs, URI gcsPath, int bufferSize,
      FileSystem.Statistics statistics, CreateFileOptions createFileOptions)
      throws IOException {
    logger.atFine().log("GoogleHadoopOutputStream(%s, %s)", gcsPath, bufferSize);
    this.ghfs = ghfs;
    this.gcsPath = gcsPath;
    this.statistics = statistics;
    initTime = System.nanoTime();
    try {
      channel = ghfs.getGcsFs().create(gcsPath, createFileOptions);
    } catch (java.nio.file.FileAlreadyExistsException faee) {
      // Need to convert to the Hadoop flavor of FileAlreadyExistsException.
      throw (FileAlreadyExistsException)
          (new FileAlreadyExistsException(faee.getMessage()).initCause(faee));
    }
    OutputStream rawStream = Channels.newOutputStream(channel);
    out = new BufferedOutputStream(rawStream, bufferSize);
  }

  /**
   * Writes the specified byte to this output stream.
   */
  @Override
  public void write(int b)
      throws IOException {
    long startTime = System.nanoTime();
    out.write(b);
    statistics.incrementBytesWritten(1);
    long duration = System.nanoTime() - startTime;
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.WRITE1);
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.WRITE1_TIME, duration);
  }

  /**
   * Writes to this output stream 'len' bytes of the specified buffer starting at the given offset.
   */
  @Override
  public void write(byte[] b, int offset, int len)
      throws IOException {
    long startTime = System.nanoTime();
    out.write(b, offset, len);
    statistics.incrementBytesWritten(len);
    long duration = System.nanoTime() - startTime;
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.WRITE);
    ghfs.increment(GoogleHadoopFileSystemBase.Counter.WRITE_TIME, duration);
  }

  /**
   * Closes this output stream and releases any system resources associated with this stream.
   */
  @Override
  public void close()
      throws IOException {
    if (out != null) {
      try {
        long startTime = System.nanoTime();
        out.close();
        long duration = System.nanoTime() - startTime;
        ghfs.increment(GoogleHadoopFileSystemBase.Counter.WRITE_CLOSE);
        ghfs.increment(GoogleHadoopFileSystemBase.Counter.WRITE_CLOSE_TIME, duration);
        long streamDuration = System.nanoTime() - initTime;
        ghfs.increment(GoogleHadoopFileSystemBase.Counter.OUTPUT_STREAM);
        ghfs.increment(
            GoogleHadoopFileSystemBase.Counter.OUTPUT_STREAM_TIME, streamDuration);
        logger.atFine().log("close(%s)", gcsPath);
      } finally {
        out = null;
        channel = null;
      }
    }
  }

  WritableByteChannel getInternalChannel() {
    return channel;
  }
}
