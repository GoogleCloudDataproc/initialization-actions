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

package com.google.cloud.hadoop.gcsio;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * A readable byte channel that supports seek operation.
 *
 * Note:
 * JDK 1.7 supports SeekableByteChannel but we cannot use it here
 * for two reasons:
 * -- 1.7 is not yet enabled internally in Google.
 * -- Hadoop v0.20.205.0 that we plan to use is built using 1.6.
 *
 * Because of these reasons, we define our own SeekableReadableByteChannel.
 * At some point in future when the 2 issues above are resolved, we should
 * get rid of our own interface and start using SeekableByteChannel.
 * For that reason, We have made our own SeekableReadableByteChannel
 * identical to the one supplied by Java 1.7. Note that SeekableByteChannel
 * in Java 1.7 supports both reading and writing. GCS does not support
 * random access writes. We will likely throw IllegalStateException if
 * one tries to read from or seek for a channel opened for writing.
 */
public interface SeekableReadableByteChannel
    extends ReadableByteChannel {

  /**
   * Returns this channel's position.
   *
   * @return This channel's position.
   * @throws IOException
   */
  public long position()
      throws IOException;

  /**
   * Sets this channel's position.
   *
   * @param newPosition The new position, counting the number of bytes from the beginning of the
   *        entity.
   * @return This channel.
   * @throws IOException
   */
  public SeekableReadableByteChannel position(long newPosition)
      throws IOException;

  /**
   * Returns the current size of entity to which this channel is connected.
   *
   * @throws IOException
   */
  public long size()
      throws IOException;
}