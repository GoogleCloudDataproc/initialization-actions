/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio.testing;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadChannel;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A SeekableByteChannel based on GoogleCloudStorageReadChannel that supports reading from in-memory
 * byte stream.
 */
public class InMemoryObjectReadChannel extends GoogleCloudStorageReadChannel {

  // All reads return data from this byte array. Set at construction time.
  private final byte[] channelContent;

  /** Creates a new instance of InMemoryObjectReadChannel. */
  public InMemoryObjectReadChannel(byte[] channelContent) throws IOException {
    this(channelContent, GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Creates a new instance of InMemoryObjectReadChannel with {@code readOptions} plumbed into the
   * base class.
   */
  public InMemoryObjectReadChannel(byte[] channelContent, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    super(readOptions);
    this.channelContent = checkNotNull(channelContent, "channelContents could not be null");
    // gzipEncoded and size should be initialized in constructor, the same as with super-class
    // gzipEncoded is false by default.
    setSize(channelContent.length);
  }

  /** No-op, because encoding and size are set in constructor. */
  @Override
  protected void initEncodingAndSize() {}

  /**
   * Opens the underlying byte array stream, sets its position to currentPosition and sets size to
   * size of the byte array.
   *
   * @param newPosition position to seek into the new stream.
   * @param limit ignored.
   * @throws IOException on IO error
   */
  @Override
  protected InputStream openStream(long newPosition, long limit) throws IOException {
    validatePosition(newPosition);
    InputStream inputStream = new ByteArrayInputStream(channelContent);
    inputStream.skip(newPosition);
    return inputStream;
  }
}
