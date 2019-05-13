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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadChannel;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * A SeekableByteChannel based on GoogleCloudStorageReadChannel that supports reading from in-memory
 * byte stream.
 */
public class InMemoryObjectReadChannel extends GoogleCloudStorageReadChannel {

  // All reads return data from this byte array. Set at construction time.
  private final byte[] content;

  /** Creates a new instance of InMemoryObjectReadChannel. */
  public InMemoryObjectReadChannel(byte[] content) throws IOException {
    this(content, GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Creates a new instance of InMemoryObjectReadChannel with {@code readOptions} plumbed into the
   * base class.
   */
  public InMemoryObjectReadChannel(byte[] content, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    super(readOptions);
    this.content = checkNotNull(content, "channelContents could not be null");
  }

  @Nullable
  @Override
  protected GoogleCloudStorageItemInfo getInitialMetadata() throws IOException {
    return null;
  }

  /**
   * Opens the underlying byte array stream, sets its position to currentPosition and sets size to
   * size of the byte array.
   *
   * @param bytesToRead ignored.
   * @throws IOException on IO error
   */
  @Override
  protected InputStream openStream(long bytesToRead) throws IOException {
    if (!metadataInitialized && content.length == 0 && currentPosition == 0) {
      setSize(content.length);
    } else {
      setSize(content.length);
      validatePosition(currentPosition);
    }
    InputStream inputStream = new ByteArrayInputStream(content);
    inputStream.skip(currentPosition);
    contentChannelPosition = currentPosition;
    return inputStream;
  }
}
