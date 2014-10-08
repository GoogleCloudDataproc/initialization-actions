/**
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

package com.google.cloud.hadoop.util;


/**
 * Options for the GoogleCloudStorageWriteChannel.
 */
public class AsyncWriteChannelOptions {

  /**
   * Default of whether to limit files to 250GB by default.
   */
  public static final boolean LIMIT_FILESIZE_TO_250GB_DEFAULT = true;
  /**
   * Default upload buffer size.
   */
  public static final int UPLOAD_BUFFER_SIZE_DEFAULT = 64 * 1024 * 1024;

  /**
   * Mutable builder for the GoogleCloudStorageWriteChannelOptions class.
   */
  public static class Builder {
    private boolean fileSizeLimitedTo250Gb = LIMIT_FILESIZE_TO_250GB_DEFAULT;
    private int uploadBufferSize = UPLOAD_BUFFER_SIZE_DEFAULT;

    public Builder setFileSizeLimitedTo250Gb(boolean fileSizeLimitedTo250Gb) {
      this.fileSizeLimitedTo250Gb = fileSizeLimitedTo250Gb;
      return this;
    }

    public Builder setUploadBufferSize(int uploadBufferSize) {
      this.uploadBufferSize = uploadBufferSize;
      return this;
    }

    public AsyncWriteChannelOptions build() {
      return new AsyncWriteChannelOptions(fileSizeLimitedTo250Gb, uploadBufferSize);
    }
  }

  /**
   * Create a new builder with default values.
   */
  public static Builder newBuilder() {
    return new Builder();
  }


  private final boolean fileSizeLimitedTo250Gb;
  private final int uploadBufferSize;

  public AsyncWriteChannelOptions(boolean fileSizeLimitedTo250Gb,
      int uploadBufferSize) {
    this.fileSizeLimitedTo250Gb = fileSizeLimitedTo250Gb;
    this.uploadBufferSize = uploadBufferSize;
  }

  public boolean isFileSizeLimitedTo250Gb() {
    return fileSizeLimitedTo250Gb;
  }

  public int getUploadBufferSize() {
    return uploadBufferSize;
  }}
