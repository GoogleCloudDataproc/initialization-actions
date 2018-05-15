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

package com.google.cloud.hadoop.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Options for the GoogleCloudStorageWriteChannel.
 */
public class AsyncWriteChannelOptions {
  
  // Logger.
  private static final Logger LOG = LoggerFactory.getLogger(AsyncWriteChannelOptions.class);

  /**
   * Default of whether to limit files to 250GB by default.
   */
  public static final boolean LIMIT_FILESIZE_TO_250GB_DEFAULT = false;
  /**
   * Default upload buffer size.
   */
  public static final int UPLOAD_BUFFER_SIZE_DEFAULT = 64 * 1024 * 1024;

  /**
   * Default of whether to use direct upload.
   */
  public static final boolean DIRECT_UPLOAD_ENABLED_DEFAULT = false;

  /**
   * Mutable builder for the GoogleCloudStorageWriteChannelOptions class.
   */
  public static class Builder {
    private boolean fileSizeLimitedTo250Gb = LIMIT_FILESIZE_TO_250GB_DEFAULT;
    private int uploadBufferSize = UPLOAD_BUFFER_SIZE_DEFAULT;
    private boolean useDirectUpload = DIRECT_UPLOAD_ENABLED_DEFAULT;

    @Deprecated
    public Builder setFileSizeLimitedTo250Gb(boolean fileSizeLimitedTo250Gb) {
      this.fileSizeLimitedTo250Gb = fileSizeLimitedTo250Gb;
      return this;
    }

    public Builder setUploadBufferSize(int uploadBufferSize) {
      this.uploadBufferSize = uploadBufferSize;
      return this;
    }

    public Builder setDirectUploadEnabled(boolean useDirectUpload) {
      this.useDirectUpload = useDirectUpload;
      return this;
    }

    public AsyncWriteChannelOptions build() {
      return new AsyncWriteChannelOptions(
          fileSizeLimitedTo250Gb, uploadBufferSize, useDirectUpload);
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
  private final boolean directUploadEnabled;

  public AsyncWriteChannelOptions(boolean fileSizeLimitedTo250Gb,
      int uploadBufferSize,
      boolean useDirectUpload) {
    this.fileSizeLimitedTo250Gb = fileSizeLimitedTo250Gb;
    this.uploadBufferSize = uploadBufferSize;
    this.directUploadEnabled = useDirectUpload;
    if (fileSizeLimitedTo250Gb) {
      LOG.warn("fileSizeLimitedTo250Gb now defaults to false. It is deprecated and will soon be "
          + "removed. Files greater than 250Gb are allowed by default.");
    }
  }

  @Deprecated
  public boolean isFileSizeLimitedTo250Gb() {
    return fileSizeLimitedTo250Gb;
  }

  public int getUploadBufferSize() {
    return uploadBufferSize;
  }

  public boolean isDirectUploadEnabled() {
    return directUploadEnabled;
  }
}
