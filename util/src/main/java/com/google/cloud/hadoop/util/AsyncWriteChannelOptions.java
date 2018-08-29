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

import com.google.auto.value.AutoValue;
import com.google.common.flogger.GoogleLogger;

/** Options for the {@link AbstractGoogleAsyncWriteChannel}. */
@AutoValue
public abstract class AsyncWriteChannelOptions {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** Default of whether to limit files to 250GB by default. */
  @Deprecated public static final boolean LIMIT_FILESIZE_TO_250GB_DEFAULT = false;

  /** Default upload buffer size. */
  public static final int BUFFER_SIZE_DEFAULT = 8 * 1024 * 1024;

  /** Default pipe buffer size. */
  public static final int PIPE_BUFFER_SIZE_DEFAULT = 1024 * 1024;

  /** Default upload chunk size. */
  public static final int UPLOAD_CHUNK_SIZE_DEFAULT = 64 * 1024 * 1024;

  @Deprecated public static final int UPLOAD_BUFFER_SIZE_DEFAULT = UPLOAD_CHUNK_SIZE_DEFAULT;

  /** Default of whether to use direct upload. */
  public static final boolean DIRECT_UPLOAD_ENABLED_DEFAULT = false;

  /** @deprecated use {@link #builder} */
  @Deprecated
  public static Builder newBuilder() {
    return builder();
  }

  public static Builder builder() {
    return new AutoValue_AsyncWriteChannelOptions.Builder()
        .setFileSizeLimitedTo250Gb(LIMIT_FILESIZE_TO_250GB_DEFAULT)
        .setBufferSize(BUFFER_SIZE_DEFAULT)
        .setPipeBufferSize(PIPE_BUFFER_SIZE_DEFAULT)
        .setUploadChunkSize(UPLOAD_CHUNK_SIZE_DEFAULT)
        .setDirectUploadEnabled(DIRECT_UPLOAD_ENABLED_DEFAULT);
  }

  /**
   * @deprecated {@code fileSizeLimitedTo250Gb} now defaults to false. It is deprecated and will
   *     soon be removed. Files greater than 250Gb are allowed by default.
   */
  @Deprecated
  public abstract boolean isFileSizeLimitedTo250Gb();

  public abstract int getBufferSize();

  public abstract int getPipeBufferSize();

  @Deprecated
  public int getUploadBufferSize() {
    return getUploadChunkSize();
  }

  public abstract int getUploadChunkSize();

  public abstract boolean isDirectUploadEnabled();

  /** Mutable builder for the GoogleCloudStorageWriteChannelOptions class. */
  @AutoValue.Builder
  public abstract static class Builder {

    @Deprecated
    public abstract Builder setFileSizeLimitedTo250Gb(boolean fileSizeLimitedTo250Gb);

    public abstract Builder setBufferSize(int bufferSize);

    public abstract Builder setPipeBufferSize(int pipeBufferSize);

    @Deprecated
    public Builder setUploadBufferSize(int uploadBufferSize) {
      return setUploadChunkSize(uploadBufferSize);
    }

    public abstract Builder setUploadChunkSize(int uploadChunkSize);

    public abstract Builder setDirectUploadEnabled(boolean directUploadEnabled);

    abstract AsyncWriteChannelOptions autoBuild();

    public AsyncWriteChannelOptions build() {
      AsyncWriteChannelOptions options = autoBuild();
      if (options.isFileSizeLimitedTo250Gb()) {
        logger.atWarning().log(
            "fileSizeLimitedTo250Gb now defaults to false. It is deprecated and will soon be "
                + "removed. Files greater than 250Gb are allowed by default.");
      }
      return options;
    }
  }
}
