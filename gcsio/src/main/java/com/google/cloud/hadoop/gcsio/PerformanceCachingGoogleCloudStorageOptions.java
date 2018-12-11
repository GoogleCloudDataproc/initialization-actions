/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;

/** Configurable options for {@link PerformanceCachingGoogleCloudStorage}. */
@AutoValue
public abstract class PerformanceCachingGoogleCloudStorageOptions {

  /** Max age of an item in cache in milliseconds. */
  public static final long MAX_ENTRY_AGE_MILLIS_DEFAULT = 5_000;

  /** Flag to enable list caching. */
  public static final boolean LIST_CACHING_ENABLED = false;

  /** Number of prefetched objects metadata in directory. */
  @Deprecated public static final long DIR_METADATA_PREFETCH_LIMIT_DEFAULT = 1_000;

  public static final PerformanceCachingGoogleCloudStorageOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_PerformanceCachingGoogleCloudStorageOptions.Builder()
        .setMaxEntryAgeMillis(MAX_ENTRY_AGE_MILLIS_DEFAULT)
        .setListCachingEnabled(LIST_CACHING_ENABLED)
        .setDirMetadataPrefetchLimit(DIR_METADATA_PREFETCH_LIMIT_DEFAULT);
  }

  /** Gets the max age of an item in cache in milliseconds. */
  public abstract long getMaxEntryAgeMillis();

  /** Gets if list caching is enabled. */
  public abstract boolean isListCachingEnabled();

  /** Gets number if list caching is enabled. */
  @Deprecated
  public abstract long getDirMetadataPrefetchLimit();

  public abstract Builder toBuilder();

  /** Builder class for PerformanceCachingGoogleCloudStorageOptions. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the max age of an item in cache in milliseconds. */
    public abstract Builder setMaxEntryAgeMillis(long maxEntryAgeMillis);

    /** Setting for enabling list caching. */
    public abstract Builder setListCachingEnabled(boolean listCachingEnabled);

    /**
     * Sets number of prefetched objects metadata in directory. Setting this property to {@code 0}
     * disables metadata prefetching in directory. Setting it to {@code -1} prefetches all objects
     * metadata in a directory.
     */
    @Deprecated
    public abstract Builder setDirMetadataPrefetchLimit(long dirMetadataPrefetchLimit);

    abstract PerformanceCachingGoogleCloudStorageOptions autoBuild();

    public PerformanceCachingGoogleCloudStorageOptions build() {
      PerformanceCachingGoogleCloudStorageOptions options = autoBuild();
      checkState(
          options.getDirMetadataPrefetchLimit() >= -1,
          "dirMetadataPrefetchLimit should be greater or equal to -1");
      return options;
    }
  }
}
