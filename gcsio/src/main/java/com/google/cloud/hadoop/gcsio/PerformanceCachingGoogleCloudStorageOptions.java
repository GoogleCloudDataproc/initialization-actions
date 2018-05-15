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

/** Configurable options for {@link PerformanceCachingGoogleCloudStorage}. */
public class PerformanceCachingGoogleCloudStorageOptions {

  /** Max age of an item in cache in milliseconds. */
  public static final long MAX_ENTRY_AGE_MILLIS_DEFAULT = 3000L;

  /** Flag to enable list caching. */
  public static final boolean LIST_CACHING_ENABLED = true;

  private final long maxEntryAgeMillis;

  private final boolean inferImplicitDirectoriesEnabled;

  private final boolean listCachingEnabled;

  private PerformanceCachingGoogleCloudStorageOptions(Builder builder) {
    this.maxEntryAgeMillis = builder.maxEntryAgeMillis;
    this.inferImplicitDirectoriesEnabled = builder.inferImplicitDirectoriesEnabled;
    this.listCachingEnabled = builder.listCachingEnabled;
  }

  /** Gets the max age of an item in cache in milliseconds. */
  public long getMaxEntryAgeMillis() {
    return maxEntryAgeMillis;
  }

  /** Gets if implicit directories are inferred. */
  public boolean isInferImplicitDirectoriesEnabled() {
    return inferImplicitDirectoriesEnabled;
  }

  /** Gets if list caching is enabled. */
  public boolean isListCachingEnabled() {
    return listCachingEnabled;
  }

  /** Builder class for PerformanceCachingGoogleCloudStorageOptions. */
  public static class Builder {

    private long maxEntryAgeMillis = MAX_ENTRY_AGE_MILLIS_DEFAULT;

    private boolean inferImplicitDirectoriesEnabled =
        GoogleCloudStorageOptions.INFER_IMPLICIT_DIRECTORIES_DEFAULT;

    private boolean listCachingEnabled = LIST_CACHING_ENABLED;

    /** Sets the max age of an item in cache in milliseconds. */
    public Builder setMaxEntryAgeMillis(long maxEntryAgeMillis) {
      this.maxEntryAgeMillis = maxEntryAgeMillis;
      return this;
    }

    /** Setting for enabling inferring of implicit directories. */
    public Builder setInferImplicitDirectoriesEnabled(boolean inferImplicitDirectoriesEnabled) {
      this.inferImplicitDirectoriesEnabled = inferImplicitDirectoriesEnabled;
      return this;
    }

    /** Setting for list caching. */
    public Builder setListCachingEnabled(boolean listCachingEnabled) {
      this.listCachingEnabled = listCachingEnabled;
      return this;
    }

    public PerformanceCachingGoogleCloudStorageOptions build() {
      return new PerformanceCachingGoogleCloudStorageOptions(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
