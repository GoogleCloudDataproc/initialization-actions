/**
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

/** Configurable options for CachingGoogleCloudStorage. */
public class PerformanceCachingGoogleCloudStorageOptions {

  /** Max age of an item in cache in milliseconds. */
  public static final long MAX_ENTRY_AGE_MILLS_DEFAULT = 3000;

  private final long maxEntryAgeMills;

  private PerformanceCachingGoogleCloudStorageOptions(Builder builder) {
    this.maxEntryAgeMills = builder.maxEntryAgeMills;
  }

  /** Gets the max age of an item in cache in milliseconds. */
  public long getMaxEntryAge() {
    return maxEntryAgeMills;
  }

  /** Builder class for CachingGoogleCloudStorageOptions. */
  public static class Builder {

    private long maxEntryAgeMills = MAX_ENTRY_AGE_MILLS_DEFAULT;

    /** Sets the max age of an item in cache in milliseconds. */
    public Builder setMaxEntryAgeMills(long maxEntryAgeMills) {
      this.maxEntryAgeMills = maxEntryAgeMills;
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
