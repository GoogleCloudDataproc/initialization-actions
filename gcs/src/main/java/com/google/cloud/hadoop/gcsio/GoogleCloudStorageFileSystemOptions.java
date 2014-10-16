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

package com.google.cloud.hadoop.gcsio;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;


/**
 * Configurable options for the GoogleCloudStorageFileSystem class.
 */
public class GoogleCloudStorageFileSystemOptions {

  /**
   * Mutable builder for GoogleCloudStorageFileSystemOptions.
   */
  public static class Builder {
    protected boolean metadataCacheEnabled = true;
    protected DirectoryListCache.Type cacheType = DirectoryListCache.Type.IN_MEMORY;
    protected String cacheBasePath = null;
    protected Predicate<String> shouldIncludeInTimestampUpdatesPredicate = Predicates.alwaysTrue();

    private GoogleCloudStorageOptions.Builder cloudStorageOptionsBuilder =
        new GoogleCloudStorageOptions.Builder();

    public GoogleCloudStorageOptions.Builder getCloudStorageOptionsBuilder() {
      return cloudStorageOptionsBuilder;
    }

    public Builder setIsMetadataCacheEnabled(boolean isMetadataCacheEnabled) {
      this.metadataCacheEnabled = isMetadataCacheEnabled;
      return this;
    }

    public Builder setCacheType(DirectoryListCache.Type cacheType) {
      this.cacheType = cacheType;
      return this;
    }

    public Builder setCacheBasePath(String cacheBasePath) {
      this.cacheBasePath = cacheBasePath;
      return this;
    }

    public Builder setShouldIncludeInTimestampUpdatesPredicate(
        Predicate<String> shouldIncludeInTimestampUpdatesPredicate) {
      this.shouldIncludeInTimestampUpdatesPredicate = shouldIncludeInTimestampUpdatesPredicate;
      return this;
    }

    public GoogleCloudStorageFileSystemOptions build() {
      return new GoogleCloudStorageFileSystemOptions(
          cloudStorageOptionsBuilder.build(),
          metadataCacheEnabled,
          cacheType,
          cacheBasePath,
          shouldIncludeInTimestampUpdatesPredicate);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final GoogleCloudStorageOptions cloudStorageOptions;
  private final boolean metadataCacheEnabled;
  private final DirectoryListCache.Type cacheType;
  private final String cacheBasePath;  // Only used if cacheType == FILESYSTEM_BACKED.
  private final Predicate<String> shouldIncludeInTimestampUpdatesPredicate;

  public GoogleCloudStorageFileSystemOptions(
      GoogleCloudStorageOptions cloudStorageOptions,
      boolean metadataCacheEnabled,
      DirectoryListCache.Type cacheType,
      String cacheBasePath,
      Predicate<String> shouldIncludeInTimestampUpdatesPredicate) {
    this.cloudStorageOptions = cloudStorageOptions;
    this.metadataCacheEnabled = metadataCacheEnabled;
    this.cacheType = cacheType;
    this.cacheBasePath = cacheBasePath;
    this.shouldIncludeInTimestampUpdatesPredicate = shouldIncludeInTimestampUpdatesPredicate;
  }

  public GoogleCloudStorageOptions getCloudStorageOptions() {
    return cloudStorageOptions;
  }

  public boolean isMetadataCacheEnabled() {
    return metadataCacheEnabled;
  }

  public DirectoryListCache.Type getCacheType() {
    return cacheType;
  }

  public String getCacheBasePath() {
    return cacheBasePath;
  }

  public Predicate<String> getShouldIncludeInTimestampUpdatesPredicate() {
    return shouldIncludeInTimestampUpdatesPredicate;
  }

  public void throwIfNotValid() {
    Preconditions.checkArgument(
        shouldIncludeInTimestampUpdatesPredicate != null,
        "Predicate for ignored directory updates should not be null. "
            + "Consider Predicates.alwasyTrue");
    cloudStorageOptions.throwIfNotValid();
  }
}
