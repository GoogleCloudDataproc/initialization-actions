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
    private boolean directoryTimestampUpdatingEnabled = true;

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

    public Builder setDirectoryTimestampUpdatingEnabled(boolean directoryTimestampUpdatingEnabled) {
      this.directoryTimestampUpdatingEnabled = directoryTimestampUpdatingEnabled;
      return this;
    }

    public GoogleCloudStorageFileSystemOptions build() {
      return new GoogleCloudStorageFileSystemOptions(
          cloudStorageOptionsBuilder.build(),
          metadataCacheEnabled,
          cacheType,
          cacheBasePath,
          directoryTimestampUpdatingEnabled);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final GoogleCloudStorageOptions cloudStorageOptions;
  private final boolean metadataCacheEnabled;
  private final DirectoryListCache.Type cacheType;
  private final String cacheBasePath;  // Only used if cacheType == LOCAL_FILE_BACKED.
  private final boolean directoryTimestampUpdatingEnabled;

  public GoogleCloudStorageFileSystemOptions(
      GoogleCloudStorageOptions cloudStorageOptions,
      boolean metadataCacheEnabled,
      DirectoryListCache.Type cacheType,
      String cacheBasePath,
      boolean directoryTimestampUpdatingEnabled) {
    this.cloudStorageOptions = cloudStorageOptions;
    this.metadataCacheEnabled = metadataCacheEnabled;
    this.cacheType = cacheType;
    this.cacheBasePath = cacheBasePath;
    this.directoryTimestampUpdatingEnabled = directoryTimestampUpdatingEnabled;
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

  public boolean isDirectoryTimestampUpdatingEnabled() {
    return directoryTimestampUpdatingEnabled;
  }

  public void throwIfNotValid() {
    cloudStorageOptions.throwIfNotValid();
  }
}
