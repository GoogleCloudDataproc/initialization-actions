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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Predicate;
import java.net.URI;
import java.util.regex.Pattern;

/**
 * Configurable options for the GoogleCloudStorageFileSystem class.
 */
public class GoogleCloudStorageFileSystemOptions {

  /**
   * Callback interface that allows applications to control which directory timestamps should be
   * modified after entries are added or removed from the directory.
   * <p>
   * This is provided as a performance optimization as directory timestamps end up updating
   * object metadata which is rate-limited to 1QPS.
   */
  public interface TimestampUpdatePredicate {

    /**
     * Called to determine if the given URI should be updated.
     */
    boolean shouldUpdateTimestamp(URI item);
  }

  /**
   * Mutable builder for GoogleCloudStorageFileSystemOptions.
   */
  public static class Builder {
    private boolean performanceCacheEnabled = false;
    private PerformanceCachingGoogleCloudStorageOptions.Builder performanceCacheOptionsBuilder =
        PerformanceCachingGoogleCloudStorageOptions.newBuilder();
    private PerformanceCachingGoogleCloudStorageOptions immutablePerformanceCacheOptions = null;

    protected boolean metadataCacheEnabled = true;
    protected DirectoryListCache.Type cacheType = DirectoryListCache.Type.IN_MEMORY;
    protected String cacheBasePath = null;
    protected TimestampUpdatePredicate shouldIncludeInTimestampUpdatesPredicate =
        new TimestampUpdatePredicate() {
          @Override
          public boolean shouldUpdateTimestamp(URI item) {
            return true;
          }
        };

    protected long cacheMaxEntryAgeMillis = DirectoryListCache.Config.MAX_ENTRY_AGE_MILLIS_DEFAULT;
    protected long cacheMaxInfoAgeMillis = DirectoryListCache.Config.MAX_INFO_AGE_MILLIS_DEFAULT;

    private GoogleCloudStorageOptions.Builder cloudStorageOptionsBuilder =
        new GoogleCloudStorageOptions.Builder();
    private GoogleCloudStorageOptions immutableCloudStorageOptions = null;

    private PathCodec pathCodec = GoogleCloudStorageFileSystem.LEGACY_PATH_CODEC;
    private boolean enableBucketDelete = false;
    private String markerFilePattern = null;

    public Builder setIsPerformanceCacheEnabled(boolean performanceCacheEnabled) {
      this.performanceCacheEnabled = performanceCacheEnabled;
      return this;
    }

    /**
     * Gets the stored {@link PerformanceCachingGoogleCloudStorageOptions.Builder}. Note: the
     * returned builder isn't referenced if an immutable PerformanceCachingOptions instance was set
     * previously using {@link
     * Builder#setImmutablePerformanceCachingOptions(PerformanceCachingGoogleCloudStorageOptions)}.
     */
    public PerformanceCachingGoogleCloudStorageOptions.Builder
        getPerformanceCachingOptionsBuilder() {
      return performanceCacheOptionsBuilder;
    }

    /**
     * Mutually exclusive with setImmutablePerformanceCachingOptions; if setting this builder, then
     * any subsequent changes made to the inner PerformanceCaching options builder will be reflected
     * at the time PerformanceCachingGoogleCloudStorageOptions.Builder.build() is called. If this is
     * called after calling setImmutablePerformanceCachingOptions, then the previous value of the
     * immutablePerformanceCacheOptions is discarded, in the same way setting other single-element
     * fields is overridden by the last call to the builder.
     */
    public Builder setPerformanceCachingOptionsBuilder(
        PerformanceCachingGoogleCloudStorageOptions.Builder performanceCacheOptionsBuilder) {
      this.performanceCacheOptionsBuilder = performanceCacheOptionsBuilder;
      this.immutablePerformanceCacheOptions = null;
      return this;
    }

    /**
     * Mutually exclusive with setPerformanceCachingOptionsBuilder If this is called after calling
     * setPerformanceCachingOptionsBuilder, then the previous value of the
     * performanceCacheOptionsBuilder is discarded, in the same way setting other single-element
     * fields is overridden by the last call to the builder.
     */
    public Builder setImmutablePerformanceCachingOptions(
        PerformanceCachingGoogleCloudStorageOptions immutablePerformanceCacheOptions) {
      this.immutablePerformanceCacheOptions = immutablePerformanceCacheOptions;
      this.performanceCacheOptionsBuilder =
          PerformanceCachingGoogleCloudStorageOptions.newBuilder();
      return this;
    }

    /**
     * Gets the stored {@link GoogleCloudStorageOptions.Builder}. Note: the returned builder isn't
     * referenced if an immutable GoogleCloudStorageOptions instance was set previously using {@link
     * Builder#setImmutableCloudStorageOptions(GoogleCloudStorageOptions)}.
     */
    public GoogleCloudStorageOptions.Builder getCloudStorageOptionsBuilder() {
      return cloudStorageOptionsBuilder;
    }

    /**
     * Mutually exclusive with setImmutableCloudStorageOptions; if setting this builder, then
     * any subsequent changes made to the inner GCS options builder will be reflected at the
     * time GoogleCloudStorageFileSystemOptions.Builder.build() is called. If this is called
     * after calling setImmutableCloudStorageOptions, then the previous value of the
     * immutabelCloudStorageOptions is discarded, in the same way setting other single-element
     * fields is overridden by the last call to the builder.
     */
    public Builder setCloudStorageOptionsBuilder(
        GoogleCloudStorageOptions.Builder cloudStorageOptionsBuilder) {
      this.cloudStorageOptionsBuilder = cloudStorageOptionsBuilder;
      this.immutableCloudStorageOptions = null;
      return this;
    }

    /**
     * Mutually exclusive with setCloudStorageOptionsBuilder If this is called
     * after calling setCloudStorageOptionsBuilder, then the previous value of the
     * cloudStorageOptionsBuilder is discarded, in the same way setting other single-element
     * fields is overridden by the last call to the builder.
     */
    public Builder setImmutableCloudStorageOptions(
        GoogleCloudStorageOptions immutableCloudStorageOptions) {
      this.immutableCloudStorageOptions = immutableCloudStorageOptions;
      this.cloudStorageOptionsBuilder = new GoogleCloudStorageOptions.Builder();
      return this;
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

    /** Set a Predicate to be applied to item paths to determine if the item should
     * have its timestamps updated */
    public Builder setShouldIncludeInTimestampUpdatesPredicate(
        final Predicate<String> shouldIncludeInTimestampUpdatesPredicate) {
      this.shouldIncludeInTimestampUpdatesPredicate = new TimestampUpdatePredicate() {
        @Override
        public boolean shouldUpdateTimestamp(URI item) {
          return shouldIncludeInTimestampUpdatesPredicate.apply(item.getPath());
        }
      };
      return this;
    }

    public Builder setShouldIncludeInTimestampUpdatesPredicate(
        TimestampUpdatePredicate shouldIncludeInTimestampUpdatesPredicate) {
      this.shouldIncludeInTimestampUpdatesPredicate = shouldIncludeInTimestampUpdatesPredicate;
      return this;
    }

    public Builder setCacheMaxEntryAgeMillis(long cacheMaxEntryAgeMillis) {
      this.cacheMaxEntryAgeMillis = cacheMaxEntryAgeMillis;
      return this;
    }

    public Builder setCacheMaxInfoAgeMillis(long cacheMaxInfoAgeMillis) {
      this.cacheMaxInfoAgeMillis = cacheMaxInfoAgeMillis;
      return this;
    }

    public Builder setPathCodec(PathCodec pathCodec) {
      this.pathCodec = pathCodec;
      return this;
    }

    public Builder setEnableBucketDelete(boolean enableBucketDelete) {
      this.enableBucketDelete = enableBucketDelete;
      return this;
    }

    public Builder setMarkerFilePattern(String markerFilePattern) {
      this.markerFilePattern = markerFilePattern;
      return this;
    }

    public GoogleCloudStorageFileSystemOptions build() {
      return new GoogleCloudStorageFileSystemOptions(
          immutablePerformanceCacheOptions != null
              ? immutablePerformanceCacheOptions
              : performanceCacheOptionsBuilder.build(),
          performanceCacheEnabled,
          immutableCloudStorageOptions != null
              ? immutableCloudStorageOptions
              : cloudStorageOptionsBuilder.build(),
          metadataCacheEnabled,
          cacheType,
          cacheBasePath,
          shouldIncludeInTimestampUpdatesPredicate,
          cacheMaxEntryAgeMillis,
          cacheMaxInfoAgeMillis,
          pathCodec,
          enableBucketDelete,
          markerFilePattern);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final PerformanceCachingGoogleCloudStorageOptions performanceCacheOptions;
  private final boolean performanceCacheEnabled;
  private final GoogleCloudStorageOptions cloudStorageOptions;
  private final boolean metadataCacheEnabled;
  private final DirectoryListCache.Type cacheType;
  private final String cacheBasePath;  // Only used if cacheType == FILESYSTEM_BACKED.
  private final TimestampUpdatePredicate shouldIncludeInTimestampUpdatesPredicate;
  private final long cacheMaxEntryAgeMillis;
  private final long cacheMaxInfoAgeMillis;
  private final PathCodec pathCodec;
  private final boolean enableBucketDelete;
  private final Pattern markerFilePattern;

  public GoogleCloudStorageFileSystemOptions(
      PerformanceCachingGoogleCloudStorageOptions performanceCacheOptions,
      boolean performanceCacheEnabled,
      GoogleCloudStorageOptions cloudStorageOptions,
      boolean metadataCacheEnabled,
      DirectoryListCache.Type cacheType,
      String cacheBasePath,
      TimestampUpdatePredicate shouldIncludeInTimestampUpdatesPredicate,
      long cacheMaxEntryAgeMillis,
      long cacheMaxInfoAgeMillis,
      PathCodec pathCodec,
      boolean enableBucketDelete,
      String markerFilePattern) {
    this.performanceCacheOptions = performanceCacheOptions;
    this.performanceCacheEnabled = performanceCacheEnabled;
    this.cloudStorageOptions = cloudStorageOptions;
    this.metadataCacheEnabled = metadataCacheEnabled;
    this.cacheType = cacheType;
    this.cacheBasePath = cacheBasePath;
    this.shouldIncludeInTimestampUpdatesPredicate = shouldIncludeInTimestampUpdatesPredicate;
    this.cacheMaxEntryAgeMillis = cacheMaxEntryAgeMillis;
    this.cacheMaxInfoAgeMillis = cacheMaxInfoAgeMillis;
    this.pathCodec = pathCodec;
    this.enableBucketDelete = enableBucketDelete;
    this.markerFilePattern = Pattern.compile("^(.+/)?" + markerFilePattern + "$");
  }

  public PerformanceCachingGoogleCloudStorageOptions getPerformanceCacheOptions() {
    return performanceCacheOptions;
  }

  public boolean isPerformanceCacheEnabled() {
    return performanceCacheEnabled;
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

  public TimestampUpdatePredicate getShouldIncludeInTimestampUpdatesPredicate() {
    return shouldIncludeInTimestampUpdatesPredicate;
  }

  public long getCacheMaxEntryAgeMillis() {
    return cacheMaxEntryAgeMillis;
  }

  public long getCacheMaxInfoAgeMillis() {
    return cacheMaxInfoAgeMillis;
  }

  public PathCodec getPathCodec() {
    return pathCodec;
  }

  public boolean enableBucketDelete() {
    return enableBucketDelete;
  }

  public Pattern getMarkerFilePattern() {
    return markerFilePattern;
  }

  public void throwIfNotValid() {
    checkNotNull(
        shouldIncludeInTimestampUpdatesPredicate,
        "Predicate for ignored directory updates should not be null."
            + " Consider Predicates.alwasyTrue");
    cloudStorageOptions.throwIfNotValid();
  }
}
