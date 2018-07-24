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
        PerformanceCachingGoogleCloudStorageOptions.builder();
    private PerformanceCachingGoogleCloudStorageOptions immutablePerformanceCacheOptions = null;

    protected TimestampUpdatePredicate shouldIncludeInTimestampUpdatesPredicate =
        new TimestampUpdatePredicate() {
          @Override
          public boolean shouldUpdateTimestamp(URI item) {
            return true;
          }
        };

    private GoogleCloudStorageOptions.Builder cloudStorageOptionsBuilder =
        GoogleCloudStorageOptions.newBuilder();
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
      this.performanceCacheOptionsBuilder = PerformanceCachingGoogleCloudStorageOptions.builder();
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
      this.cloudStorageOptionsBuilder = GoogleCloudStorageOptions.newBuilder();
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
          shouldIncludeInTimestampUpdatesPredicate,
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
  private final TimestampUpdatePredicate shouldIncludeInTimestampUpdatesPredicate;
  private final PathCodec pathCodec;
  private final boolean enableBucketDelete;
  private final Pattern markerFilePattern;

  public GoogleCloudStorageFileSystemOptions(
      PerformanceCachingGoogleCloudStorageOptions performanceCacheOptions,
      boolean performanceCacheEnabled,
      GoogleCloudStorageOptions cloudStorageOptions,
      TimestampUpdatePredicate shouldIncludeInTimestampUpdatesPredicate,
      PathCodec pathCodec,
      boolean enableBucketDelete,
      String markerFilePattern) {
    this.performanceCacheOptions = performanceCacheOptions;
    this.performanceCacheEnabled = performanceCacheEnabled;
    this.cloudStorageOptions = cloudStorageOptions;
    this.shouldIncludeInTimestampUpdatesPredicate = shouldIncludeInTimestampUpdatesPredicate;
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

  public TimestampUpdatePredicate getShouldIncludeInTimestampUpdatesPredicate() {
    return shouldIncludeInTimestampUpdatesPredicate;
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
