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

import com.google.auto.value.AutoValue;
import java.net.URI;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/** Configurable options for the {@link GoogleCloudStorageFileSystem} class. */
@AutoValue
public abstract class GoogleCloudStorageFileSystemOptions {

  /** @deprecated use {@link #builder()} instead */
  @Deprecated
  public static Builder newBuilder() {
    return builder();
  }

  public static Builder builder() {
    return new AutoValue_GoogleCloudStorageFileSystemOptions.Builder()
        .setPerformanceCacheEnabled(false)
        .setPerformanceCacheOptions(PerformanceCachingGoogleCloudStorageOptions.DEFAULT)
        .setCloudStorageOptions(GoogleCloudStorageOptions.DEFAULT)
        .setShouldIncludeInTimestampUpdatesPredicate(i -> true)
        .setPathCodec(GoogleCloudStorageFileSystem.URI_ENCODED_PATH_CODEC)
        .setBucketDeleteEnabled(false)
        .setMarkerFilePattern((String) null)
        .setStatusParallelEnabled(false)
        .setCooperativeLockingEnabled(false);
  }

  public abstract boolean isPerformanceCacheEnabled();

  public abstract PerformanceCachingGoogleCloudStorageOptions getPerformanceCacheOptions();

  public abstract GoogleCloudStorageOptions getCloudStorageOptions();

  public abstract Predicate<URI> getShouldIncludeInTimestampUpdatesPredicate();

  public abstract PathCodec getPathCodec();

  public abstract boolean isBucketDeleteEnabled();

  @Nullable
  public abstract Pattern getMarkerFilePattern();

  public abstract boolean isStatusParallelEnabled();

  public abstract boolean isCooperativeLockingEnabled();

  public void throwIfNotValid() {
    getCloudStorageOptions().throwIfNotValid();
  }

  public abstract Builder toBuilder();

  /** Mutable builder for {@link GoogleCloudStorageFileSystemOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setPerformanceCacheEnabled(boolean performanceCacheEnabled);

    public abstract Builder setPerformanceCacheOptions(
        PerformanceCachingGoogleCloudStorageOptions options);

    public abstract Builder setCloudStorageOptions(GoogleCloudStorageOptions options);

    /**
     * Set a Predicate to be applied to item paths to determine if the item should have its
     * timestamps updated
     */
    public abstract Builder setShouldIncludeInTimestampUpdatesPredicate(Predicate<URI> predicate);

    public abstract Builder setPathCodec(PathCodec pathCodec);

    public abstract Builder setBucketDeleteEnabled(boolean bucketDeleteEnabled);

    abstract Builder setMarkerFilePattern(Pattern markerFilePattern);

    public Builder setMarkerFilePattern(String markerFilePattern) {
      return setMarkerFilePattern(
          markerFilePattern == null ? null : Pattern.compile("^(.+/)?" + markerFilePattern + "$"));
    }

    /**
     * Enables parallel execution of GCS requests in {@code listFileInfo} and {@code getFileInfo}
     * methods to reduce latency.
     */
    public abstract Builder setStatusParallelEnabled(boolean statusParallelEnabled);

    public abstract Builder setCooperativeLockingEnabled(boolean cooperativeLockingEnabled);

    public abstract GoogleCloudStorageFileSystemOptions build();
  }
}
