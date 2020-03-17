/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import javax.annotation.Nullable;

/** Options that can be specified when creating a bucket in the {@code GoogleCloudStorage}. */
@AutoValue
public abstract class CreateBucketOptions {

  /** Create bucket with all default settings. */
  public static final CreateBucketOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_CreateBucketOptions.Builder();
  }

  public abstract Builder toBuilder();

  /** Returns the bucket location. */
  @Nullable
  public abstract String getLocation();

  /** Returns the bucket storage class. */
  @Nullable
  public abstract String getStorageClass();

  /** Returns the bucket retention period. */
  @Nullable
  public abstract Duration getTtl();

  /** Builder for {@link CreateBucketOptions} */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setLocation(String location);

    public abstract Builder setStorageClass(String storageClass);

    public abstract Builder setTtl(Duration ttl);

    public abstract CreateBucketOptions build();
  }
}
