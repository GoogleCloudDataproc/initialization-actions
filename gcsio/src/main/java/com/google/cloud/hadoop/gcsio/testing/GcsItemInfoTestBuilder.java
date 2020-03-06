/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio.testing;

import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.VerificationAttributes;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Builder used in tests for {@link GoogleCloudStorageItemInfo} class */
@AutoValue
public abstract class GcsItemInfoTestBuilder {

  public static Builder create() {
    return new AutoValue_GcsItemInfoTestBuilder.Builder()
        .setStorageResourceId(new StorageResourceId("foo-test-bucket", "bar/test/object"))
        .setCreationTime(System.currentTimeMillis())
        .setModificationTime(System.currentTimeMillis())
        .setSize(12_456)
        .setLocation("us-central-1")
        .setStorageClass("standard")
        .setContentType("application/octet-stream")
        .setContentEncoding(null)
        .setMetadata(ImmutableMap.of("foo-test-meta", new byte[] {8, 33}))
        .setContentGeneration(821741945)
        .setMetaGeneration(3)
        .setVerificationAttributes(new VerificationAttributes(null, null));
  }

  public abstract StorageResourceId getStorageResourceId();

  public abstract long getCreationTime();

  public abstract long getModificationTime();

  public abstract long getSize();

  @Nullable
  public abstract String getLocation();

  @Nullable
  public abstract String getStorageClass();

  @Nullable
  public abstract String getContentType();

  @Nullable
  public abstract String getContentEncoding();

  public abstract Map<String, byte[]> getMetadata();

  public abstract long getContentGeneration();

  public abstract long getMetaGeneration();

  public abstract VerificationAttributes getVerificationAttributes();

  /** Mutable builder for the GoogleCloudStorageItemInfo class. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setStorageResourceId(StorageResourceId storageResourceId);

    public abstract Builder setCreationTime(long creationTime);

    public abstract Builder setModificationTime(long modificationTime);

    public abstract Builder setSize(long size);

    public abstract Builder setLocation(String location);

    public abstract Builder setStorageClass(String storageClass);

    public abstract Builder setContentType(String contentType);

    public abstract Builder setContentEncoding(String contentEncoding);

    public abstract Builder setMetadata(Map<String, byte[]> metadata);

    public abstract Builder setContentGeneration(long contentGeneration);

    public abstract Builder setMetaGeneration(long metaGeneration);

    public abstract Builder setVerificationAttributes(
        VerificationAttributes verificationAttributes);

    abstract GcsItemInfoTestBuilder autoBuild();

    public GoogleCloudStorageItemInfo build() {
      GcsItemInfoTestBuilder instance = autoBuild();
      return new GoogleCloudStorageItemInfo(
          instance.getStorageResourceId(),
          instance.getCreationTime(),
          instance.getModificationTime(),
          instance.getSize(),
          instance.getLocation(),
          instance.getStorageClass(),
          instance.getContentType(),
          instance.getContentEncoding(),
          instance.getMetadata(),
          instance.getContentGeneration(),
          instance.getMetaGeneration(),
          instance.getVerificationAttributes());
    }
  }
}
