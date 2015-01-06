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

import com.google.api.client.util.DateTime;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.StorageObject;

import java.math.BigInteger;

/**
 * Misc helpers for generating test items.
 */
public class DirectoryListCacheTestUtils {
  // TODO(user): Move the createItemInfo* methods into GoogleCloudStorageItemInfo.java.

  public static final long BUCKET_BASE_CREATE_TIME = 1234L;
  public static final long OBJECT_BASE_UPDATED_TIME = 1234L;

  /**
   * Creates a GoogleCloudStorageItemInfo for the given {@code bucketName} with all its fields set
   * to some reasonable values.
   */
  public static GoogleCloudStorageItemInfo createBucketInfo(String bucketName) {
    Bucket fakeBucket = new Bucket()
        .setName(bucketName)
        .setTimeCreated(new DateTime(BUCKET_BASE_CREATE_TIME))
        .setLocation("us")
        .setStorageClass("class-af4");
    return GoogleCloudStorageImpl.createItemInfoForBucket(
        new StorageResourceId(bucketName), fakeBucket);
  }

  /**
   * Creates a GoogleCloudStorageItemInfo for the given StorageObject denoted by {@code bucketName}
   * and {@code objectName} with all its fields set to some reasonable values.
   */
  public static GoogleCloudStorageItemInfo createObjectInfo(String bucketName, String objectName) {
    StorageObject fakeStorageObject = new StorageObject()
        .setName(objectName)
        .setBucket(bucketName)
        .setUpdated(new DateTime(OBJECT_BASE_UPDATED_TIME))
        .setSize(BigInteger.valueOf(1024L * 1024 * 1024 * 1024))
        .setGeneration(1L)
        .setMetageneration(1L);
    return GoogleCloudStorageImpl.createItemInfoForStorageObject(
        new StorageResourceId(bucketName, objectName), fakeStorageObject);
  }
}
