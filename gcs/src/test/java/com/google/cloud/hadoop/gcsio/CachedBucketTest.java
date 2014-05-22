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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.testing.ExceptionUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * UnitTests for CachedBucket class.
 */
@RunWith(JUnit4.class)
public class CachedBucketTest {
  private Clock mockClock;
  private StorageResourceId bucketResourceId;
  private StorageResourceId objectResourceId;
  private GoogleCloudStorageItemInfo bucketInfo;
  private GoogleCloudStorageItemInfo objectInfo;

  @Before
  public void setUp() {
    mockClock = mock(Clock.class);

    CacheEntry.setClock(mockClock);

    bucketInfo = DirectoryListCacheTestUtils.createBucketInfo("foo-bucket");
    bucketResourceId = bucketInfo.getResourceId();
    objectInfo = DirectoryListCacheTestUtils.createObjectInfo("foo-bucket", "bar-object");
    objectResourceId = objectInfo.getResourceId();
  }

  @Test
  public void testConstructorBucketNameInvalidArgs() {
    // Disallow null bucketName.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CachedBucket.class, "<init>", String.class);

    // Disallow empty bucketName.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CachedBucket.class, "<init>", "");
  }

  @Test
  public void testConstructorBucketInfoInvalidArgs() {
    // Disallow null.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CachedBucket.class, "<init>", GoogleCloudStorageItemInfo.class);

    // Disallow root.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CachedBucket.class, "<init>", GoogleCloudStorageItemInfo.ROOT_INFO);

    // Disallow nonexistent bucket.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CachedBucket.class, "<init>",
        GoogleCloudStorageImpl.createItemInfoForNotFound(bucketResourceId));

    // Disallow StorageObject.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CachedBucket.class, "<init>", objectInfo);
  }

  @Test
  public void testInvalidResourceIdForInnerObjects() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());

    // Disallow null.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "get", StorageResourceId.class);
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "remove", StorageResourceId.class);
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "put", StorageResourceId.class);

    // Disallow root.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "get", StorageResourceId.ROOT);
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "remove", StorageResourceId.ROOT);
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "put", StorageResourceId.ROOT);

    // Disallow buckets (for inner objects).
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "get", bucketResourceId);
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "remove", bucketResourceId);
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "put", bucketResourceId);

    // Disallow objects whose bucket doesn't the CachedBucket's bucketName.
    StorageResourceId invalidObjectId =
        DirectoryListCacheTestUtils.createObjectInfo("other-bucket", "bar-object").getResourceId();
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "get", invalidObjectId);
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "remove", invalidObjectId);
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        bucket, "put", invalidObjectId);
  }

  /**
   * Helper for verifying basic interactions with a CachedBucket after it has had its bucket-level
   * info set already.
   */
  private void validateBasicInteractions(StorageResourceId resourceId,
      GoogleCloudStorageItemInfo itemInfo, long creationTime, long lastUpdatedTime,
      CachedBucket bucket) {
    // Info on the bucket itself.
    assertEquals(resourceId.getBucketName(), bucket.getName());
    assertEquals(creationTime, bucket.getCreationTimeMillis());
    assertEquals(lastUpdatedTime, bucket.getItemInfoUpdateTimeMillis());
    assertEquals(itemInfo, bucket.getItemInfo());

    // Even when empty, getObjecList() doesn't return null.
    assertEquals(0, bucket.getNumObjects());
    assertNotNull(bucket.getObjectList());

    // It's fine to get/remove an non-existent StorageObject.
    assertNull(bucket.get(objectResourceId));
    bucket.remove(objectResourceId);

    // Now add a StorageObject within the bucket.
    long addTime = 43L;
    when(mockClock.currentTimeMillis()).thenReturn(addTime);
    bucket.put(objectResourceId);
    CacheEntry objectEntry = bucket.get(objectResourceId);
    assertNotNull(objectEntry);

    // Check the added object's CacheEntry info.
    assertEquals(addTime, objectEntry.getCreationTimeMillis());
    assertEquals(0, objectEntry.getItemInfoUpdateTimeMillis());
    assertNull(objectEntry.getItemInfo());
    assertEquals(1, bucket.getObjectList().size());
    assertEquals(1, bucket.getNumObjects());
    assertEquals(objectEntry, bucket.getObjectList().get(0));

    // Populate the object's info.
    long objectUpdateTime = 67L;
    when(mockClock.currentTimeMillis()).thenReturn(objectUpdateTime);
    assertTrue(objectUpdateTime != addTime);
    objectEntry.setItemInfo(objectInfo);
    assertEquals(addTime, objectEntry.getCreationTimeMillis());
    assertEquals(objectUpdateTime, objectEntry.getItemInfoUpdateTimeMillis());
    assertEquals(objectInfo, objectEntry.getItemInfo());

    // Adding the same thing doesn't modify the existing cache entry at all or invalidate
    // the entry's info. NB: This will change if we change the behavior to invalidate any existing
    // entry's info on a second add.
    CacheEntry returnedEntry = bucket.put(objectResourceId);
    objectEntry = bucket.get(objectResourceId);

    // Check for true reference equality, not just ".equals()"; we expect the CacheEntry to really
    // be the exact same object.
    assertTrue(returnedEntry == objectEntry);
    assertEquals(addTime, objectEntry.getCreationTimeMillis());
    assertEquals(objectUpdateTime, objectEntry.getItemInfoUpdateTimeMillis());
    assertEquals(objectInfo, objectEntry.getItemInfo());

    // Make sure the returned list doesn't start giving us duplicate entries.
    assertEquals(1, bucket.getObjectList().size());
    assertEquals(1, bucket.getNumObjects());

    // Now remove the object.
    bucket.remove(objectResourceId);
    assertEquals(0, bucket.getObjectList().size());
    assertEquals(0, bucket.getNumObjects());
  }

  @Test
  public void testBasicInfoFromResourceId() {
    long constructorTime = 1003L;
    when(mockClock.currentTimeMillis()).thenReturn(constructorTime);
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());

    assertEquals(bucketResourceId.getBucketName(), bucket.getName());
    assertEquals(0, bucket.getNumObjects());

    // We update the CacheEntry directly; it will be reflected in the actual CacheEntry instance
    // such that future calls to bucket.getItemInfo() will reflect the new data.
    long updateTime = 1010L;
    when(mockClock.currentTimeMillis()).thenReturn(updateTime);
    assertTrue(constructorTime != updateTime);
    bucket.setItemInfo(bucketInfo);

    validateBasicInteractions(bucketResourceId, bucketInfo, constructorTime, updateTime, bucket);
  }

  @Test
  public void testBasicInfoFromItemInfo() {
    long constructorTime = 1515L;
    when(mockClock.currentTimeMillis()).thenReturn(constructorTime);

    CachedBucket bucket = new CachedBucket(bucketInfo);
    validateBasicInteractions(
        bucketResourceId, bucketInfo, constructorTime, constructorTime, bucket);
  }
}
