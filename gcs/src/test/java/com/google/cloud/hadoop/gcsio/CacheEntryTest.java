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
 * UnitTests for CacheEntry class.
 */
@RunWith(JUnit4.class)
public class CacheEntryTest {
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
  public void testConstructorResourceIdInvalidArgs() {
    // Disallow null.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CacheEntry.class, "<init>", StorageResourceId.class);

    // Disallow root.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CacheEntry.class, "<init>", StorageResourceId.ROOT);
  }

  @Test
  public void testConstructorItemInfoInvalidArgs() {
    // Disallow null.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CacheEntry.class, "<init>", GoogleCloudStorageItemInfo.class);

    // Disallow root.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CacheEntry.class, "<init>", GoogleCloudStorageItemInfo.ROOT_INFO);

    // Disallow nonexistent bucket and object.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CacheEntry.class, "<init>",
        GoogleCloudStorageImpl.createItemInfoForNotFound(bucketResourceId));
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        CacheEntry.class, "<init>",
        GoogleCloudStorageImpl.createItemInfoForNotFound(objectResourceId));
  }

  /**
   * Helper for verifying the values of the provided resourceId, itemInfo, creationTime, and
   * lastUpdatedTime once the itemInfo has been properly set in the CacheEntry. Also verifies
   * the stickiness of the StorageResourceId and creationTime in the face of clearItemInfo()
   * calls.
   */
  private void validateBasicInteractions(StorageResourceId resourceId,
      GoogleCloudStorageItemInfo itemInfo, long creationTime, long lastUpdatedTime,
      CacheEntry entry) {
    assertEquals(resourceId, entry.getResourceId());

    // The 'creationTimeMillis' of the CacheEntry is the time it was constructed, *not* the
    // lastUpdated or timeCreated of the GoogleCloudStorageItemInfo within.
    assertEquals(creationTime, entry.getCreationTimeMillis());
    assertEquals(lastUpdatedTime, entry.getItemInfoUpdateTimeMillis());
    assertEquals(itemInfo, entry.getItemInfo());

    // Re-set the itemInfo without changing it.
    long newUpdateTime = lastUpdatedTime + 1010;
    when(mockClock.currentTimeMillis()).thenReturn(newUpdateTime);
    assertTrue(lastUpdatedTime != newUpdateTime);
    entry.setItemInfo(itemInfo);

    // No change in creationTime.
    assertEquals(creationTime, entry.getCreationTimeMillis());
    assertEquals(newUpdateTime, entry.getItemInfoUpdateTimeMillis());
    assertEquals(itemInfo, entry.getItemInfo());

    // After clearing itemInfo, the StorageResourceId should still remain inside and creationTime
    // doesn't change.
    entry.clearItemInfo();
    assertNull(entry.getItemInfo());
    assertEquals(creationTime, entry.getCreationTimeMillis());
    assertEquals(0, entry.getItemInfoUpdateTimeMillis());
    assertEquals(resourceId, entry.getResourceId());
  }

  @Test
  public void testBasicInfoFromBucketResourceId() {
    long constructorTime = 10L;
    when(mockClock.currentTimeMillis()).thenReturn(constructorTime);
    CacheEntry entry = new CacheEntry(bucketResourceId);

    assertEquals(bucketResourceId, entry.getResourceId());
    assertEquals(constructorTime, entry.getCreationTimeMillis());
    assertEquals(0, entry.getItemInfoUpdateTimeMillis());
    assertNull(entry.getItemInfo());

    long updateTime = 20L;
    when(mockClock.currentTimeMillis()).thenReturn(updateTime);
    entry.setItemInfo(bucketInfo);

    validateBasicInteractions(bucketResourceId, bucketInfo, constructorTime, updateTime, entry);
  }

  @Test
  public void testBasicInfoFromBucketItemInfo() {
    long constructorTime = 15L;
    when(mockClock.currentTimeMillis()).thenReturn(constructorTime);
    CacheEntry entry = new CacheEntry(bucketInfo);
    validateBasicInteractions(
        bucketResourceId, bucketInfo, constructorTime, constructorTime, entry);
  }

  @Test
  public void testBasicInfoFromObjectResourceId() {
    long constructorTime = 21L;
    when(mockClock.currentTimeMillis()).thenReturn(constructorTime);
    CacheEntry entry = new CacheEntry(objectResourceId);

    assertEquals(objectResourceId, entry.getResourceId());
    assertEquals(constructorTime, entry.getCreationTimeMillis());
    assertEquals(0, entry.getItemInfoUpdateTimeMillis());
    assertNull(entry.getItemInfo());

    long updateTime = 31L;
    when(mockClock.currentTimeMillis()).thenReturn(updateTime);
    entry.setItemInfo(objectInfo);

    validateBasicInteractions(objectResourceId, objectInfo, constructorTime, updateTime, entry);
  }

  @Test
  public void testBasicInfoFromObjectItemInfo() {
    long constructorTime = 51L;
    when(mockClock.currentTimeMillis()).thenReturn(constructorTime);
    CacheEntry entry = new CacheEntry(bucketInfo);

    validateBasicInteractions(
        bucketResourceId, bucketInfo, constructorTime, constructorTime, entry);
  }

  @Test
  public void testSetItemInfoInvalidArgs() {
    CacheEntry entry = new CacheEntry(bucketInfo);

    // Disallow null.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        entry, "setItemInfo", GoogleCloudStorageItemInfo.class);

    // Disallow root.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        entry, "setItemInfo", GoogleCloudStorageItemInfo.ROOT_INFO);

    // Disallow not found.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        entry, "setItemInfo", GoogleCloudStorageImpl.createItemInfoForNotFound(bucketResourceId));

    // Disallow info with resourceId != existing resourceId.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        entry, "setItemInfo", objectInfo);
  }
}
