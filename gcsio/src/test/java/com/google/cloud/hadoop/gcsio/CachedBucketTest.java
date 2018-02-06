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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Clock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * UnitTests for CachedBucket class.
 */
@RunWith(JUnit4.class)
public class CachedBucketTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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
  public void testConstructorThrowsWhenBucketNameIsNull() {
    expectedException.expect(IllegalArgumentException.class);
    new CachedBucket((String) null);
  }

  @Test
  public void testConstructorThrowsWhenBucketNameIsEmpty() {
    expectedException.expect(IllegalArgumentException.class);
    new CachedBucket("");
  }

  @Test
  public void testConstructorThrowsWhenStorageItemInfoIsNull() {
    expectedException.expect(IllegalArgumentException.class);
    new CachedBucket((GoogleCloudStorageItemInfo) null);
  }

  @Test
  public void testConstructorThrowsWhenStorageItemInfoIsRoot() {
    expectedException.expect(IllegalArgumentException.class);
    new CachedBucket(GoogleCloudStorageItemInfo.ROOT_INFO);
  }

  @Test
  public void testConstructorThrowsWhenNonExistentBucket() {
    expectedException.expect(IllegalArgumentException.class);
    new CachedBucket(GoogleCloudStorageImpl.createItemInfoForNotFound(bucketResourceId));
  }

  @Test
  public void testConstructorThrowsWhenStorageItemInfoIsStorageObject() {
    expectedException.expect(IllegalArgumentException.class);
    new CachedBucket(objectInfo);
  }

  @Test
  public void testGetThrowsWhenStorageResourceIsNull() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    expectedException.expect(IllegalArgumentException.class);
    bucket.get(null);
  }

  @Test
  public void testRemoveThrowsWhenStorageResourceIsNull() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    expectedException.expect(IllegalArgumentException.class);
    bucket.remove(null);
  }

  @Test
  public void testPutThrowsWhenStorageResourceIsNull() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    expectedException.expect(IllegalArgumentException.class);
    bucket.put(null);
  }

  @Test
  public void testGetThrowsWhenStorageResourceIsRoot() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    expectedException.expect(IllegalArgumentException.class);
    bucket.get(StorageResourceId.ROOT);
  }

  @Test
  public void testRemoveThrowsWhenStorageResourceIsRoot() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    expectedException.expect(IllegalArgumentException.class);
    bucket.remove(StorageResourceId.ROOT);
  }

  @Test
  public void testPutThrowsWhenStorageResourceIsRoot() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    expectedException.expect(IllegalArgumentException.class);
    bucket.put(StorageResourceId.ROOT);
  }

  @Test
  public void testGetThrowsWhenStorageResourceIdIsABucket() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    expectedException.expect(IllegalArgumentException.class);
    bucket.get(bucketResourceId);
  }

  @Test
  public void testRemoveThrowsWhenStorageResourceIdIsABucket() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    expectedException.expect(IllegalArgumentException.class);
    bucket.remove(bucketResourceId);
  }

  @Test
  public void testPutThrowsWhenStorageResourceIdIsABucket() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    expectedException.expect(IllegalArgumentException.class);
    bucket.put(bucketResourceId);
  }

  @Test
  public void testGetThrowsWhenStorageResourceIsInDifferentBucket() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());

    // Disallow objects whose bucket doesn't match the CachedBucket's bucketName.
    StorageResourceId invalidObjectId =
        DirectoryListCacheTestUtils.createObjectInfo("other-bucket", "bar-object").getResourceId();
    expectedException.expect(IllegalArgumentException.class);
    bucket.get(invalidObjectId);
  }

  @Test
  public void testRemoveThrowsWhenStorageResourceIsInDifferentBucket() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());

    // Disallow objects whose bucket doesn't match the CachedBucket's bucketName.
    StorageResourceId invalidObjectId =
        DirectoryListCacheTestUtils.createObjectInfo("other-bucket", "bar-object").getResourceId();
    expectedException.expect(IllegalArgumentException.class);
    bucket.remove(invalidObjectId);
  }

  @Test
  public void testPutThrowsWhenStorageResourceIsInDifferentBucket() {
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());
    // Disallow objects whose bucket doesn't match the CachedBucket's bucketName.
    StorageResourceId invalidObjectId =
        DirectoryListCacheTestUtils.createObjectInfo("other-bucket", "bar-object").getResourceId();
    expectedException.expect(IllegalArgumentException.class);
    bucket.put(invalidObjectId);
  }

  /**
   * Helper for verifying basic interactions with a CachedBucket after it has had its bucket-level
   * info set already.
   */
  private void validateBasicInteractions(StorageResourceId resourceId,
      GoogleCloudStorageItemInfo itemInfo, long creationTime, long lastUpdatedTime,
      CachedBucket bucket) {
    // Info on the bucket itself.
    assertThat(bucket.getName()).isEqualTo(resourceId.getBucketName());
    assertThat(bucket.getCreationTimeMillis()).isEqualTo(creationTime);
    assertThat(bucket.getItemInfoUpdateTimeMillis()).isEqualTo(lastUpdatedTime);
    assertEquals(itemInfo, bucket.getItemInfo());

    // Even when empty, getObjecList() doesn't return null.
    assertThat(bucket.getNumObjects()).isEqualTo(0);
    assertThat(bucket.getObjectList()).isNotNull();

    // It's fine to get/remove an non-existent StorageObject.
    assertThat(bucket.get(objectResourceId)).isNull();
    bucket.remove(objectResourceId);

    // Now add a StorageObject within the bucket.
    long addTime = 43L;
    when(mockClock.currentTimeMillis()).thenReturn(addTime);
    bucket.put(objectResourceId);
    CacheEntry objectEntry = bucket.get(objectResourceId);
    assertThat(objectEntry).isNotNull();

    // Check the added object's CacheEntry info.
    assertThat(objectEntry.getCreationTimeMillis()).isEqualTo(addTime);
    assertThat(objectEntry.getItemInfoUpdateTimeMillis()).isEqualTo(0);
    assertThat(objectEntry.getItemInfo()).isNull();
    assertThat(bucket.getObjectList()).hasSize(1);
    assertThat(bucket.getNumObjects()).isEqualTo(1);
    assertEquals(objectEntry, bucket.getObjectList().get(0));

    // Populate the object's info.
    long objectUpdateTime = 67L;
    when(mockClock.currentTimeMillis()).thenReturn(objectUpdateTime);
    assertThat(objectUpdateTime != addTime).isTrue();
    objectEntry.setItemInfo(objectInfo);
    assertThat(objectEntry.getCreationTimeMillis()).isEqualTo(addTime);
    assertThat(objectEntry.getItemInfoUpdateTimeMillis()).isEqualTo(objectUpdateTime);
    assertEquals(objectInfo, objectEntry.getItemInfo());

    // Adding the same thing doesn't modify the existing cache entry at all or invalidate
    // the entry's info. NB: This will change if we change the behavior to invalidate any existing
    // entry's info on a second add.
    CacheEntry returnedEntry = bucket.put(objectResourceId);
    objectEntry = bucket.get(objectResourceId);

    // Check for true reference equality, not just ".equals()"; we expect the CacheEntry to really
    // be the exact same object.
    assertThat(returnedEntry == objectEntry).isTrue();
    assertThat(objectEntry.getCreationTimeMillis()).isEqualTo(addTime);
    assertThat(objectEntry.getItemInfoUpdateTimeMillis()).isEqualTo(objectUpdateTime);
    assertEquals(objectInfo, objectEntry.getItemInfo());

    // Make sure the returned list doesn't start giving us duplicate entries.
    assertThat(bucket.getObjectList()).hasSize(1);
    assertThat(bucket.getNumObjects()).isEqualTo(1);

    // Now remove the object.
    bucket.remove(objectResourceId);
    assertThat(bucket.getObjectList()).isEmpty();
    assertThat(bucket.getNumObjects()).isEqualTo(0);
  }

  @Test
  public void testBasicInfoFromResourceId() {
    long constructorTime = 1003L;
    when(mockClock.currentTimeMillis()).thenReturn(constructorTime);
    CachedBucket bucket = new CachedBucket(bucketResourceId.getBucketName());

    assertThat(bucket.getName()).isEqualTo(bucketResourceId.getBucketName());
    assertThat(bucket.getNumObjects()).isEqualTo(0);

    // We update the CacheEntry directly; it will be reflected in the actual CacheEntry instance
    // such that future calls to bucket.getItemInfo() will reflect the new data.
    long updateTime = 1010L;
    when(mockClock.currentTimeMillis()).thenReturn(updateTime);
    assertThat(constructorTime != updateTime).isTrue();
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
