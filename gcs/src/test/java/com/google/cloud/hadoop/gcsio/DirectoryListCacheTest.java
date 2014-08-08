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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * UnitTests for DirectoryListCache class.
 */
@RunWith(JUnit4.class)
public class DirectoryListCacheTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final long MAX_ENTRY_AGE = 10000L;
  private static final long MAX_INFO_AGE = 2000L;
  private static final long BASE_TIME = 123L;

  private Clock mockClock;
  private StorageResourceId bucketResourceId;
  private StorageResourceId objectResourceId;
  private GoogleCloudStorageItemInfo bucketInfo;
  private GoogleCloudStorageItemInfo objectInfo;
  private DirectoryListCache cache;

  @Before
  public void setUp() {
    bucketInfo = DirectoryListCacheTestUtils.createBucketInfo("foo-bucket");
    bucketResourceId = bucketInfo.getResourceId();
    objectInfo = DirectoryListCacheTestUtils.createObjectInfo("foo-bucket", "bar-object");
    objectResourceId = objectInfo.getResourceId();

    cache = new DirectoryListCache();
    cache.getMutableConfig()
        .setMaxEntryAgeMillis(MAX_ENTRY_AGE)
        .setMaxInfoAgeMillis(MAX_INFO_AGE);

    // No auto-increment on the clock because we will want very fine-grained control over elapsed
    // time to test the expiration behavior.
    mockClock = mock(Clock.class);
    when(mockClock.currentTimeMillis()).thenReturn(BASE_TIME);

    CacheEntry.setClock(mockClock);
    cache.setClock(mockClock);
  }

  @Test
  public void testGetInstance() {
    assertNotNull(DirectoryListCache.getInstance());
  }

  @Test
  public void testPutResourceIdThrowsWhenStorageResourceIdIsNull() {
    expectedException.expect(IllegalArgumentException.class);
    cache.putResourceId(null);
  }

  @Test
  public void testGetCacheEntryThrowsWhenStorageResourceIdIsNull() {
    expectedException.expect(IllegalArgumentException.class);
    cache.getCacheEntry(null);
  }

  @Test
  public void testRemoveResourceIdThrowsWhenStorageResourceIdIsNull() {
    expectedException.expect(IllegalArgumentException.class);
    cache.removeResourceId(null);
  }

  @Test
  public void testPutResourceIdThrowsWhenStorageResourceIdIsRoot() {
    expectedException.expect(IllegalArgumentException.class);
    cache.putResourceId(StorageResourceId.ROOT);
  }

  @Test
  public void testGetCacheEntryTHrowsWhenStorageResourceIdIsRoot() {
    expectedException.expect(IllegalArgumentException.class);
    cache.getCacheEntry(StorageResourceId.ROOT);
  }

  @Test
  public void testRemoveResourceIdThrowsWhenStorageResourceIdIsRoot() {
    expectedException.expect(IllegalArgumentException.class);
    cache.removeResourceId(StorageResourceId.ROOT);
  }

  /**
   * Helper for testing basic add/get/remove and setItemInfo interactions intentionally not yet
   * testing the more involved expiration logic.
   */
  private void basicTestHelper(StorageResourceId resourceId, GoogleCloudStorageItemInfo itemInfo) {
    assertEquals(0, cache.getInternalNumBuckets());
    assertEquals(0, cache.getInternalNumObjects());
    CacheEntry entry = cache.putResourceId(resourceId);
    assertNotNull(entry);

    // A Bucket is always created.
    assertEquals(1, cache.getInternalNumBuckets());
    if (resourceId.isStorageObject()) {
      assertEquals(1, cache.getInternalNumObjects());
    } else {
      assertEquals(0, cache.getInternalNumObjects());
    }

    // Verify that the entr started out without itemInfo, then set some info on the entry.
    assertNull(entry.getItemInfo());
    entry.setItemInfo(itemInfo);
    assertNotNull(entry.getItemInfo());
    assertEquals(itemInfo, entry.getItemInfo());

    // Adding the same thing again doesn't actually insert a new entry; the returned value should
    // be the exact same CacheEntry instance (test == instead of just equals()).
    CacheEntry newEntry = cache.putResourceId(resourceId);
    assertTrue(newEntry == entry);
    assertEquals(1, cache.getInternalNumBuckets());
    if (resourceId.isStorageObject()) {
      assertEquals(1, cache.getInternalNumObjects());
    } else {
      assertEquals(0, cache.getInternalNumObjects());
    }

    // The info is still in there. This will change if we make putResourceId invalidate existing
    // info.
    assertNotNull(newEntry.getItemInfo());
    assertEquals(itemInfo, newEntry.getItemInfo());

    // Retrieve the entry directly.
    CacheEntry retrievedEntry = cache.getCacheEntry(resourceId);
    assertTrue(retrievedEntry == entry);
    assertEquals(itemInfo, retrievedEntry.getItemInfo());

    // Now remove it. If it was a StorageObject, the implied bucket still hangs around.
    cache.removeResourceId(resourceId);
    if (resourceId.isStorageObject()) {
      assertEquals(1, cache.getInternalNumBuckets());
      assertEquals(0, cache.getInternalNumObjects());
    } else {
      assertEquals(0, cache.getInternalNumBuckets());
      assertEquals(0, cache.getInternalNumObjects());
    }
    assertNull(cache.getCacheEntry(resourceId));

    // Fine to remove nonexistent resourceId.
    cache.removeResourceId(resourceId);
    if (resourceId.isStorageObject()) {
      assertEquals(1, cache.getInternalNumBuckets());
      assertEquals(0, cache.getInternalNumObjects());
    } else {
      assertEquals(0, cache.getInternalNumBuckets());
      assertEquals(0, cache.getInternalNumObjects());
    }

    // Re-insert; the CacheEntry should now be different and will not contain any itemInfo.
    CacheEntry reinsertedEntry = cache.putResourceId(resourceId);
    assertTrue(reinsertedEntry != entry);
    assertNull(reinsertedEntry.getItemInfo());
    assertEquals(1, cache.getInternalNumBuckets());
    if (resourceId.isStorageObject()) {
      assertEquals(1, cache.getInternalNumObjects());
    } else {
      assertEquals(0, cache.getInternalNumObjects());
    }
  }

  @Test
  public void testBasicBucketOnly() {
    basicTestHelper(bucketResourceId, bucketInfo);
  }

  @Test
  public void testBasicStorageObjectOnly() {
    basicTestHelper(objectResourceId, objectInfo);
  }

  @Test
  public void testRemoveNonEmptyBucket() {
    CacheEntry objectEntry = cache.putResourceId(objectResourceId);
    assertEquals(1, cache.getInternalNumBuckets());
    assertEquals(1, cache.getInternalNumObjects());
    assertEquals(1, cache.getBucketList().size());
    assertEquals(1, cache.getObjectList(bucketResourceId.getBucketName()).size());

    // Removing the auto-created bucket will auto-remove all its children objects as well.
    cache.removeResourceId(bucketResourceId);
    assertEquals(0, cache.getInternalNumBuckets());
    assertEquals(0, cache.getInternalNumObjects());
    assertEquals(0, cache.getBucketList().size());
    assertNull(cache.getObjectList(bucketResourceId.getBucketName()));
  }

  @Test
  public void testExpirationBucketOnly() {
    // Even with no buckets, we should still get an empty non-null list.
    assertEquals(0, cache.getInternalNumBuckets());
    List<CacheEntry> listedBuckets = cache.getBucketList();
    assertNotNull(listedBuckets);

    CacheEntry bucketEntry = cache.putResourceId(bucketResourceId);
    assertEquals(1, cache.getInternalNumBuckets());
    assertEquals(0, cache.getInternalNumObjects());  // Buckets don't count as Objects.
    assertNotNull(bucketEntry);
    bucketEntry.setItemInfo(bucketInfo);
    assertEquals(bucketInfo, bucketEntry.getItemInfo());

    // With 0 time elapsed, the list should return our bucket just fine.
    listedBuckets = cache.getBucketList();
    assertEquals(1, listedBuckets.size());
    assertTrue(bucketEntry == listedBuckets.get(0));

    // Elapse time to 1 millisecond before info expiration; info should still be there.
    long nextTime = BASE_TIME + MAX_INFO_AGE - 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertEquals(1, listedBuckets.size());
    assertEquals(bucketInfo, listedBuckets.get(0).getItemInfo());

    // At exactly MAX_INFO_AGE, the info hasn't expired yet (it is inclusive).
    nextTime += 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertEquals(1, listedBuckets.size());
    assertEquals(bucketInfo, listedBuckets.get(0).getItemInfo());

    // One millisecond later, it will have expired. However, getCacheEntry does not expire entries,
    // only getBucketList does.
    nextTime += 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    CacheEntry retrieved = cache.getCacheEntry(bucketResourceId);
    assertTrue(retrieved == bucketEntry);
    assertEquals(bucketInfo, retrieved.getItemInfo());

    // The list command will proactively remove the info; the removal will manifest in our other
    // CacheEntry references as well.
    listedBuckets = cache.getBucketList();
    assertEquals(1, listedBuckets.size());
    assertNull(listedBuckets.get(0).getItemInfo());
    assertNull(retrieved.getItemInfo());
    assertNull(bucketEntry.getItemInfo());

    // Now expire the entry entirely.
    when(mockClock.currentTimeMillis()).thenReturn(
        bucketEntry.getCreationTimeMillis() + MAX_ENTRY_AGE + 1);

    // Since it was empty, the CachedBucket will truly get removed entirely; getCacheEntry will
    // not retrieve it any longer.
    listedBuckets = cache.getBucketList();
    assertEquals(0, listedBuckets.size());
    assertEquals(0, cache.getInternalNumBuckets());
    assertNull(cache.getCacheEntry(bucketResourceId));
  }

  @Test
  public void testBucketExpiredButInfoNotExpired() {
    CacheEntry bucketEntry = cache.putResourceId(bucketResourceId);

    long nextTime = BASE_TIME + MAX_ENTRY_AGE;
    // Increment time until the cache entry is just about to expire, before adding info.
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    bucketEntry.setItemInfo(bucketInfo);
    assertEquals(bucketInfo, bucketEntry.getItemInfo());

    List<CacheEntry> listedBuckets = cache.getBucketList();
    assertEquals(1, listedBuckets.size());
    assertEquals(bucketInfo, listedBuckets.get(0).getItemInfo());

    // Now expire it out of the cache; the entry will no longer get returned despite the inner
    // info not having expired.
    nextTime += 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertEquals(0, listedBuckets.size());
    assertEquals(0, cache.getInternalNumBuckets());
    assertNull(cache.getCacheEntry(bucketResourceId));

    // However, our old reference to the CacheEntry is still alive, and still holding the info.
    assertEquals(bucketInfo, bucketEntry.getItemInfo());

    // The cache no longer controls the reference we hold, therefore no matter how much more time
    // elapses, its info will not get removed.
    nextTime += MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertEquals(0, listedBuckets.size());

    assertEquals(bucketInfo, bucketEntry.getItemInfo());
  }

  @Test
  public void testBucketExpiredButNonEmpty() {
    CacheEntry bucketEntry = cache.putResourceId(bucketResourceId);
    long nextTime = BASE_TIME + MAX_ENTRY_AGE;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);

    CacheEntry objectEntry = cache.putResourceId(objectResourceId);
    objectEntry.setItemInfo(objectInfo);
    assertEquals(1, cache.getInternalNumBuckets());
    assertEquals(1, cache.getInternalNumObjects());
    List<CacheEntry> listedBuckets = cache.getBucketList();
    assertEquals(1, listedBuckets.size());

    // Move time past the bucket expiration time. Should list 0 buckets, even though internally
    // there is still a bucket.
    nextTime += 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertEquals(0, listedBuckets.size());
    assertEquals(1, cache.getInternalNumBuckets());
    assertEquals(1, cache.getInternalNumObjects());

    // The child info is still there.
    assertEquals(objectInfo, cache.getCacheEntry(objectResourceId).getItemInfo());
    List<CacheEntry> listedObjects = cache.getObjectList(bucketResourceId.getBucketName());
    assertNotNull(listedObjects);
    assertEquals(1, listedObjects.size());
    assertEquals(objectInfo, listedObjects.get(0).getItemInfo());

    // List of objects should be empty after we remove the object, but not null.
    cache.removeResourceId(objectResourceId);
    listedObjects = cache.getObjectList(bucketResourceId.getBucketName());
    assertNotNull(listedObjects);
    assertEquals(0, listedObjects.size());

    assertEquals(1, cache.getInternalNumBuckets());
    assertEquals(0, cache.getInternalNumObjects());

    // Next time we call getBucketList, the expired bucket will actually be fully removed.
    listedBuckets = cache.getBucketList();
    assertEquals(0, cache.getInternalNumBuckets());
    assertEquals(0, cache.getInternalNumObjects());
    assertEquals(0, listedBuckets.size());

    // With the bucket removed, the listedObjects will now be null.
    listedObjects = cache.getObjectList(bucketResourceId.getBucketName());
    assertNull(listedObjects);
  }

  @Test
  public void testExpirationBucketAndObject() {
    CacheEntry objectEntry = cache.putResourceId(objectResourceId);
    CacheEntry bucketEntry = cache.getCacheEntry(bucketResourceId);
    assertNotNull(bucketEntry);
    bucketEntry.setItemInfo(bucketInfo);
    objectEntry.setItemInfo(objectInfo);

    List<CacheEntry> listedBuckets = cache.getBucketList();
    List<CacheEntry> listedObjects = cache.getObjectList(bucketResourceId.getBucketName());
    assertEquals(1, listedBuckets.size());
    assertEquals(1, listedObjects.size());

    long nextTime = BASE_TIME + MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);

    // Listing buckets only affects info-invalidation for buckets.
    listedBuckets = cache.getBucketList();
    assertEquals(1, listedBuckets.size());

    assertNull(bucketEntry.getItemInfo());
    assertEquals(objectInfo, objectEntry.getItemInfo());

    listedObjects = cache.getObjectList(bucketResourceId.getBucketName());
    assertEquals(1, listedObjects.size());

    assertNull(bucketEntry.getItemInfo());
    assertNull(objectEntry.getItemInfo());

    // Reset the info.
    bucketEntry.setItemInfo(bucketInfo);
    objectEntry.setItemInfo(objectInfo);
    nextTime += MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);

    // Listing objects only affects info-invalidation for objects.
    listedObjects = cache.getObjectList(bucketResourceId.getBucketName());
    assertEquals(1, listedObjects.size());

    assertEquals(bucketInfo, bucketEntry.getItemInfo());
    assertNull(objectEntry.getItemInfo());

    listedBuckets = cache.getBucketList();
    assertEquals(1, listedBuckets.size());

    assertNull(bucketEntry.getItemInfo());
    assertNull(objectEntry.getItemInfo());

    // Listing objects can remove objects *and* buckets.
    nextTime += MAX_ENTRY_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedObjects = cache.getObjectList(bucketResourceId.getBucketName());
    assertEquals(0, listedObjects.size());
    listedBuckets = cache.getBucketList();
    assertEquals(0, listedBuckets.size());
    assertEquals(0, cache.getInternalNumBuckets());
    assertEquals(0, cache.getInternalNumObjects());

    // The next call to getObjectList returns null since the bucket is gone.
    listedObjects = cache.getObjectList(bucketResourceId.getBucketName());
    assertNull(listedObjects);
  }
}
