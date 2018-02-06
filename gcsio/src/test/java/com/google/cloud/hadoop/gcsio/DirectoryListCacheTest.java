/**
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Clock;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Abstract base class for UnitTests for DirectoryListCache class; subclasses should override this
 * test to provide the type of DirectoryListCache to be tested.
 */
@RunWith(JUnit4.class)
public abstract class DirectoryListCacheTest {
  protected static final long MAX_ENTRY_AGE = 10000L;
  protected static final long MAX_INFO_AGE = 2000L;
  protected static final long BASE_TIME = 123000L;

  protected static final String BUCKET_NAME = "foo-bucket";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected Clock mockClock;
  protected StorageResourceId bucketResourceId;
  protected StorageResourceId objectResourceId;
  protected GoogleCloudStorageItemInfo bucketInfo;
  protected GoogleCloudStorageItemInfo objectInfo;
  protected DirectoryListCache cache;

  /**
   * Subclasses should override this to provide the DirectoryListCache instance to test.
   */
  protected abstract DirectoryListCache getTestInstance() throws IOException;

  /**
   * Helper to create a StorageResourceId without the verbosity of re-specifying a bucket each time
   * if we're willing to let all objects be in the same bucket.
   */
  static StorageResourceId createId(String objectName) {
    return new StorageResourceId(BUCKET_NAME, objectName);
  }

  @Before
  public void setUp() throws IOException {
    bucketInfo = DirectoryListCacheTestUtils.createBucketInfo(BUCKET_NAME);
    bucketResourceId = bucketInfo.getResourceId();
    objectInfo = DirectoryListCacheTestUtils.createObjectInfo(BUCKET_NAME, "bar-object");
    objectResourceId = objectInfo.getResourceId();

    cache = getTestInstance();

    // No auto-increment on the clock because we will want very fine-grained control over elapsed
    // time to test the expiration behavior.
    mockClock = mock(Clock.class);
    when(mockClock.currentTimeMillis()).thenReturn(BASE_TIME);

    CacheEntry.setClock(mockClock);
    cache.setClock(mockClock);
  }

  @Test
  public void testPutResourceIdThrowsWhenStorageResourceIdIsNull() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    cache.putResourceId(null);
  }

  @Test
  public void testGetCacheEntryThrowsWhenStorageResourceIdIsNull() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    cache.getCacheEntry(null);
  }

  @Test
  public void testRemoveResourceIdThrowsWhenStorageResourceIdIsNull() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    cache.removeResourceId(null);
  }

  @Test
  public void testPutResourceIdThrowsWhenStorageResourceIdIsRoot() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    cache.putResourceId(StorageResourceId.ROOT);
  }

  @Test
  public void testGetCacheEntryThrowsWhenStorageResourceIdIsRoot() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    cache.getCacheEntry(StorageResourceId.ROOT);
  }

  @Test
  public void testRemoveResourceIdThrowsWhenStorageResourceIdIsRoot() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    cache.removeResourceId(StorageResourceId.ROOT);
  }

  /**
   * Helper for testing basic add/get/remove and setItemInfo interactions intentionally not yet
   * testing the more involved expiration logic.
   */
  private void basicTestHelper(StorageResourceId resourceId, GoogleCloudStorageItemInfo itemInfo)
      throws IOException {
    assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
    assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    CacheEntry entry = cache.putResourceId(resourceId);
    assertThat(entry).isNotNull();

    // A Bucket is always created.
    assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
    if (resourceId.isStorageObject()) {
      assertThat(cache.getInternalNumObjects()).isEqualTo(1);
    } else {
      assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    }

    // Verify that the entry started out without itemInfo, then set some info on the entry.
    assertThat(entry.getItemInfo()).isNull();

    if (cache.supportsCacheEntryByReference()) {
      entry.setItemInfo(itemInfo);
      assertThat(entry.getItemInfo()).isNotNull();
      assertThat(entry.getItemInfo()).isEqualTo(itemInfo);
    }

    // Adding the same thing again doesn't actually insert a new entry; the returned value should
    // be the exact same CacheEntry instance (test == instead of just equals()).
    CacheEntry newEntry = cache.putResourceId(resourceId);
    assertThat(newEntry).isNotNull();
    if (cache.supportsCacheEntryByReference()) {
      assertThat(newEntry == entry).isTrue();
    }
    assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
    if (resourceId.isStorageObject()) {
      assertThat(cache.getInternalNumObjects()).isEqualTo(1);
    } else {
      assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    }

    // The info is still in there. This will change if we make putResourceId invalidate existing
    // info.
    if (cache.supportsCacheEntryByReference()) {
      assertThat(newEntry.getItemInfo()).isNotNull();
      assertThat(newEntry.getItemInfo()).isEqualTo(itemInfo);
    }

    // Retrieve the entry directly.
    CacheEntry retrievedEntry = cache.getCacheEntry(resourceId);
    assertThat(retrievedEntry).isNotNull();
    if (cache.supportsCacheEntryByReference()) {
      assertThat(retrievedEntry == entry).isTrue();
      assertThat(retrievedEntry.getItemInfo()).isEqualTo(itemInfo);
    }

    // Now remove it. If it was a StorageObject, the implied bucket still hangs around.
    cache.removeResourceId(resourceId);
    if (resourceId.isStorageObject()) {
      assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
      assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    } else {
      assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
      assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    }
    assertThat(cache.getCacheEntry(resourceId)).isNull();

    // Fine to remove nonexistent resourceId.
    cache.removeResourceId(resourceId);
    if (resourceId.isStorageObject()) {
      assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
      assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    } else {
      assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
      assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    }

    // Re-insert; the CacheEntry should now be different and will not contain any itemInfo.
    CacheEntry reinsertedEntry = cache.putResourceId(resourceId);
    assertThat(reinsertedEntry).isNotNull();
    if (cache.supportsCacheEntryByReference()) {
      assertThat(reinsertedEntry != entry).isTrue();
      assertThat(reinsertedEntry.getItemInfo()).isNull();
    }
    assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
    if (resourceId.isStorageObject()) {
      assertThat(cache.getInternalNumObjects()).isEqualTo(1);
    } else {
      assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    }
  }

  @Test
  public void testBasicBucketOnly() throws IOException {
    basicTestHelper(bucketResourceId, bucketInfo);
  }

  @Test
  public void testBasicStorageObjectOnly() throws IOException {
    basicTestHelper(objectResourceId, objectInfo);
  }

  @Test
  public void testExpirationBucketOnly() throws IOException {
    // Even with no buckets, we should still get an empty non-null list.
    assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
    List<CacheEntry> listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).isNotNull();

    CacheEntry bucketEntry = cache.putResourceId(bucketResourceId);
    assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
    assertThat(cache.getInternalNumObjects()).isEqualTo(0); // Buckets don't count as Objects.
    assertThat(bucketEntry).isNotNull();
    bucketEntry.setItemInfo(bucketInfo);
    assertThat(bucketEntry.getItemInfo()).isEqualTo(bucketInfo);

    // With 0 time elapsed, the list should return our bucket just fine.
    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).hasSize(1);
    if (cache.supportsCacheEntryByReference()) {
      assertThat(bucketEntry == listedBuckets.get(0)).isTrue();
    }

    // Elapse time to 1 millisecond before info expiration; info should still be there.
    long nextTime = BASE_TIME + MAX_INFO_AGE - 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).hasSize(1);
    if (cache.supportsCacheEntryByReference()) {
      assertThat(listedBuckets.get(0).getItemInfo()).isEqualTo(bucketInfo);
    }

    // At exactly MAX_INFO_AGE, the info hasn't expired yet (it is inclusive).
    nextTime += 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).hasSize(1);
    if (cache.supportsCacheEntryByReference()) {
      assertThat(listedBuckets.get(0).getItemInfo()).isEqualTo(bucketInfo);
    }

    // One millisecond later, it will have expired. However, getCacheEntry does not expire entries,
    // only getBucketList does.
    nextTime += 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    CacheEntry retrieved = cache.getCacheEntry(bucketResourceId);
    assertThat(retrieved).isNotNull();
    if (cache.supportsCacheEntryByReference()) {
      assertThat(retrieved == bucketEntry).isTrue();
      assertThat(retrieved.getItemInfo()).isEqualTo(bucketInfo);
    }

    // The list command will proactively remove the info; the removal will manifest in our other
    // CacheEntry references as well.
    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).hasSize(1);
    if (cache.supportsCacheEntryByReference()) {
      assertThat(listedBuckets.get(0).getItemInfo()).isNull();
      assertThat(retrieved.getItemInfo()).isNull();
      assertThat(bucketEntry.getItemInfo()).isNull();
    }

    // Now expire the entry entirely.
    when(mockClock.currentTimeMillis()).thenReturn(
        bucketEntry.getCreationTimeMillis() + MAX_ENTRY_AGE + 1);

    // Since it was empty, the CachedBucket will truly get removed entirely; getCacheEntry will
    // not retrieve it any longer.
    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).isEmpty();
    assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
    assertThat(cache.getCacheEntry(bucketResourceId)).isNull();
  }

  @Test
  public void testBucketExpiredButInfoNotExpired() throws IOException {
    CacheEntry bucketEntry = cache.putResourceId(bucketResourceId);

    long nextTime = BASE_TIME + MAX_ENTRY_AGE;
    // Increment time until the cache entry is just about to expire, before adding info.
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);

    if (cache.supportsCacheEntryByReference()) {
      bucketEntry.setItemInfo(bucketInfo);
      assertThat(bucketEntry.getItemInfo()).isEqualTo(bucketInfo);
    }

    List<CacheEntry> listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).hasSize(1);
    if (cache.supportsCacheEntryByReference()) {
      assertThat(listedBuckets.get(0).getItemInfo()).isEqualTo(bucketInfo);
    }

    // Now expire it out of the cache; the entry will no longer get returned despite the inner
    // info not having expired.
    nextTime += 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).isEmpty();
    assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
    assertThat(cache.getCacheEntry(bucketResourceId)).isNull();

    if (cache.supportsCacheEntryByReference()) {
      // However, our old reference to the CacheEntry is still alive, and still holding the info.
      assertThat(bucketEntry.getItemInfo()).isEqualTo(bucketInfo);
    }

    // The cache no longer controls the reference we hold, therefore no matter how much more time
    // elapses, its info will not get removed.
    nextTime += MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).isEmpty();

    if (cache.supportsCacheEntryByReference()) {
      assertThat(bucketEntry.getItemInfo()).isEqualTo(bucketInfo);
    }
  }

  @Test
  public void testBucketExpiredButNonEmpty() throws IOException {
    CacheEntry bucketEntry = cache.putResourceId(bucketResourceId);
    long nextTime = BASE_TIME + MAX_ENTRY_AGE;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);

    CacheEntry objectEntry = cache.putResourceId(objectResourceId);
    objectEntry.setItemInfo(objectInfo);
    assertThat(cache.getRawBucketList()).hasSize(1);
    assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
    assertThat(cache.getInternalNumObjects()).isEqualTo(1);
    List<CacheEntry> listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).hasSize(1);

    // Move time past the bucket expiration time. Should list 0 buckets, even though internally
    // there is still a bucket.
    nextTime += 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).isEmpty();
    assertThat(cache.getRawBucketList()).hasSize(1);
    assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
    assertThat(cache.getInternalNumObjects()).isEqualTo(1);

    // The child info is still there.
    if (cache.supportsCacheEntryByReference()) {
      assertThat(cache.getCacheEntry(objectResourceId).getItemInfo()).isEqualTo(objectInfo);
    }
    List<CacheEntry> listedObjects = cache.getObjectList(
        BUCKET_NAME, "", null, null);
    assertThat(listedObjects).isNotNull();
    assertThat(listedObjects).hasSize(1);
    if (cache.supportsCacheEntryByReference()) {
      assertThat(listedObjects.get(0).getItemInfo()).isEqualTo(objectInfo);
    }

    // List of objects should be empty after we remove the object, but not null.
    cache.removeResourceId(objectResourceId);
    listedObjects = cache.getObjectList(BUCKET_NAME, "", null, null);
    assertThat(listedObjects).isNotNull();
    assertThat(listedObjects).isEmpty();

    assertThat(cache.getRawBucketList()).hasSize(1);
    assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
    assertThat(cache.getInternalNumObjects()).isEqualTo(0);

    // Next time we call getBucketList, the expired bucket will actually be fully removed.
    listedBuckets = cache.getBucketList();
    assertThat(cache.getRawBucketList()).isEmpty();
    assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
    assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    assertThat(listedBuckets).isEmpty();

    // With the bucket removed, the listedObjects will now be null.
    listedObjects = cache.getObjectList(BUCKET_NAME, "", null, null);
    assertThat(listedObjects).isNull();
  }

  @Test
  public void testExpirationBucketAndObject() throws IOException {
    CacheEntry objectEntry = cache.putResourceId(objectResourceId);
    CacheEntry bucketEntry = cache.getCacheEntry(bucketResourceId);
    assertThat(bucketEntry).isNotNull();
    if (cache.supportsCacheEntryByReference()) {
      bucketEntry.setItemInfo(bucketInfo);
      objectEntry.setItemInfo(objectInfo);
    }

    List<CacheEntry> listedBuckets = cache.getBucketList();
    List<CacheEntry> listedObjects = cache.getObjectList(
        BUCKET_NAME, "", null, null);
    assertThat(listedBuckets).hasSize(1);
    assertThat(listedObjects).hasSize(1);

    long nextTime = BASE_TIME + MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);

    // Listing buckets only affects info-invalidation for buckets.
    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).hasSize(1);

    if (cache.supportsCacheEntryByReference()) {
      assertWithMessage(
              String.format("Expected null, got itemInfo: '%s'", bucketEntry.getItemInfo()))
          .that(bucketEntry.getItemInfo())
          .isNull();
      assertThat(objectEntry.getItemInfo()).isEqualTo(objectInfo);
    }

    listedObjects = cache.getObjectList(BUCKET_NAME, "", null, null);
    assertThat(listedObjects).hasSize(1);

    if (cache.supportsCacheEntryByReference()) {
      assertThat(bucketEntry.getItemInfo()).isNull();
      assertThat(objectEntry.getItemInfo()).isNull();

      // Reset the info.
      bucketEntry.setItemInfo(bucketInfo);
      objectEntry.setItemInfo(objectInfo);
    }
    nextTime += MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);

    // Listing objects only affects info-invalidation for objects.
    listedObjects = cache.getObjectList(BUCKET_NAME, "", null, null);
    assertThat(listedObjects).hasSize(1);

    if (cache.supportsCacheEntryByReference()) {
      assertThat(bucketEntry.getItemInfo()).isEqualTo(bucketInfo);
      assertThat(objectEntry.getItemInfo()).isNull();
    }

    listedBuckets = cache.getBucketList();
    assertThat(listedBuckets).hasSize(1);

    if (cache.supportsCacheEntryByReference()) {
      assertThat(bucketEntry.getItemInfo()).isNull();
      assertThat(objectEntry.getItemInfo()).isNull();
    }

    // Listing objects can remove objects *and* buckets.
    nextTime += MAX_ENTRY_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    listedObjects = cache.getObjectList(BUCKET_NAME, "", null, null);
    assertThat(listedObjects).isEmpty();
    listedBuckets = cache.getBucketList();
    assertWithMessage(String.format("Got listedBuckets: '%s'", listedBuckets))
        .that(listedBuckets.size())
        .isEqualTo(0);
    assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
    assertThat(cache.getInternalNumObjects()).isEqualTo(0);

    // The next call to getObjectList returns null since the bucket is gone.
    listedObjects = cache.getObjectList(BUCKET_NAME, "", null, null);
    assertThat(listedObjects).isNull();
  }

  @Test
  public void testExpirationLargeDeeplyNestedDirectories() throws IOException {
    cache.putResourceId(new StorageResourceId(
        "foo-bucket", "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z"));

    if (cache.containsEntriesForImplicitDirectories()) {
      assertThat(cache.getInternalNumObjects()).isEqualTo(26);
    } else {
      assertThat(cache.getInternalNumObjects()).isEqualTo(1);
    }
    long nextTime = BASE_TIME + MAX_ENTRY_AGE + 1000L;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    List<CacheEntry> listedObjects = cache.getObjectList(BUCKET_NAME, "", null, null);
    assertThat(listedObjects).isEmpty();
    assertThat(cache.getInternalNumObjects()).isEqualTo(0);
  }

  /**
   * Helper to set up entries in a way amenable for testing more sophisticated list operations.
   */
  protected void setupForListTests() throws IOException {
    cache.putResourceId(createId("foo/"));
    cache.putResourceId(createId("foo/bar/"));
    cache.putResourceId(createId("foo/bar/data1.txt"));
    cache.putResourceId(createId("foo/baz/"));
    cache.putResourceId(createId("foo/data2.txt"));
    assertThat(cache.getInternalNumObjects()).isEqualTo(5);
  }

  /**
   * Helper to extract a Set set StorageResourceId from a List of CacheEntry.
   */
  protected Set<StorageResourceId> extractResourceIdSet(List<CacheEntry> entries) {
    Set<StorageResourceId> listedSet = new HashSet<>();
    for (CacheEntry entry : entries) {
      listedSet.add(entry.getResourceId());
    }
    return listedSet;
  }

  @Test
  public void testGetObjectListNullPrefixWithDelimiter() throws IOException {
    setupForListTests();

    // Should only list "foo/".
    Set<String> prefixes = new HashSet<>();
    List<CacheEntry> listedObjects = cache.getObjectList(BUCKET_NAME, null, "/", prefixes);

    Set<StorageResourceId> listedSet = extractResourceIdSet(listedObjects);
    assertThat(listedObjects).hasSize(1);
    assertThat(listedSet).contains(createId("foo/"));

    // Only check 'prefixes' logic for implementations which don't auto-create entries for implicit
    // directories.
    if (!cache.containsEntriesForImplicitDirectories()) {
      assertThat(prefixes).hasSize(1);
      assertThat(prefixes).contains("foo/");
    }
  }

  @Test
  public void testGetObjectListEntireRootPrefixWithoutTrailingDelimiter() throws IOException {
    setupForListTests();

    // Should only list "foo/".
    Set<String> prefixes = new HashSet<>();
    List<CacheEntry> listedObjects = cache.getObjectList(BUCKET_NAME, "foo", "/", prefixes);
    Set<StorageResourceId> listedSet = extractResourceIdSet(listedObjects);
    assertThat(listedObjects).hasSize(1);
    assertThat(listedSet).contains(createId("foo/"));

    // Only check 'prefixes' logic for implementations which don't auto-create entries for implicit
    // directories.
    if (!cache.containsEntriesForImplicitDirectories()) {
      assertThat(prefixes).hasSize(1);
      assertThat(prefixes).contains("foo/");
    }
  }

  @Test
  public void testGetObjectListTopLevelPrefixNullDelimiter() throws IOException {
    setupForListTests();

    // Should list everything except "foo/".
    Set<String> prefixes = new HashSet<>();
    List<CacheEntry> listedObjects = cache.getObjectList(BUCKET_NAME, "foo/", null, prefixes);

    Set<StorageResourceId> listedSet = extractResourceIdSet(listedObjects);
    assertThat(listedObjects).hasSize(4);
    assertThat(listedSet).contains(createId("foo/bar/"));
    assertThat(listedSet).contains(createId("foo/bar/data1.txt"));
    assertThat(listedSet).contains(createId("foo/baz/"));
    assertThat(listedSet).contains(createId("foo/data2.txt"));

    // No prefixes if no delimiter.
    assertThat(prefixes).isEmpty();
  }

  @Test
  public void testGetObjectListPartialPrefixNullDelimiter() throws IOException {
    setupForListTests();

    // Everything except "foo/" and "foo/data2.txt".
    Set<String> prefixes = new HashSet<>();
    List<CacheEntry> listedObjects = cache.getObjectList(BUCKET_NAME, "foo/ba", null, prefixes);

    Set<StorageResourceId> listedSet = extractResourceIdSet(listedObjects);
    assertThat(listedObjects).hasSize(3);
    assertThat(listedSet).contains(createId("foo/bar/"));
    assertThat(listedSet).contains(createId("foo/bar/data1.txt"));
    assertThat(listedSet).contains(createId("foo/baz/"));

    // No prefixes if no delimiter.
    assertThat(prefixes).isEmpty();
  }

  @Test
  public void testGetObjectListPartialPrefixWithDelimiter() throws IOException {
    setupForListTests();

    // Only lists foo/bar/ and foo/baz/.
    Set<String> prefixes = new HashSet<>();
    List<CacheEntry> listedObjects = cache.getObjectList(BUCKET_NAME, "foo/ba", "/", prefixes);

    Set<StorageResourceId> listedSet = extractResourceIdSet(listedObjects);
    assertWithMessage(String.format("Expected 2 items in '%s'", listedObjects))
        .that(listedObjects.size())
        .isEqualTo(2);
    assertThat(listedSet).contains(createId("foo/bar/"));
    assertThat(listedSet).contains(createId("foo/baz/"));

    // Only check 'prefixes' logic for implementations which don't auto-create entries for implicit
    // directories.
    if (!cache.containsEntriesForImplicitDirectories()) {
      // This one doesn't contain "foo/baz/" as a prefix, since "prefixes" are strictly generated
      // by an object whose name is of length strictly greatehr than the prefix that would be
      // generated; in our case, "foo/baz/" is the exact-match object, so no prefix is generated.
      assertThat(prefixes).hasSize(1);
      assertThat(prefixes).contains("foo/bar/");
    }
  }

  @Test
  public void testGetObjectListPartialPrefixWithDelimiterImplicitDirectories()
      throws IOException {
    // Don't add the explicit directory objects.
    cache.putResourceId(createId("foo/bar/data1.txt"));
    cache.putResourceId(createId("foo/baz/bat/"));
    cache.putResourceId(createId("foo/bat.txt"));
    cache.putResourceId(createId("foo/brt/"));

    if (cache.containsEntriesForImplicitDirectories()) {
      assertThat(cache.getInternalNumObjects()).isEqualTo(7);
    } else {
      assertThat(cache.getInternalNumObjects()).isEqualTo(4);
    }

    Set<String> prefixes = new HashSet<>();
    List<CacheEntry> listedObjects = cache.getObjectList(BUCKET_NAME, "foo/ba", "/", prefixes);
    Set<StorageResourceId> listedSet = extractResourceIdSet(listedObjects);

    if (cache.containsEntriesForImplicitDirectories()) {
      // Since entries are auto-inserted for implicit directories, we'll fetch actual entries
      // for bar/ and baz/, and the implementation may or may not populated 'prefixes' but
      // we don't care since we got the directory entries corresponding to the prefixes
      // already.
      assertWithMessage(String.format("Expected 3 items in '%s'", listedObjects))
          .that(listedObjects.size())
          .isEqualTo(3);
      assertThat(listedSet).contains(createId("foo/bar/"));
      assertThat(listedSet).contains(createId("foo/baz/"));
      assertThat(listedSet).contains(createId("foo/bat.txt"));
    } else {
      // Since the objects "foo/bar/" and "foo/baz/" don't actually exist, we won't list cache
      // entries for them.
      assertWithMessage(String.format("Expected 1 items in '%s'", listedObjects))
          .that(listedObjects.size())
          .isEqualTo(1);
      assertThat(listedSet).contains(createId("foo/bat.txt"));

      // But we should still get them as prefixes.
      assertWithMessage(String.format("Expected 2 items in '%s'", prefixes))
          .that(prefixes.size())
          .isEqualTo(2);
      assertThat(prefixes).contains("foo/bar/");
      assertThat(prefixes).contains("foo/baz/");
    }
  }

  @Test
  public void testListPrefixAsSubrange() throws IOException {
    cache.putResourceId(createId("fo"));
    cache.putResourceId(createId("fon"));
    cache.putResourceId(createId("foo/"));
    cache.putResourceId(createId("foo/data1.txt"));
    cache.putResourceId(createId("foo/brt/"));
    cache.putResourceId(createId("fop"));
    cache.putResourceId(createId("fopa"));

    // Directory match.
    List<CacheEntry> listedObjects = cache.getObjectList(BUCKET_NAME, "foo", null, null);
    Set<StorageResourceId> listedSet = extractResourceIdSet(listedObjects);
    assertWithMessage(String.format("Expected 3 items in '%s'", listedObjects))
        .that(listedObjects.size())
        .isEqualTo(3);
    assertThat(listedSet).contains(createId("foo/"));
    assertThat(listedSet).contains(createId("foo/data1.txt"));
    assertThat(listedSet).contains(createId("foo/brt/"));

    // Single-object match.
    listedObjects = cache.getObjectList(BUCKET_NAME, "fon", "/", null);
    listedSet = extractResourceIdSet(listedObjects);
    assertWithMessage(String.format("Expected 1 items in '%s'", listedObjects))
        .that(listedObjects.size())
        .isEqualTo(1);
    assertThat(listedSet).contains(createId("fon"));

    // Multi-object no-directory match.
    listedObjects = cache.getObjectList(BUCKET_NAME, "fop", "/", null);
    listedSet = extractResourceIdSet(listedObjects);
    assertWithMessage(String.format("Expected 2 items in '%s'", listedObjects))
        .that(listedObjects.size())
        .isEqualTo(2);
    assertThat(listedSet).contains(createId("fop"));
    assertThat(listedSet).contains(createId("fopa"));

    // No match, prefix is beyond the head of the map.
    listedObjects = cache.getObjectList(BUCKET_NAME, "fn", null, null);
    assertThat(listedObjects).isEmpty();

    // No match, prefix is beyond the tail of the map.
    listedObjects = cache.getObjectList(BUCKET_NAME, "fp", null, null);
    assertThat(listedObjects).isEmpty();
  }
}
