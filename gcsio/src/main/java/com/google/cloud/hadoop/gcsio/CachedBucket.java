/**
 * Copyright 2013 Google Inc. All Rights Reserved.
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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A cache entry for the relevant metadata of a single GCS Bucket, used by {@code
 * DirectoryListCache} to supplement "list" operations with buckets known locally, as well as
 * serving as the container for retrieving CachedObjects from their respective cached buckets.
 *
 * Other than direct Bucket creation, a CachedBucket may also come into existence when generating
 * a CachedObject that inherently implies the existence of its parent bucket. Such an entry may
 * not have associated GoogleCloudStorageItemInfo available. The caller is responsible for fetching
 * or updating such stale/nonexistent metadata if it is desired; for name-listing operations,
 * cached bucket/object names may be used directly without lazily populating associated metadata.
 */
public class CachedBucket extends CacheEntry {
  // Mapping from objectNames to cache entries for GCS StorageObjects.
  private final SortedMap<String, CacheEntry> objectLookup = new TreeMap<>();

  /**
   * Constructs a CachedBucket that has no associated GoogleCloudStorageItemInfo for the bucket.
   *
   * @param bucketName Must be non-null and non-empty.
   */
  public CachedBucket(String bucketName) {
    super(new StorageResourceId(bucketName));
  }

  /**
   * Constructs a CachedBucket with the provided GoogleCloudStorageItemInfo for the bucket.
   *
   * @param bucketItemInfo Must be non-null and must correspond to a Bucket.
   */
  public CachedBucket(GoogleCloudStorageItemInfo bucketItemInfo) {
    super(bucketItemInfo);
    Preconditions.checkArgument(bucketItemInfo.isBucket(),
        "CachedBucket requires bucketItemInfo.isBucket() to be true");
  }

  /**
   * Returns the name of the Bucket associated with this CachedBucket.
   */
  public String getName() {
    return getResourceId().getBucketName();
  }

  /**
   * Helper to be called by all methods that take a resourceId that corresponds to a
   * StorageObject that resides in this bucket; validates that the {@code resourceId} is indeed
   * a StorageObject and that its bucket matches this CachedBucket's bucketName.
   */
  private void validateStorageObjectId(StorageResourceId resourceId) {
    Preconditions.checkArgument(resourceId != null, "resourceId must not be null.");
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "resourceId must be a StorageObject, got: %s", resourceId);
    Preconditions.checkArgument(
        resourceId.getBucketName().equals(getName()),
        "resourceId.getBucketName() (%s) doesn't match this.getName() (%s)",
        resourceId.getBucketName(), getName());
  }

  /**
   * Returns the CacheEntry entry corresponding to {@code resourceId} that must be a StorageObject
   * residing inside this CachedBucket, or null if one doesn't exist. The CacheEntry is the shared
   * reference, so that any mutations to the CacheEntry made by the caller will be reflected
   * for future callers retrieving the same CacheEntry.
   *
   * @param resourceId identifies a StorageObject. Bucket must match this CachedBucket's name.
   */
  public synchronized CacheEntry get(StorageResourceId resourceId) {
    validateStorageObjectId(resourceId);
    return objectLookup.get(resourceId.getObjectName());
  }

  /**
   * Removes the CacheEntry entry corresponding to {@code resourceId} that must be a StorageObject
   * residing inside this CachedBucket, if it exists.
   *
   * @param resourceId identifies a StorageObject. Bucket must match this CachedBucket's name.
   */
  public synchronized void remove(StorageResourceId resourceId) {
    validateStorageObjectId(resourceId);
    objectLookup.remove(resourceId.getObjectName());
  }

  /**
   * Adds a CacheEntry entry to this bucket corresponding to the StorageObject for
   * {@code resourceId}.
   *
   * @param resourceId identifies a StorageObject. Bucket must match this CachedBucket's name.
   * @return The CacheEntry that got added, *or* the pre-existing entry.
   */
  public synchronized CacheEntry put(StorageResourceId resourceId) {
    validateStorageObjectId(resourceId);

    // Only add a new CacheEntry entry if it doesn't already exist.
    // TODO(user): Maybe invalidate any existing entry's info.
    CacheEntry returnEntry = objectLookup.get(resourceId.getObjectName());
    if (returnEntry == null) {
      returnEntry = new CacheEntry(resourceId);
      objectLookup.put(resourceId.getObjectName(), returnEntry);
    }
    return returnEntry;
  }

  /**
   * @return List of CacheEntrys for StorageObjects residing in this bucket. May be empty. The
   *     list is a copy, so any later additions/removals of StorageObjects to this bucket will
   *     not be reflected in the returned list instance.
   */
  public synchronized List<CacheEntry> getObjectList() {
    return ImmutableList.copyOf(objectLookup.values());
  }

  /**
   * Gets sublist of all objects which match {@code prefix}; the returned list will be in
   * lexicographical order. NB: This will omit objects containing '\uFFFF' in the position
   * immediately after {@code prefix}.
   *
   * @param prefix a prefix for all returned objects to share; if null or empty this is
   *     the same as calling {@link #getObjectList()} without any prefix.
   */
  public synchronized List<CacheEntry> getObjectList(String prefix) {
    if (Strings.isNullOrEmpty(prefix)) {
      return getObjectList();
    }
    return ImmutableList.copyOf(objectLookup.subMap(prefix, prefix + Character.MAX_VALUE).values());
  }

  /**
   * @return The number of StorageObjects residing within this CachedBucket.
   */
  public synchronized int getNumObjects() {
    return objectLookup.size();
  }
}
