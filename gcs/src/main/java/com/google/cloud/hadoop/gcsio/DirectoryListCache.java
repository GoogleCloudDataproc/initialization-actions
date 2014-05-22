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

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DirectoryListCache provides in-memory accounting of full paths for directories and files created
 * and deleted in GoogleCloudStorageFileSystem within a single running process. It serves primarily
 * as a supplemental list index for underlying Storage.list() requests, because the remote listing
 * is updated asynchronously, so that recently-created files may not appear in a subsequent listing,
 * even if the request is made by the same client which created the file.
 * <p>
 * This in-memory layer guarantees same-client consistency between file-creation and a
 * subsequent "list" request. Same-client deletes will remove associated entries from the cache,
 * so that same-client create-followed-by-delete will not erroneously supplement a nonexistent
 * object. List-after-delete consistency is implemented by blacklisting associated entries.
 * Cache staleness in the face of cross-client operations is mitigated by the fact that the
 * cached GoogleCloudStorageItemInfo can be configured to expire with a shorter lifespan than
 * the cache entry itself; when the GoogleCloudStorageItemInfo is fetched lazily, 404s allow
 * pre-emptively removing stale deleted CacheEntries, and likewise, non-404s allow removing
 * an incorrect cache-blacklist entry.
 * <p>
 * The tradeoff of using this cache is that cross-client deletes may take longer to become visible,
 * if the associated GoogleCloudStorageItemInfo is still active in the cache after another client
 * deletes the associated object. To summarize:
 * <p>
 * 1. Same-client 'create' followed by 'list' will be immediately consistent.
 * 2. Cross-client 'delete' followed by 'list' will be worse than pure-GCS.
 * 3. Same-client 'delete' followed by 'list' is unchanged with respect to eventual consistency.
 * <p>
 * This class is thread-safe.
 */
public class DirectoryListCache {
  // TODO(user): Actually add support for delete-followed-by-list, cache-removal on 404, and
  // cache-blacklist-removal on non-404 of what we think is a "deleted" entry.
  // Logger.
  private static final LogUtil log = new LogUtil(DirectoryListCache.class);

  // The shared singleton instance of DirectoryListCache.
  private static final DirectoryListCache singletonInstance = new DirectoryListCache();

  // Clock instance used for calculating expiration times.
  private Clock clock = Clock.SYSTEM;

  // Mapping from bucketName to data structure which holds both the CacheEntry corresponding to
  // the bucket itself as well as mappings to CacheEntry values for StorageObjects residing in
  // the bucket. Existence of CacheEntrys is solely handled through synchronized methods of
  // DirectoryListCache. However, the handling of GoogleCloudStorageItemInfos within each CacheEntry
  // is synchronized only by the CacheEntry itself; therefore inner itemInfos may change outside
  // of a DirectoryListCache method.
  private final Map<String, CachedBucket> bucketLookup = new HashMap<>();

  /**
   * Container for various cache-configuration parameters used by a DirectoryListCache when
   * managing expiration/retention policies, etc.
   */
  public static class Config {
    // Maximum number of milliseconds a cache entry will remain in this cache, even as an id-only
    // entry (no risk of stale GoogleCloudStorageItemInfo). In general, entries should be allowed
    // to expire fully from the cache once reasonably certain the remote GCS API's list-index
    // is up-to-date to save memory and computation when trying to supplement new results using
    // the cache.
    private long maxEntryAgeMillis = 60 * 1000L;

    // Maximum number of milliseconds a GoogleCloudStorageItemInfo will remain "valid" in the cache,
    // after which the next attempt to fetch the itemInfo will require fetching fresh info from
    // a GoogleCloudStorage instance.
    private long maxInfoAgeMillis = 10 * 1000L;

    /**
     * Getter for maxEntryAgeMillis.
     */
    public synchronized long getMaxEntryAgeMillis() {
      return maxEntryAgeMillis;
    }

    /**
     * Setter for maxEntryAgeMillis.
     */
    public synchronized Config setMaxEntryAgeMillis(long maxEntryAgeMillis) {
      this.maxEntryAgeMillis = maxEntryAgeMillis;
      return this;
    }

    /**
     * Getter for maxInfoAgeMillis.
     */
    public synchronized long getMaxInfoAgeMillis() {
      return maxInfoAgeMillis;
    }

    /**
     * Setter for maxInfoAgeMillis.
     */
    public synchronized Config setMaxInfoAgeMillis(long maxInfoAgeMillis) {
      this.maxInfoAgeMillis = maxInfoAgeMillis;
      return this;
    }
  }

  // The configuration settings for this DirectlyListCache instance.
  private Config cacheConfig = new Config();

  /**
   * Callers should usually only obtain an instance via {@link #getInstance()} so that cache
   * info is shared process-wide, but instances can be created for temporary caches.
   */
  public DirectoryListCache() {
  }

  /**
   * Accessor for shared singleton instance of DirectoryListCache.
   */
  public static DirectoryListCache getInstance() {
    return singletonInstance;
  }

  /**
   * Sets the static Clock instance used for calculating expiration times.
   */
  @VisibleForTesting
  synchronized void setClock(Clock clock) {
    this.clock = clock;
  }

  /**
   * Gets the internal number of CachedBucket entries, which may not be equal to the size of
   * getBucketList() if there are expired entries. Does not mutate the cache.
   */
  @VisibleForTesting
  synchronized int getInternalNumBuckets() {
    return bucketLookup.size();
  }

  /**
   * Gets the internal total count of cached StorageObject entries. Does not mutate the cache.
   */
  @VisibleForTesting
  synchronized int getInternalNumObjects() {
    int objectCount = 0;
    for (CachedBucket bucket : bucketLookup.values()) {
      objectCount += bucket.getNumObjects();
    }
    return objectCount;
  }

  /**
   * Returns the {@code Config} instance used by this DirectoryListCache instance to determine
   * expiration ages, etc. It is the actual mutable Config object, such that modifications in-place
   * made to the returned Config will immediately be reflected in the behavior of the
   * DirectoryListCache.
   */
  public Config getMutableConfig() {
    return cacheConfig;
  }

  /**
   * Helper to validate {@code resourceId} which may be a Bucket or StorageObject.
   */
  private void validateResourceId(StorageResourceId resourceId) {
    Preconditions.checkArgument(resourceId != null,
        "DirectoryListCache requires non-null resourceId.");
    Preconditions.checkArgument(!resourceId.isRoot(),
        "DirectoryListCache cannot be used to cache ROOT info.");
  }

  /**
   * Adds the names of the Bucket or StorageObject referenced by {@code resourceId} to the cache,
   * with no attached metadata. If the entry already exists, then nothing is modified. If resourceId
   * is a StorageObject, the parent Bucket name is also added to the cache, if it doesn't already
   * exist.
   * TODO(user): Even if the entry exists, it might be correct to invalidate any existing metadata
   * and force a refresh next time it is fetched.
   *
   * @return The CacheEntry corresponding to the item added.
   */
  public synchronized CacheEntry putResourceId(StorageResourceId resourceId) {
    validateResourceId(resourceId);

    // Whether the resourceId is a Bucket or StorageObject, there will be a bucketName to cache.
    CachedBucket resourceBucket = bucketLookup.get(resourceId.getBucketName());
    if (resourceBucket == null) {
      // TODO(user): Maybe invalidate any existing Bucket entry's info.
      resourceBucket = new CachedBucket(resourceId.getBucketName());
      bucketLookup.put(resourceId.getBucketName(), resourceBucket);
    }

    if (resourceId.isStorageObject()) {
      return resourceBucket.put(resourceId);
    } else {
      return resourceBucket;
    }
  }

  /**
   * Returns the CacheEntry associated with {@code resourceId}, or null if it doesn't exist.
   * This returns the real mutable CacheEntry (rather than a copy of the data) so that the
   * caller may efficiently update the info stored in the CacheEntry if necessary.
   */
  public synchronized CacheEntry getCacheEntry(StorageResourceId resourceId) {
    validateResourceId(resourceId);

    CachedBucket bucket = bucketLookup.get(resourceId.getBucketName());
    if (bucket == null) {
      return null;
    }

    if (resourceId.isStorageObject()) {
      return bucket.get(resourceId);
    } else {
      return bucket;
    }
  }

  /**
   * Removes CacheEntry associated with {@code resourceId}, if it exists. Cached
   * GoogleCloudStorageItemInfo associated with the resourceId is also removed, if it exists.
   * If {@code resourceId} denotes a non-empty bucket, then all the cached StorageObject children
   * of that bucket will also be removed from the cache; it is the caller's responsibility to
   * ensure that the bucket should really be removed. Note that normal expiration of a CachedBucket
   * will *not* remove the actual CachedBucket, even though the bucketName will stop appearing
   * in calls to getBucketList().
   */
  public synchronized void removeResourceId(StorageResourceId resourceId) {
    validateResourceId(resourceId);

    CachedBucket bucket = bucketLookup.get(resourceId.getBucketName());
    if (bucket == null) {
      log.debug("Tried to remove resourceId '%s' from nonexistent bucket '%s'",
          resourceId, resourceId.getBucketName());
      return;
    }

    if (resourceId.isStorageObject()) {
      log.debug("Explicitly removing StorageObject from CachedBucket: '%s'", resourceId);
      bucket.remove(resourceId);

      // TODO(user): Maybe proactively check for whether this removal now lets us fully remove
      // an expired CachedBucket.
    } else {
      if (bucket.getNumObjects() > 0) {
        log.warn("Explicitly removing non-empty Bucket: '%s' which contains %d items",
            resourceId, bucket.getNumObjects());
      } else {
        log.debug("Explicitly removing empty Bucket: '%s'", resourceId);
      }
      bucketLookup.remove(resourceId.getBucketName());
    }
  }

  /**
   * Helper for possibly clearing the GoogleCloudStorageItemInfo stored in {@code entry} based
   * on cacheConfig settings.
   */
  private synchronized void maybeInvalidateExpiredInfo(CacheEntry entry) {
    // We must synchronize on 'entry' since we are reading its itemInfoUpdateTimeMillis and then
    // possibly mutating it based on that value. Requires that CacheEntry's other mutators like
    // setItemInfo are also synchronized at the object-instance level.
    synchronized (entry) {
      long lastUpdated = entry.getItemInfoUpdateTimeMillis();
      long infoAge = clock.currentTimeMillis() - lastUpdated;
      if (lastUpdated > 0 && infoAge > cacheConfig.getMaxInfoAgeMillis()) {
        log.debug("Clearing itemInfo for CacheEntry '%s' with infoAge: %d ms",
            entry.getResourceId(), infoAge);
        entry.clearItemInfo();
      }
    }
  }

  /**
   * Helper for determining whether a CacheEntry is entirely expired and should be removed
   * from the cache.
   */
  private synchronized boolean isCacheEntryExpired(CacheEntry entry) {
    long creationTime = entry.getCreationTimeMillis();
    long entryAge = clock.currentTimeMillis() - creationTime;
    if (entryAge > cacheConfig.getMaxEntryAgeMillis()) {
      return true;
    }
    return false;
  }

  /**
   * @return List of CacheEntry corresponding to Buckets known by this cache; includes buckets
   *     added to the cache automatically when caching a StorageObject contained in the
   *     corresponding bucket. Will not return null. Hides/removes expired bucket entries and
   *     clears any expired GoogleCloudStorageItemInfo associated with buckets.
   */
  public synchronized List<CacheEntry> getBucketList() {
    List<CacheEntry> bucketEntries = new ArrayList<>();
    List<CachedBucket> expiredBuckets = new ArrayList<>();
    for (CachedBucket bucket : bucketLookup.values()) {
      maybeInvalidateExpiredInfo(bucket);

      if (isCacheEntryExpired(bucket)) {
        // Keep a list of expired buckets to handle after the loop. We may not be able to garbage-
        // collect it because of inner StorageObjects, but we at least won't list it anymore.
        expiredBuckets.add(bucket);
      } else {
        bucketEntries.add(bucket);
      }
    }

    // Handle bucket expiration; we'll only remove the bucket if it's empty. Since each
    // CachedBucket is only modified within synchronized methods of DirectoryListCache, we are
    // certain the CachedBucket is still empty when we remove it from the bucketLookup if it
    // returns getNumObjects() == 0 immediately prior.
    for (CachedBucket expiredBucket : expiredBuckets) {
      if (expiredBucket.getNumObjects() == 0) {
        log.debug("Removing empty expired CachedBucket: '%s'", expiredBucket.getName());
        bucketLookup.remove(expiredBucket.getName());
      }
    }

    return bucketEntries;
  }

  /**
   * @return List of CacheEntrys for StorageObjects residing in bucket {@code bucketName}, or
   *     possibly null if no such objects are present. May also return an empty list.
   */
  public synchronized List<CacheEntry> getObjectList(String bucketName) {
    CachedBucket bucket = bucketLookup.get(bucketName);
    if (bucket == null) {
      return null;
    }

    List<CacheEntry> objectEntries = new ArrayList<>();
    boolean removedExpiredEntries = false;
    for (CacheEntry objectEntry : bucket.getObjectList()) {
      maybeInvalidateExpiredInfo(objectEntry);

      if (isCacheEntryExpired(objectEntry)) {
        // We can remove items from the bucket mid-iteration because we are iterating over a
        // *copy* of the bucket contents.
        log.debug("Removing expired CacheEntry: '%s'", objectEntry.getResourceId());
        bucket.remove(objectEntry.getResourceId());
        removedExpiredEntries = true;
      } else {
        objectEntries.add(objectEntry);
      }
    }

    // Proactively remove the entire entry for the bucket if our iteration caused it to be empty
    // and the CachedBucket is itself expired.
    if (removedExpiredEntries
        && bucket.getNumObjects() == 0
        && isCacheEntryExpired(bucket)) {
      log.debug("Removing empty expired CachedBucket: '%s'", bucket.getName());
      bucketLookup.remove(bucket.getName());
    }
    return objectEntries;
  }
}
