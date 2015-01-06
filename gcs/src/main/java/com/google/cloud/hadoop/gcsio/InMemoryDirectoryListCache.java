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

import com.google.cloud.hadoop.util.LogUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * InMemoryDirectoryListCache provides in-memory accounting of full paths for directories and files
 * created and deleted in GoogleCloudStorageFileSystem within a single running process.
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
public class InMemoryDirectoryListCache extends DirectoryListCache {
  private static final LogUtil log = new LogUtil(InMemoryDirectoryListCache.class);

  // The shared singleton instance of DirectoryListCache.
  private static final InMemoryDirectoryListCache singletonInstance =
      new InMemoryDirectoryListCache();

  // Mapping from bucketName to data structure which holds both the CacheEntry corresponding to
  // the bucket itself as well as mappings to CacheEntry values for StorageObjects residing in
  // the bucket. Existence of CacheEntrys is solely handled through synchronized methods of
  // DirectoryListCache. However, the handling of GoogleCloudStorageItemInfos within each CacheEntry
  // is synchronized only by the CacheEntry itself; therefore inner itemInfos may change outside
  // of a DirectoryListCache method.
  private final Map<String, CachedBucket> bucketLookup = new HashMap<>();

  /**
   * Callers should usually only obtain an instance via {@link #getInstance()} so that cache
   * info is shared process-wide, but instances can be created for temporary caches.
   */
  public InMemoryDirectoryListCache() {
  }

  /**
   * Accessor for shared singleton instance of DirectoryListCache.
   */
  public static DirectoryListCache getInstance() {
    return singletonInstance;
  }

  /**
   * We use in-memory data structures to hold CacheEntry items, and thus manage them in a shared
   * manner; returned CacheEntry items are shared references, and updating their cached info
   * effectively updates the entry's info for all users of the cache.
   */
  @Override
  public boolean supportsCacheEntryByReference() {
    return true;
  }

  /**
   * We don't inspect StorageResourceIds in putResourceId to auto-insert entries for parent
   * directories, nor do we return fake entries in getObjectList when a delimiter implies a
   * pure-prefix match.
   */
  @Override
  public boolean containsEntriesForImplicitDirectories() {
    return false;
  }

  @Override
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

  @Override
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

  @Override
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

  @Override
  public synchronized List<CacheEntry> getBucketList() {
    log.debug("getBucketList()");
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

  @Override
  public synchronized List<CacheEntry> getRawBucketList() {
    log.debug("getRawBucketList()");
    return new ArrayList<CacheEntry>(bucketLookup.values());
  }

  @Override
  public synchronized List<CacheEntry> getObjectList(
      String bucketName, String objectNamePrefix, String delimiter, Set<String> returnedPrefixes) {
    log.debug("getObjectList(%s, %s, %s)", bucketName, objectNamePrefix, delimiter);
    CachedBucket bucket = bucketLookup.get(bucketName);
    if (bucket == null) {
      return null;
    }

    List<CacheEntry> matchingObjectEntries = new ArrayList<>();
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
        String objectName = objectEntry.getResourceId().getObjectName();
        String matchedName = GoogleCloudStorageStrings.matchListPrefix(
            objectNamePrefix, delimiter, objectName);
        // We get a non-null matchedName if either an implicit 'prefix' matches or if it's an
        // exact match.
        if (matchedName != null) {
          if (objectName.equals(matchedName)) {
            // Exact match.
            matchingObjectEntries.add(objectEntry);
          } else if (returnedPrefixes != null) {
            // Prefix match; only need to populate the container if the caller actually provided
            // a non-null container.
            returnedPrefixes.add(matchedName);
          }
        }
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
    return matchingObjectEntries;
  }

  @Override
  public synchronized int getInternalNumBuckets() {
    return bucketLookup.size();
  }

  @Override
  public synchronized int getInternalNumObjects() {
    int objectCount = 0;
    for (CachedBucket bucket : bucketLookup.values()) {
      objectCount += bucket.getNumObjects();
    }
    return objectCount;
  }
}
