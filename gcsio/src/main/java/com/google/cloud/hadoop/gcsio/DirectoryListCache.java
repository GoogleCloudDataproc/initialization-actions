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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DirectoryListCache is an abstract class providing supplemental accounting of full paths for
 * directories and files created and deleted in GoogleCloudStorageFileSystem. It serves primarily
 * as a supplemental list index for underlying Storage.list() requests, because the remote listing
 * is updated asynchronously, so that recently-created files may not appear in a subsequent listing,
 * even if the request is made by the same client which created the file.
 * <p>
 * Depending on the extent to which the underlying data structures accessed by implementations of
 * this class are shared, callers can guarantee varying levels of immediate list consistency
 * between file-creation and a subsequent "list" request.
 * <p>
 * Implementations of this class must be thread-safe.
 */
public abstract class DirectoryListCache {
  // TODO(user): Actually add support for delete-followed-by-list, cache-removal on 404, and
  // cache-blacklist-removal on non-404 of what we think is a "deleted" entry.
  // Logger.
  private static final Logger LOG = LoggerFactory.getLogger(DirectoryListCache.class);

  // Clock instance used for calculating expiration times.
  protected Clock clock = Clock.SYSTEM;

  // The configuration settings for this DirectlyListCache instance.
  protected Config cacheConfig = new Config();

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
    public static final long MAX_ENTRY_AGE_MILLIS_DEFAULT = 4 * 60 * 60 * 1000L;

    // Maximum number of milliseconds a GoogleCloudStorageItemInfo will remain "valid" in the cache,
    // after which the next attempt to fetch the itemInfo will require fetching fresh info from
    // a GoogleCloudStorage instance.
    public static final long MAX_INFO_AGE_MILLIS_DEFAULT = 10 * 1000L;

    private long maxEntryAgeMillis = MAX_ENTRY_AGE_MILLIS_DEFAULT;
    private long maxInfoAgeMillis = MAX_INFO_AGE_MILLIS_DEFAULT;

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

  /**
   * Known types of DirectoryListCache implementations.
   */
  public static enum Type {
    IN_MEMORY,
    FILESYSTEM_BACKED
  }

  /**
   * Implementations should indicate whether or not CacheEntry objects returned from methods are
   * intended to be shared references that are authoritative cache state regarding metadata
   * like cached GoogleCloudStorageItemInfo, etc. If true, then the implementation implicitly
   * supports caching of GoogleCloudStorageItemInfo, and if false, the implication is that info
   * caching is *not* supported. Likewise, if 'true', CacheEntries are implied to be shared
   * references with their own synchronization, so that multiple entry holders may pass info
   * from one holder to another without re-querying the DirectoryListCache. If 'false', then each
   * CacheEntry logically represents a wrapper around some other source of authoritative
   * information, and thus references should not be assumed to be shared, and cached info should
   * not be expected to persist between CacheEntry instances corresponding to the same
   * StorageResourceId.
   */
  @VisibleForTesting
  public abstract boolean supportsCacheEntryByReference();

  /**
   * Implementations should indicate whether implicit directories are automatically detected and
   * added as CacheEntry items when putResourceId is called without first adding explicit
   * directory objects for the implied parent directories of the added object.
   */
  @VisibleForTesting
  public abstract boolean containsEntriesForImplicitDirectories();

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
  public abstract CacheEntry putResourceId(StorageResourceId resourceId) throws IOException;

  /**
   * Returns the CacheEntry associated with {@code resourceId}, or null if it doesn't exist.
   * This returns the real mutable CacheEntry (rather than a copy of the data) so that the
   * caller may efficiently update the info stored in the CacheEntry if necessary.
   */
  public abstract CacheEntry getCacheEntry(StorageResourceId resourceId) throws IOException;

  /**
   * Removes CacheEntry associated with {@code resourceId}, if it exists. Cached
   * GoogleCloudStorageItemInfo associated with the resourceId is also removed, if it exists.
   * If {@code resourceId} denotes a non-empty bucket, then all the cached StorageObject children
   * of that bucket will also be removed from the cache; it is the caller's responsibility to
   * ensure that the bucket should really be removed. Note that normal expiration of a CachedBucket
   * will *not* remove the actual CachedBucket, even though the bucketName will stop appearing
   * in calls to getBucketList().
   */
  public abstract void removeResourceId(StorageResourceId resourceId) throws IOException;

  /**
   * @return List of CacheEntry corresponding to Buckets known by this cache; includes buckets
   *     added to the cache automatically when caching a StorageObject contained in the
   *     corresponding bucket. Will not return null. Hides/removes expired bucket entries and
   *     clears any expired GoogleCloudStorageItemInfo associated with buckets.
   */
  public abstract List<CacheEntry> getBucketList() throws IOException;

  /**
   * @return List of *all* Bucket CacheEntries, including ones that might be expired. Doesn't
   *     actively invalidate, set expiration, or delete expired entries. Can be used when the
   *     internal bucket list is needed without wanting to cause any mutations in the cache.
   */
  public abstract List<CacheEntry> getRawBucketList() throws IOException;

  /**
   * @param bucketName The bucket inside of which to list objects.
   * @param objectNamePrefix The prefix to be used to match object names to return.
   * @param delimiter The character for specifying 'directory' boundaries, or null.
   * @param returnedPrefixes A container to be populated with implied "directory objects" that
   *     come from some object that includes the prefix, followed by some string, then followed
   *     by the 'delimiter'. This may or may not be a duplicate of one of the actual returned
   *     directory objects. For example, if gs://foo/bar/baz.txt exists and we query with parameters
   *     ("foo", "ba", "/", {}) then the returnedPrefixes will be populated with the string
   *     "bar/" by virtue of existence of the "bar/baz.txt" file. May be null if the caller doesn't
   *     desire fetching such returnedPrefixes.
   *
   * @return List of CacheEntrys for StorageObjects residing in bucket {@code bucketName} that
   *     match the provided objectNamePrefix and delimiter, or possibly null if no such objects
   *     are present. May also return an empty list.
   */
  public abstract List<CacheEntry> getObjectList(
      String bucketName, String objectNamePrefix, String delimiter, Set<String> returnedPrefixes)
      throws IOException;

  /**
   * Gets the internal number of CachedBucket entries, which may not be equal to the size of
   * getBucketList() if there are expired entries. Does not mutate the cache.
   */
  @VisibleForTesting
  public abstract int getInternalNumBuckets() throws IOException;

  /**
   * Gets the internal total count of cached StorageObject entries. Does not mutate the cache.
   */
  @VisibleForTesting
  public abstract int getInternalNumObjects() throws IOException;

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
   * Sets the static Clock instance used for calculating expiration times.
   */
  @VisibleForTesting
  public synchronized void setClock(Clock clock) {
    this.clock = clock;
  }

  /**
   * Helper for determining whether a CacheEntry is entirely expired and should be removed
   * from the cache.
   */
  protected synchronized boolean isCacheEntryExpired(CacheEntry entry) {
    long creationTime = entry.getCreationTimeMillis();
    long entryAge = clock.currentTimeMillis() - creationTime;
    return entryAge > cacheConfig.getMaxEntryAgeMillis();
  }


  /**
   * Helper to validate {@code resourceId} that may be a Bucket or StorageObject.
   */
  protected static void validateResourceId(StorageResourceId resourceId) {
    Preconditions.checkArgument(resourceId != null,
        "DirectoryListCache requires non-null resourceId.");
    Preconditions.checkArgument(!resourceId.isRoot(),
        "DirectoryListCache cannot be used to cache ROOT info.");
  }

  /**
   * Helper for possibly clearing the GoogleCloudStorageItemInfo stored in {@code entry} based
   * on cacheConfig settings.
   */
  protected void maybeInvalidateExpiredInfo(CacheEntry entry) {
    long currentTimeMillis;
    long maxInfoAgeMillis;
    synchronized (this) {
      currentTimeMillis = clock.currentTimeMillis();
      maxInfoAgeMillis = cacheConfig.getMaxInfoAgeMillis();
    }

    // We must synchronize on 'entry' since we are reading its itemInfoUpdateTimeMillis and then
    // possibly mutating it based on that value. Requires that CacheEntry's other mutators like
    // setItemInfo are also synchronized at the object-instance level.
    synchronized (entry) {
      long lastUpdated = entry.getItemInfoUpdateTimeMillis();
      long infoAge = currentTimeMillis - lastUpdated;
      if (lastUpdated > 0 && infoAge > maxInfoAgeMillis) {
        LOG.debug("Clearing itemInfo for CacheEntry '{}' with infoAge: {} ms",
            entry.getResourceId(), infoAge);
        entry.clearItemInfo();
      }
    }
  }
}
