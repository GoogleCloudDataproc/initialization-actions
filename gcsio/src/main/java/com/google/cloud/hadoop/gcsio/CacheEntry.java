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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container for various pieces of metadata for a single cache entry, which may be either a Bucket
 * or a StorageObject; includes GCS API metadata such as StorageResourceId and
 * GoogleCloudStorageItemInfo as well as cache-specific metadata, such as creation time,
 * and last-updated time.
 */
public class CacheEntry {
  // Logger.
  private static final Logger LOG = LoggerFactory.getLogger(CacheEntry.class);

  // Clock instance used for calculating cache creation time and updated time.
  private static Clock clock = Clock.SYSTEM;

  // The resourceId of the GCS Bucket or StorageObject associated with this cache entry; non-null.
  private final StorageResourceId resourceId;

  // Creation time of this cache entry (*not* the creation time of the underlying GCS resource)
  // in milliseconds since January 1, 1970 UTC.
  private final long creationTimeMillis;

  // The metadata for the GCS Bucket or StorageObject associated with this cache entry; may be null
  // since the metadata is only populated lazily.
  private GoogleCloudStorageItemInfo itemInfo;

  // Time at which we last populated itemInfo, in milliseconds since January 1, 1970 ETC.
  // Might be 0 if the info was never retrieved.
  private long itemInfoUpdateTimeMillis;

  /**
   * Constructs a CacheEntry with no known GoogleCloudStorageItemInfo; callers may have to
   * fetch the associated GoogleCloudStorageItemInfo on-demand.
   *
   * @param resourceId Must be non-null, and correspond to either a Bucket or StorageObject.
   */
  public CacheEntry(StorageResourceId resourceId) {
    Preconditions.checkArgument(resourceId != null,
        "CacheEntry requires non-null resourceId.");
    Preconditions.checkArgument(!resourceId.isRoot(),
        "CacheEntry cannot take a resourceId corresponding to 'root'.");

    this.resourceId = resourceId;
    this.creationTimeMillis = clock.currentTimeMillis();
    this.itemInfo = null;
    this.itemInfoUpdateTimeMillis = 0;
  }

  /**
   * Constructs a CacheEntry with no known GoogleCloudStorageItemInfo and an explicit
   * creationTimeMillis; callers may have to fetch the associated GoogleCloudStorageItemInfo
   * on-demand. This should be used for implementations where the in-memory CacheEntry objects
   * are not the authoritative listing, and presumably the cache-entry creation times are
   * stored somewhere else, e.g. on a filesystem.
   *
   * @param resourceId Must be non-null, and correspond to either a Bucket or StorageObject.
   * @param creationTimeMillis The logical creation time of the authoritative cache entry.
   */
  public CacheEntry(StorageResourceId resourceId, long creationTimeMillis) {
    Preconditions.checkArgument(resourceId != null,
        "CacheEntry requires non-null resourceId.");
    Preconditions.checkArgument(!resourceId.isRoot(),
        "CacheEntry cannot take a resourceId corresponding to 'root'.");

    this.resourceId = resourceId;
    this.creationTimeMillis = creationTimeMillis;
    this.itemInfo = null;
    this.itemInfoUpdateTimeMillis = 0;
  }

  /**
   * @param itemInfo A last-known itemInfo associated to be held and returned by this CacheEntry;
   *     must be non-null, must be a Bucket or StorageObject, and exists() must return true.
   */
  public CacheEntry(GoogleCloudStorageItemInfo itemInfo) {
    validateItemInfo(itemInfo);

    this.resourceId = itemInfo.getResourceId();
    this.creationTimeMillis = clock.currentTimeMillis();
    this.itemInfo = itemInfo;
    this.itemInfoUpdateTimeMillis = this.creationTimeMillis;
  }

  /**
   * Helper for checking basic requirements of a GoogleCloudStorageItemInfo given to a CacheEntry;
   * namely, that it's non-null, isn't ROOT, and that it exists.
   */
  private static void validateItemInfo(GoogleCloudStorageItemInfo itemInfo) {
    Preconditions.checkArgument(itemInfo != null,
        "CacheEntry requires non-null itemInfo.");
    Preconditions.checkArgument(!itemInfo.isRoot(),
        "CacheEntry cannot take an itemInfo corresponding to 'root'.");
    Preconditions.checkArgument(itemInfo.exists(),
        "CacheEntry cannot take an itemInfo for which exists() == false.");
  }

  /**
   * Sets a custom Clock to be used for computing cache creation and update times.
   */
  @VisibleForTesting
  public static void setClock(Clock clock) {
    CacheEntry.clock = clock;
  }

  /**
   * Returns the StorageResourceId associated with this CacheEntry; may identify a Bucket or
   * StorageObject.
   */
  public StorageResourceId getResourceId() {
    return resourceId;
  }

  /**
   * Accessor for the creation-time of this CacheEntry, which was set at construction-time and is
   * immutable.
   */
  public long getCreationTimeMillis() {
    return creationTimeMillis;
  }

  /**
   * Accessor for the last time the GoogleCloudStorageItemInfo of this CacheEntry was updated,
   * or 0 if it was never updated.
   */
  public synchronized long getItemInfoUpdateTimeMillis() {
    return itemInfoUpdateTimeMillis;
  }

  /**
   * Returns the GoogleCloudStorageItemInfo currently held by this CacheEntry; may be null if one
   * was never provided.
   */
  public synchronized GoogleCloudStorageItemInfo getItemInfo() {
    return itemInfo;
  }

  /**
   * Clears the GoogleCloudStorageItemInfo stored by this CacheEntry, if any, and sets
   * itemInfoUpdateTimeMillis to 0.
   */
  public synchronized void clearItemInfo() {
    itemInfo = null;
    itemInfoUpdateTimeMillis = 0;
  }

  /**
   * Sets the GoogleCloudStorageItemInfo corresponding to this CacheEntry's StorageResourceId,
   * and updates the itemInfoUpdateTimeMillis. Returns the old info, or null if it was never set
   * before.
   *
   * @param newItemInfo Info corresponding to this entry's Storage resource, must not be null,
   *     must not be root, and the info's StorageResourceId must match the existing
   *     StorageResourceId of this CacheEntry.
   */
  public synchronized GoogleCloudStorageItemInfo setItemInfo(
      GoogleCloudStorageItemInfo newItemInfo) {
    validateItemInfo(newItemInfo);
    Preconditions.checkArgument(
        newItemInfo.getResourceId().equals(resourceId),
        "newItemInfo's resourceId (%s) doesn't match existing resourceId (%s)!",
        newItemInfo.getResourceId(), resourceId);

    if (itemInfo != null) {
      // TODO(user): Maybe skip the update if the newItemInfo has an older creationTime than
      // the existing itemInfo.
      LOG.debug("Replacing existing itemInfo '{}' with newItemInfo '{}'", itemInfo, newItemInfo);
    } else {
      LOG.debug("Setting itemInfo for first time for cache entry: '{}'", newItemInfo);
    }
    GoogleCloudStorageItemInfo oldInfo = itemInfo;
    itemInfo = newItemInfo;
    itemInfoUpdateTimeMillis = clock.currentTimeMillis();
    return oldInfo;
  }
}
