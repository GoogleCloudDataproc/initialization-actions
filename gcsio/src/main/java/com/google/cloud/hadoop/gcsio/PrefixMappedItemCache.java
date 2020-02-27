/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Comparator.naturalOrder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * A semi-persistent storage for {@link GoogleCloudStorageItemInfo} that maintains indexes based on
 * the item's bucket and object name. In addition to caching {@link StorageResourceId} to item
 * mappings, it provides options for storing groups of items under similar bucket and object name
 * prefixes.
 */
public class PrefixMappedItemCache {

  /** Map to hold item info. */
  private final TreeMap<PrefixKey, CacheValue<GoogleCloudStorageItemInfo>> itemMap;

  /** The time in nanoseconds before an entry expires. */
  private final long maxEntryAgeNanos;

  /** Ticker for tracking expiration. */
  private final Ticker ticker;

  /**
   * Creates a new {@link PrefixMappedItemCache}.
   *
   * @param maxEntryAge time after which entries in cache expire.
   */
  public PrefixMappedItemCache(Duration maxEntryAge) {
    this(Ticker.systemTicker(), maxEntryAge);
  }

  @VisibleForTesting
  PrefixMappedItemCache(Ticker ticker, Duration maxEntryAge) {
    this.itemMap = new TreeMap<>(PrefixKey.COMPARATOR);
    this.ticker = ticker;
    this.maxEntryAgeNanos = maxEntryAge.toNanos();
  }

  /**
   * Gets the cached item associated with the given resource id.
   *
   * @param id the resource id of the item to get.
   * @return the cached item associated with the given resource id, null if the item isn't cached or
   *     it has expired in the cache.
   */
  public synchronized GoogleCloudStorageItemInfo getItem(StorageResourceId id) {
    PrefixKey key = new PrefixKey(id.getBucketName(), id.getObjectName());
    CacheValue<GoogleCloudStorageItemInfo> value = itemMap.get(key);

    if (value == null) {
      return null;
    }

    if (isExpired(value)) {
      itemMap.remove(key);
      return null;
    }

    return value.getValue();
  }

  /**
   * Inserts an item into the cache. If an item with the same resource id is present, it is
   * overwritten by the new item.
   *
   * @param item the item to insert. The item must have a valid resource id.
   * @return the overwritten item, null if no item was overwritten.
   */
  public synchronized GoogleCloudStorageItemInfo putItem(GoogleCloudStorageItemInfo item) {
    StorageResourceId id = item.getResourceId();
    PrefixKey key = new PrefixKey(id.getBucketName(), id.getObjectName());
    CacheValue<GoogleCloudStorageItemInfo> value = new CacheValue<>(item, ticker.read());
    CacheValue<GoogleCloudStorageItemInfo> oldValue = itemMap.put(key, value);
    return oldValue == null || isExpired(oldValue) ? null : oldValue.getValue();
  }

  /**
   * Removes the item from the cache. If the item has expired, associated lists are invalidated.
   *
   * @param id the resource id of the item to remove.
   * @return the removed item, null if no item was removed.
   */
  public synchronized GoogleCloudStorageItemInfo removeItem(StorageResourceId id) {
    PrefixKey key = new PrefixKey(id.getBucketName(), id.getObjectName());
    CacheValue<GoogleCloudStorageItemInfo> value = itemMap.remove(key);
    if (id.isDirectory()) {
      getPrefixSubMap(itemMap, key).clear();
    }
    return value == null || isExpired(value) ? null : value.getValue();
  }

  /**
   * Invalidates all cached items and lists associated with the given bucket.
   *
   * @param bucket the bucket to invalidate. This must not be null.
   */
  public synchronized void invalidateBucket(String bucket) {
    PrefixKey key = new PrefixKey(bucket, "");

    getPrefixSubMap(itemMap, key).clear();
  }

  /** Invalidates all entries in the cache. */
  public synchronized void invalidateAll() {
    itemMap.clear();
  }

  /**
   * Checks if the {@link CacheValue} has expired.
   *
   * @param value the value to check.
   * @return true if the value has expired, false otherwise.
   */
  private <V> boolean isExpired(CacheValue<V> value) {
    long diff = ticker.read() - value.getCreationTimeNanos();
    return diff > maxEntryAgeNanos;
  }

  /**
   * Extracts all the cached values in a map.
   *
   * @param map the map to extract the cached values from.
   * @return a list of references to cached values in the map. If the is empty, and empty list is
   *     returned.
   */
  private static <K, V> List<V> aggregateCacheValues(Map<K, CacheValue<V>> map) {
    List<V> values = new ArrayList<>(map.size());
    for (Map.Entry<K, CacheValue<V>> entry : map.entrySet()) {
      values.add(entry.getValue().getValue());
    }
    return values;
  }

  /**
   * Helper function that handles creating the lower and upper bounds for calling {@link
   * SortedMap#subMap(Object, Object)}.
   *
   * @see SortedMap#subMap(Object, Object)
   */
  private static <E> SortedMap<PrefixKey, E> getPrefixSubMap(
      TreeMap<PrefixKey, E> map, PrefixKey lowerBound) {
    PrefixKey upperBound =
        new PrefixKey(lowerBound.getBucket(), lowerBound.getObjectName() + Character.MAX_VALUE);
    return map.subMap(lowerBound, upperBound);
  }

  /** Gets all the items in the item map without modifying the map. Used for testing only. */
  @VisibleForTesting
  List<GoogleCloudStorageItemInfo> getAllItemsRaw() {
    return aggregateCacheValues(itemMap);
  }

  /**
   * Tuple of a value and a creation time in nanoseconds.
   *
   * @param <V> the type of the value being cached.
   */
  private static class CacheValue<V> {
    /** The value being cached. */
    private final V value;

    /** The time the entry was created in nanoseconds. */
    private final long creationTimeNanos;

    /**
     * Creates a new {@link CacheValue}.
     *
     * @param value the value being cached.
     * @param creationTimeNanos the time the entry was created in nanoseconds.
     */
    public CacheValue(V value, long creationTimeNanos) {
      this.value = value;
      this.creationTimeNanos = creationTimeNanos;
    }

    /** Gets the value being cached. */
    public V getValue() {
      return value;
    }

    /** Gets the time the entry was created in nanoseconds. */
    public long getCreationTimeNanos() {
      return creationTimeNanos;
    }

    @Override
    public String toString() {
      return "CacheValue [value=" + value + ", creationTimeNanos=" + creationTimeNanos + "]";
    }
  }

  /** A class that represents a unique key for an entry in the prefix cache. */
  private static class PrefixKey implements Comparable<PrefixKey> {

    /**
     * Instance of a comparator that compares {@link PrefixKey}'s. This is provided for the TreeMap
     * to off-load to for performance reasons. This throws a NullPointerException if either of the
     * entries being compared are null.
     */
    public static final Comparator<PrefixKey> COMPARATOR = naturalOrder();

    /** The bucket for the entry. */
    private final String bucket;

    /** The object name for the entry. */
    private final String objectName;

    /**
     * Creates a new {@link PrefixKey}.
     *
     * @param bucket the bucket for the entry. If this string is null, it is converted to empty
     *     string.
     * @param objectName the object name for the entry. If this string is null, it is converted to
     *     empty string.
     * @throws IllegalArgumentException if the bucket is null and object is not null.
     */
    public PrefixKey(String bucket, @Nullable String objectName) {
      checkArgument(
          bucket != null || objectName == null, "bucket must not be null if object is not null.");
      this.bucket = nullToEmpty(bucket);
      this.objectName = nullToEmpty(objectName);
    }

    /** Gets the bucket for the entry. */
    public String getBucket() {
      return bucket;
    }

    /** Gets the object name for the entry. */
    public String getObjectName() {
      return objectName;
    }

    @Override
    public String toString() {
      return "PrefixKey [bucket=" + bucket + ", objectName=" + objectName + "]";
    }

    /**
     * Checks if the given {@link PrefixKey} is a parent of this. A parent exists in the same bucket
     * and its object name is a prefix to this one.
     */
    public boolean isParent(PrefixKey other) {
      return bucket.equals(other.bucket) && objectName.startsWith(other.objectName);
    }

    /**
     * Compares based on bucket, then by object name if the buckets are equal.
     *
     * @throws NullPointerException if the other {@link PrefixKey} is null.
     */
    @Override
    public int compareTo(PrefixKey other) {
      int result = bucket.compareTo(other.bucket);
      return result == 0 ? objectName.compareTo(other.objectName) : result;
    }

    /** Generates hash based on bucket and objectName. */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + bucket.hashCode();
      result = prime * result + objectName.hashCode();
      return result;
    }

    /** Compares by bucket and objectName. */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof PrefixKey)) {
        return false;
      }

      PrefixKey other = (PrefixKey) obj;

      return bucket.equals(other.bucket) && objectName.equals(other.objectName);
    }
  }
}
