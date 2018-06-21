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

import static com.google.common.base.Strings.nullToEmpty;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
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

  /** Map to hold known prefixes. */
  private final TreeMap<PrefixKey, CacheValue<Object>> prefixMap;

  /** The time in nanoseconds before an entry expires. */
  private final long maxEntryAgeNanos;

  /** Ticker for tracking expirations. */
  private final Ticker ticker;

  /**
   * Creates a new {@link PrefixMappedItemCache}.
   *
   * @param config the configuration parameters to initialize the cache with. The configuration
   *     parameters are copied during construction, future modifications to the configuration will
   *     not be reflected in the cache.
   */
  public PrefixMappedItemCache(Config config) {
    itemMap = new TreeMap<PrefixKey, CacheValue<GoogleCloudStorageItemInfo>>(PrefixKey.COMPARATOR);
    prefixMap = new TreeMap<PrefixKey, CacheValue<Object>>(PrefixKey.COMPARATOR);

    maxEntryAgeNanos =
        TimeUnit.NANOSECONDS.convert(config.getMaxEntryAgeMillis(), TimeUnit.MILLISECONDS);
    ticker = config.getTicker();
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
      cleanupLists(key);
      return null;
    }

    return value.getValue();
  }

  /**
   * Gets the cached list associated with the given bucket and object name prefix.
   *
   * @param bucket the bucket where the entries are being retrieved from.
   * @param objectNamePrefix the object name prefix to match against. If this is empty string or
   *     null, it will match everything in the bucket.
   * @return a list of items matching in the given bucket matching the given object name prefix,
   *     null if a list isn't found or the is expired.
   */
  public synchronized List<GoogleCloudStorageItemInfo> getList(
      String bucket, @Nullable String objectNamePrefix) {
    PrefixKey key = new PrefixKey(bucket, objectNamePrefix);
    Entry<PrefixKey, CacheValue<Object>> entry = getParentEntry(prefixMap, key);

    if (entry == null) {
      return null;
    }

    if (isExpired(entry.getValue())) {
      cleanupLists(key);
      return null;
    }

    return aggregateCacheValues(getPrefixSubMap(itemMap, key));
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
    CacheValue<GoogleCloudStorageItemInfo> value =
        new CacheValue<GoogleCloudStorageItemInfo>(item, ticker.read());

    CacheValue<GoogleCloudStorageItemInfo> oldValue = itemMap.put(key, value);

    if (oldValue == null) {
      return null;
    }

    if (isExpired(oldValue)) {
      cleanupLists(key);
      return null;
    }

    return oldValue.getValue();
  }

  /**
   * Inserts a list entry and the given items into the cache. If a list entry under the same
   * bucket/objectNamePrefix is present, its expiration time is reset. If an item with the same
   * resource id is present, it is overwritten by the new item.
   *
   * @param bucket the bucket to index the items by.
   * @param objectNamePrefix the object name prefix to index the items by. If this is null, it will
   *     be converted to empty string.
   * @param items the list of items to insert.
   */
  public synchronized void putList(
      String bucket, @Nullable String objectNamePrefix, List<GoogleCloudStorageItemInfo> items) {
    // Give all entries the same creation time so they expire at the same time.
    long creationTime = ticker.read();
    PrefixKey key = new PrefixKey(bucket, objectNamePrefix);

    // The list being inserted is always fresher than any child lists.
    getPrefixSubMap(itemMap, key).clear();
    getPrefixSubMap(prefixMap, key).clear();

    // Populate the maps.
    prefixMap.put(key, new CacheValue<Object>(null, creationTime));
    for (GoogleCloudStorageItemInfo item : items) {
      StorageResourceId itemId = item.getResourceId();
      PrefixKey itemKey = new PrefixKey(itemId.getBucketName(), itemId.getObjectName());
      CacheValue<GoogleCloudStorageItemInfo> itemValue =
          new CacheValue<GoogleCloudStorageItemInfo>(item, creationTime);
      itemMap.put(itemKey, itemValue);
    }
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

    if (value == null) {
      return null;
    }

    if (isExpired(value)) {
      cleanupLists(key);
      return null;
    }

    return value.getValue();
  }

  /**
   * Invalidates all cached items and lists associated with the given bucket.
   *
   * @param bucket the bucket to invalidate. This must not be null.
   */
  public synchronized void invalidateBucket(String bucket) {
    PrefixKey key = new PrefixKey(bucket, "");

    getPrefixSubMap(itemMap, key).clear();
    getPrefixSubMap(prefixMap, key).clear();
  }

  /** Invalidates all entries in the cache. */
  public synchronized void invalidateAll() {
    itemMap.clear();
    prefixMap.clear();
  }

  /**
   * Checks if the {@link CacheValue} has expired.
   *
   * @param value the value to check.
   * @return true if the value has expired, false otherwise.
   */
  private <V> boolean isExpired(CacheValue<V> value) {
    long diff = ticker.read() - value.getCreationTime();
    return diff > maxEntryAgeNanos;
  }

  /**
   * Removes expired list entries that contain the given key. If a list was removed, it's contained
   * items are checked for expiration too.
   *
   * @param key the key to cleanup the list entries for.
   */
  private void cleanupLists(PrefixKey key) {
    // Remove expired list entries. Keep track of the last list removed.
    NavigableMap<PrefixKey, CacheValue<Object>> head = prefixMap.headMap(key, true).descendingMap();
    Iterator<Entry<PrefixKey, CacheValue<Object>>> headItr = head.entrySet().iterator();
    Entry<PrefixKey, CacheValue<Object>> last = null;

    while (headItr.hasNext()) {
      Entry<PrefixKey, CacheValue<Object>> entry = headItr.next();
      if (isExpired(entry.getValue()) && key.isParent(entry.getKey())) {
        last = entry;
        headItr.remove();
      }
    }

    // If a list was removed, check its contained items for expiration.
    if (last != null) {
      SortedMap<PrefixKey, CacheValue<GoogleCloudStorageItemInfo>> prefix =
          getPrefixSubMap(itemMap, last.getKey());
      Iterator<Entry<PrefixKey, CacheValue<GoogleCloudStorageItemInfo>>> prefixItr =
          prefix.entrySet().iterator();

      while (prefixItr.hasNext()) {
        Entry<PrefixKey, CacheValue<GoogleCloudStorageItemInfo>> entry = prefixItr.next();
        if (isExpired(entry.getValue())) {
          prefixItr.remove();
        }
      }
    }
  }

  /**
   * Extracts all the cached values in a map.
   *
   * @param map the map to extract the cached values from.
   * @return a list of references to cached values in the map. If the is empty, and empty list is
   *     returned.
   */
  private static <K, V> List<V> aggregateCacheValues(Map<K, CacheValue<V>> map) {
    List<V> values = new ArrayList<V>(map.size());
    Iterator<Entry<K, CacheValue<V>>> itr = map.entrySet().iterator();
    while (itr.hasNext()) {
      values.add(itr.next().getValue().getValue());
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

  /**
   * Helper function that finds the parent entry (inclusive) for the given upper bound. This is like
   * {@link TreeMap#floorEntry(Object)}, but does additional filtering.
   *
   * @param map the map to find the parent entry in.
   * @param upperBound the upper bound to search for a parent.
   * @return the parent entry for the given upper bound, null if no entry was found.
   */
  private static <E> Entry<PrefixKey, E> getParentEntry(
      TreeMap<PrefixKey, E> map, PrefixKey upperBound) {
    NavigableMap<PrefixKey, E> head = map.headMap(upperBound, true).descendingMap();
    for (Entry<PrefixKey, E> entry : head.entrySet()) {
      if (upperBound.isParent(entry.getKey())) {
        return entry;
      }
    }
    return null;
  }

  /** Gets all the items in the item map without modifying the map. Used for testing only. */
  @VisibleForTesting
  List<GoogleCloudStorageItemInfo> getAllItemsRaw() {
    return aggregateCacheValues(itemMap);
  }

  /** Checks if the prefix map contains an exact entry for the given bucket/objectName. */
  @VisibleForTesting
  boolean containsListRaw(String bucket, String objectName) {
    return prefixMap.containsKey(new PrefixKey(bucket, objectName));
  }

  /**
   * Container for various cache-configuration parameters used by a {@link PrefixMappedItemCache}
   * when managing expiration/retention policies, etc.
   */
  public static class Config {
    /** The time in milliseconds before an entry expires. */
    private long maxEntryAgeMillis;

    /** Ticker for tracking expirations. */
    private Ticker ticker = Ticker.systemTicker();

    /** Gets the time in milliseconds before an entry expires. */
    public long getMaxEntryAgeMillis() {
      return maxEntryAgeMillis;
    }

    /** Sets the time in milliseconds before an entry expires. */
    public void setMaxEntryAgeMillis(long maxEntryAgeMillis) {
      this.maxEntryAgeMillis = maxEntryAgeMillis;
    }

    /** Gets the ticker for tracking expirations. */
    public Ticker getTicker() {
      return ticker;
    }

    /** Sets the ticker for tracking expirations. */
    public void setTicker(Ticker ticker) {
      this.ticker = ticker;
    }
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
    private final long creationTime;

    /**
     * Creates a new {@link CacheValue}.
     *
     * @param value the value being cached.
     * @param creationTime the time the entry was created in nanoseconds.
     */
    public CacheValue(V value, long creationTime) {
      this.value = value;
      this.creationTime = creationTime;
    }

    /** Gets the value being cached. */
    public V getValue() {
      return value;
    }

    /** Gets the time the entry was created in nanoseconds. */
    public long getCreationTime() {
      return creationTime;
    }

    @Override
    public String toString() {
      return "CacheValue [value=" + value + ", creationTime=" + creationTime + "]";
    }
  }

  /** A class that represents a unique key for an entry in the prefix cache. */
  private static class PrefixKey implements Comparable<PrefixKey> {

    /**
     * Instance of a comparator that compares {@link PrefixKey}'s. This is provided for the TreeMap
     * to off-load to for performance reasons. This throws a NullPointerException if either of the
     * entries being compared are null.
     */
    public static final Comparator<PrefixKey> COMPARATOR =
        new Comparator<PrefixKey>() {
          @Override
          public int compare(PrefixKey a, PrefixKey b) {
            return a.compareTo(b);
          }
        };

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
      Preconditions.checkArgument(
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

      if (!bucket.equals(other.bucket) || !objectName.equals(other.objectName)) {
        return false;
      }

      return true;
    }
  }
}
