/**
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * This class adds a caching layer in-between a GoogleCloudStorage instance, caching calls that
 * create, update, remove, and query for GoogleCloudStorageItemInfo. Those cached copies are
 * returned when requesting data through {@link GoogleCloudStorage#getItemInfo(StorageResourceId)}
 * and {@link GoogleCloudStorage#getItemInfo(StorageResourceId)}. This provides faster access to
 * recently queried data in the scope of this instance. Because the data is cached, modifications
 * made outside of this instance may not be immediately reflected.
 *
 * <p>Unlike {@link CacheSupplementedGoogleCloudStorage}, this class does not guarantee list
 * consistency. It can be composed with a CacheSupplementedGoogleCloudStorage in any order to
 * provide list consistency without conflict.
 */
public class PerformanceCachingGoogleCloudStorage extends ForwardingGoogleCloudStorage {

  /** Cache to hold item info and manage invalidation. */
  private final Cache<StorageResourceId, GoogleCloudStorageItemInfo> cache;

  /**
   * Creates a wrapper around a GoogleCloudStorage instance, caching calls that create, update,
   * remove, and query for GoogleCloudStorageItemInfo. Those cached copies are returned when
   * requesting data through {@link GoogleCloudStorage#getItemInfo(StorageResourceId)} and {@link
   * GoogleCloudStorage#getItemInfo(StorageResourceId)}. This provides faster access to recently
   * queried data in the scope of this instance. Because the data is cached, modifications made
   * outside of this instance may not be immediately reflected.
   *
   * @param delegate the {@link GoogleCloudStorage} instance to wrap and delegate calls to.
   * @param options the options to configure this cache with.
   */
  public PerformanceCachingGoogleCloudStorage(
      GoogleCloudStorage delegate, PerformanceCachingGoogleCloudStorageOptions options) {
    this(
        delegate,
        CacheBuilder.newBuilder()
            .expireAfterWrite(options.getMaxEntryAgeMills(), TimeUnit.MILLISECONDS)
            .build());
  }

  /**
   * Creates a wrapper around a GoogleCloudStorage instance, caching calls that create, update,
   * remove, and query for GoogleCloudStorageItemInfo. Those cached copies are returned when
   * requesting data through {@link GoogleCloudStorage#getItemInfo(StorageResourceId)} and {@link
   * GoogleCloudStorage#getItemInfo(StorageResourceId)}. This provides faster access to recently
   * queried data in the scope of this instance. Because the data is cached, modifications made
   * outside of this instance may not be immediately reflected.
   *
   * <p>This is visible for testing.
   *
   * @param delegate the {@link GoogleCloudStorage} instance to wrap and delegate calls to.
   * @param cache the {@link Cache} to store recently queried data in.
   */
  @VisibleForTesting
  PerformanceCachingGoogleCloudStorage(
      GoogleCloudStorage delegate, Cache<StorageResourceId, GoogleCloudStorageItemInfo> cache) {
    super(delegate);

    this.cache = cache;
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    super.deleteBuckets(bucketNames);

    // Remove objects that reside in deleted buckets.
    HashSet<String> bucketsHash = new HashSet<String>(bucketNames);
    Iterator<Entry<StorageResourceId, GoogleCloudStorageItemInfo>> iterator =
        cache.asMap().entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<StorageResourceId, GoogleCloudStorageItemInfo> entry = iterator.next();
      if (bucketsHash.contains(entry.getKey().getBucketName())) {
        iterator.remove();
      }
    }
  }

  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    super.deleteObjects(fullObjectNames);

    // Remove the deleted objects from cache.
    for (StorageResourceId resourceId : fullObjectNames) {
      cache.invalidate(resourceId);
    }
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    List<GoogleCloudStorageItemInfo> result = super.listBucketInfo();

    // Add the results to the cache.
    for (GoogleCloudStorageItemInfo item : result) {
      cache.put(item.getResourceId(), item);
    }

    return result;
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter) throws IOException {
    List<GoogleCloudStorageItemInfo> result =
        super.listObjectInfo(bucketName, objectNamePrefix, delimiter);

    // Add the results to the cache.
    for (GoogleCloudStorageItemInfo item : result) {
      cache.put(item.getResourceId(), item);
    }

    return result;
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    List<GoogleCloudStorageItemInfo> result =
        super.listObjectInfo(bucketName, objectNamePrefix, delimiter, maxResults);

    // Add the results to the cache.
    for (GoogleCloudStorageItemInfo item : result) {
      cache.put(item.getResourceId(), item);
    }

    return result;
  }

  /** This function may return cached copies of GoogleCloudStorageItemInfo. */
  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
    // Get the item from cache.
    GoogleCloudStorageItemInfo item = cache.getIfPresent(resourceId);

    // If it wasn't in the cache, request it then add it.
    if (item == null) {
      item = super.getItemInfo(resourceId);
      cache.put(item.getResourceId(), item);
    }

    return item;
  }

  /** This function may return cached copies of GoogleCloudStorageItemInfo. */
  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    List<GoogleCloudStorageItemInfo> result =
        new ArrayList<GoogleCloudStorageItemInfo>(resourceIds.size());
    List<StorageResourceId> request = new ArrayList<StorageResourceId>(resourceIds.size());

    // Populate the result list with items in the cache, and the request list with resources that
    // still need to be resolved. Null items are added to the result list to preserve ordering.
    for (StorageResourceId resourceId : resourceIds) {
      GoogleCloudStorageItemInfo item = cache.getIfPresent(resourceId);
      if (item == null) {
        request.add(resourceId);
      }
      result.add(item);
    }

    // Resolve all the resources which were not cached, cache them, and add them to the result list.
    // Null entries in the result list are replaced by the fresh entries from the underlying
    // GoogleCloudStorage.
    if (!request.isEmpty()) {
      List<GoogleCloudStorageItemInfo> response = super.getItemInfos(request);
      Iterator<GoogleCloudStorageItemInfo> responseIterator = response.iterator();

      // Iterate through the result set, replacing the null entries added previously with entries
      // from the response.
      for (int i = 0; i < result.size() && responseIterator.hasNext(); i++) {
        if (result.get(i) == null) {
          GoogleCloudStorageItemInfo item = responseIterator.next();
          cache.put(item.getResourceId(), item);
          result.set(i, item);
        }
      }
    }

    return result;
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    List<GoogleCloudStorageItemInfo> result = super.updateItems(itemInfoList);

    // Update the cache with the returned items. This overwrites the originals as the
    // StorageResourceIds of the items do not change in an update.
    for (GoogleCloudStorageItemInfo item : result) {
      cache.put(item.getResourceId(), item);
    }

    return result;
  }

  @Override
  public void close() {
    super.close();

    // Respect close and empty the cache.
    cache.invalidateAll();
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException {
    GoogleCloudStorageItemInfo item = super.composeObjects(sources, destination, options);

    // Cache the composed object.
    cache.put(item.getResourceId(), item);

    return item;
  }
}
