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

import static com.google.api.client.util.Strings.isNullOrEmpty;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.inferOrFilterNotRepairedInfos;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class adds a caching layer around a GoogleCloudStorage instance, caching calls that create,
 * update, remove, and query for GoogleCloudStorageItemInfo. Those cached copies are returned when
 * requesting data through {@link GoogleCloudStorage#getItemInfo(StorageResourceId)} and {@link
 * GoogleCloudStorage#getItemInfo(StorageResourceId)}. This provides faster access to recently
 * queried data in the scope of this instance. Because the data is cached, modifications made
 * outside of this instance may not be immediately reflected.
 */
public class PerformanceCachingGoogleCloudStorage extends ForwardingGoogleCloudStorage {

  private static final Logger LOG =
      LoggerFactory.getLogger(PerformanceCachingGoogleCloudStorage.class);

  /** Cache to hold item info and manage invalidation. */
  private final PrefixMappedItemCache cache;

  /** Setting for enabling inferring of implicit directories. */
  private final boolean inferImplicitDirectoriesEnabled;

  /** Setting for enabling list caching. */
  private final boolean listCachingEnabled;

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
    super(delegate);

    this.inferImplicitDirectoriesEnabled = options.isInferImplicitDirectoriesEnabled();
    this.listCachingEnabled = options.isListCachingEnabled();

    // Create the cache configuration.
    PrefixMappedItemCache.Config config = new PrefixMappedItemCache.Config();
    config.setMaxEntryAgeMillis(options.getMaxEntryAgeMillis());
    this.cache = new PrefixMappedItemCache(config);
  }

  @VisibleForTesting
  PerformanceCachingGoogleCloudStorage(
      GoogleCloudStorage delegate,
      PerformanceCachingGoogleCloudStorageOptions options,
      PrefixMappedItemCache cache) {
    super(delegate);
    this.inferImplicitDirectoriesEnabled = options.isInferImplicitDirectoriesEnabled();
    this.listCachingEnabled = options.isListCachingEnabled();
    this.cache = cache;
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    super.deleteBuckets(bucketNames);

    // Remove objects that reside in deleted buckets.
    for (String bucket : bucketNames) {
      cache.invalidateBucket(bucket);
    }
  }

  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    super.deleteObjects(fullObjectNames);

    // Remove the deleted objects from cache.
    for (StorageResourceId id : fullObjectNames) {
      cache.removeItem(id);
    }
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    List<GoogleCloudStorageItemInfo> result = super.listBucketInfo();

    // Add the results to the cache.
    for (GoogleCloudStorageItemInfo item : result) {
      cache.putItem(item);
    }

    return result;
  }

  /** This function may return cached copies of GoogleCloudStorageItemInfo. */
  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter) throws IOException {

    return this.listObjectInfo(
        bucketName, objectNamePrefix, delimiter, GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  /** This function may return cached copies of GoogleCloudStorageItemInfo. */
  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    List<GoogleCloudStorageItemInfo> result;

    if (listCachingEnabled) {
      result = cache.getList(bucketName, objectNamePrefix);

      if (result == null) {
        result = super.listObjectInfo(bucketName, objectNamePrefix, null);
        cache.putList(bucketName, objectNamePrefix, result);
      }

      if (GoogleCloudStorage.PATH_DELIMITER.equals(delimiter)) {
        filter(result, bucketName, objectNamePrefix, delimiter);
      }

      if (maxResults > 0 && result.size() > maxResults) {
        result = result.subList(0, (int) maxResults);
      }
    } else {
      result = super.listObjectInfo(bucketName, objectNamePrefix, delimiter, maxResults);
      for (GoogleCloudStorageItemInfo item : result) {
        cache.putItem(item);
      }
    }
    return result;
  }

  /**
   * Matches Google Cloud Storage's delimiter filtering.
   *
   * @param items the mutable list of items to filter. Items matching the filter conditions will be
   *     removed from this list.
   * @param bucketName the bucket name to filter for.
   * @param prefix the object name prefix to filter for.
   * @param delimiter the delimiter to filter on.
   */
  private void filter(
      List<GoogleCloudStorageItemInfo> items,
      String bucketName,
      @Nullable String prefix,
      @Nullable String delimiter)
      throws IOException {
    prefix = nullToEmpty(prefix);
    if (isNullOrEmpty(delimiter)) {
      return;
    }

    HashSet<String> dirs = new HashSet<>();
    Iterator<GoogleCloudStorageItemInfo> itr = items.iterator();
    while (itr.hasNext()) {
      String objectName = itr.next().getObjectName();

      // Remove if doesn't start with the prefix.
      if (!objectName.startsWith(prefix)) {
        itr.remove();
      } else if (prefix.endsWith(delimiter) && objectName.equals(prefix)) {
        // Remove prefix object if it ends with delimiter:
        // do not return prefix dir to avoid infinite recursion.
        itr.remove();
      } else {
        // Retain if missing the delimiter after the prefix.
        int firstIndex = objectName.indexOf(delimiter, prefix.length());
        if (firstIndex != -1) {
          // Remove if the first occurrence of the delimiter after the prefix isn't the last.
          // Remove if the last occurrence of the delimiter isn't the end of the string.
          int lastIndex = objectName.lastIndexOf(delimiter);
          if (firstIndex != lastIndex || lastIndex != objectName.length() - 1) {
            itr.remove();
            dirs.add(objectName.substring(0, firstIndex + 1));
          }
        }
      }
    }

    // Remove non-implicit directories (i.e. have corresponding directory objects)
    for (GoogleCloudStorageItemInfo item : items) {
      dirs.remove(item.getObjectName());
    }

    if (dirs.isEmpty()) {
      return;
    }

    List<StorageResourceId> dirIds = new ArrayList<>(dirs.size());
    for (String dir : dirs) {
      dirIds.add(new StorageResourceId(bucketName, dir));
    }

    if (getDelegate().getOptions().isAutoRepairImplicitDirectoriesEnabled()) {
      try {
        if (dirIds.size() == 1) {
          createEmptyObject(dirIds.get(0));
        } else {
          createEmptyObjects(dirIds);
        }
      } catch (IOException ioe) {
        // Don't totally fail the listObjectInfo call, since auto-repair is best-effort anyways.
        LOG.error("Failed to repair some missing directories.", ioe);
      }
      // cache repaired dirs
      List<GoogleCloudStorageItemInfo> repairedDirInfos = getItemInfos(dirIds);
      items.addAll(
          inferOrFilterNotRepairedInfos(repairedDirInfos, inferImplicitDirectoriesEnabled));
    } else if (inferImplicitDirectoriesEnabled) {
      for (StorageResourceId dirId : dirIds) {
        items.add(GoogleCloudStorageItemInfo.createInferredDirectory(dirId));
      }
    }
  }

  /** This function may return cached copies of GoogleCloudStorageItemInfo. */
  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
    // Get the item from cache.
    GoogleCloudStorageItemInfo item = cache.getItem(resourceId);

    // If it wasn't in the cache, request it then add it.
    if (item == null) {
      item = super.getItemInfo(resourceId);
      cache.putItem(item);
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
      GoogleCloudStorageItemInfo item = cache.getItem(resourceId);
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
          cache.putItem(item);
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
      cache.putItem(item);
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
    cache.putItem(item);

    return item;
  }

  @VisibleForTesting
  public void invalidateCache() {
    cache.invalidateAll();
  }
}
