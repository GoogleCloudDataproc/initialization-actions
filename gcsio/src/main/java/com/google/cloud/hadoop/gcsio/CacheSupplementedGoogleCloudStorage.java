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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * CacheSupplementedGoogleCloudStorage adds additional book-keeping to a GoogleCloudStorage instance
 * using a {@code DirectoryListCache} and wraps the create/copy/delete/list methods to provide
 * immediate same-client consistency for "list" operations following a "create/copy/delete". See
 * {@code DirectoryListCache} for details of consistency semantics.
 */
public class CacheSupplementedGoogleCloudStorage
    implements GoogleCloudStorage {
  // Logger.
  private static final Logger LOG =
      LoggerFactory.getLogger(CacheSupplementedGoogleCloudStorage.class);

  // An actual implementation of GoogleCloudStorage that will be used for the actual logic of
  // GCS operations, while this class adds book-keeping around the delegated calls.
  private final GoogleCloudStorage gcsDelegate;

  // Cache of freshly created Buckets or StorageObjects to be updated on create/copy/delete to
  // supplement "list" calls with GCS resources that may not have appeared in the Cloud list
  // index yet.
  // TODO(user): Add support for perf-boosting use-cases, such as serving getItemInfo directly
  // from cache once we have plumbing in-place to pre-populate metadata on create/copy. Also,
  // consider cases where it's possible to serve list* exclusively from cache as long as cross-
  // client consistency isn't enforced.
  private DirectoryListCache resourceCache;

  /**
   * Constructs a CacheSupplementedGoogleCloudStorage that should be usable anywhere a
   * GoogleCloudStorage interface is used and that supplements missing listObject/listBucket
   * results from an in-memory cache of known GCS resources that may not have propagated into
   * the eventually-consistent remote "list" index yet.
   *
   * @param gcsDelegate The GoogleCloudStorage to be used for normal API interactions, before
   *     supplementing with in-memory info.
   */
  public CacheSupplementedGoogleCloudStorage(
      GoogleCloudStorage gcsDelegate, DirectoryListCache resourceCache) {
    Preconditions.checkArgument(gcsDelegate != null, "gcsDelegate must not be null");
    Preconditions.checkArgument(resourceCache != null, "resourceCache must not be null");

    this.gcsDelegate = gcsDelegate;
    this.resourceCache = resourceCache;
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return gcsDelegate.getOptions();
  }

  /**
   * Wraps the delegate's returned WritableByteChannel in a helper that will update the
   * resourceCache when close() is called.
   */
  @Override
  public WritableByteChannel create(final StorageResourceId resourceId)
      throws IOException {
    LOG.debug("create({})", resourceId);
    return create(resourceId, CreateObjectOptions.DEFAULT);
  }

  @Override
  public WritableByteChannel create(final StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    LOG.debug("create({}, {})", resourceId, options);

    final WritableByteChannel innerChannel = gcsDelegate.create(resourceId, options);

    // Wrap the delegate's channel in our own channel that simply adds the additional book-keeping
    // hook to close().
    return new WritableByteChannel() {
      @Override
      public int write(ByteBuffer buffer)
          throws IOException {
        return innerChannel.write(buffer);
      }

      @Override
      public boolean isOpen() {
        return innerChannel.isOpen();
      }

      @Override
      public void close()
          throws IOException {
        innerChannel.close();
        // TODO(user): Make create() somehow wire the StorageObject through to the caller,
        // possibly through an onClose() handler so that we can pre-emptively populate the
        // metadata in the CacheEntry.
        resourceCache.putResourceId(resourceId);
      }
    };
  }

  /**
   * Records the resourceId after delegating.
   */
  @Override
  public void createEmptyObject(StorageResourceId resourceId)
      throws IOException {
    LOG.debug("createEmptyObject({})", resourceId);
    gcsDelegate.createEmptyObject(resourceId);
    resourceCache.putResourceId(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    LOG.debug("createEmptyObject({}, {})", resourceId, options);
    gcsDelegate.createEmptyObject(resourceId, options);
    resourceCache.putResourceId(resourceId);
  }

  /**
   * Records the resourceIds after delegating.
   */
  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds)
      throws IOException {
    LOG.debug("createEmptyObjects({})", resourceIds);
    gcsDelegate.createEmptyObjects(resourceIds);
    for (StorageResourceId resourceId : resourceIds) {
      resourceCache.putResourceId(resourceId);
    }
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    LOG.debug("createEmptyObjects({}, {})", resourceIds, options);
    gcsDelegate.createEmptyObjects(resourceIds, options);
    for (StorageResourceId resourceId : resourceIds) {
      resourceCache.putResourceId(resourceId);
    }
  }

  /**
   * Pure pass-through.
   */
  @Override
  public SeekableByteChannel open(StorageResourceId resourceId)
      throws IOException {
    LOG.debug("open({})", resourceId);
    return gcsDelegate.open(resourceId);
  }

  /**
   * Updates cache with bucketName.
   */
  @Override
  public void create(String bucketName)
      throws IOException {
    LOG.debug("create({})", bucketName);
    // TODO(user): Make create() return the Bucket so that we can pre-emptively populate the
    // metadata in the CachedBucket.
    gcsDelegate.create(bucketName);
    resourceCache.putResourceId(new StorageResourceId(bucketName));
  }

  /**
   * Updates cache with bucketName.
   */
  @Override
  public void create(String bucketName, CreateBucketOptions options)
      throws IOException {
    LOG.debug("create({})", bucketName);
    // TODO(user): Make create() return the Bucket so that we can pre-emptively populate the
    // metadata in the CachedBucket.
    gcsDelegate.create(bucketName, options);
    resourceCache.putResourceId(new StorageResourceId(bucketName));
  }

  /**
   * Removes buckets from cache, if they exist.
   */
  @Override
  public void deleteBuckets(List<String> bucketNames)
      throws IOException {
    LOG.debug("deleteBuckets({})", bucketNames);
    // TODO(user): Potentially include as blacklist entry in cache along with timestamp to clobber
    // incorrect/stale "list" results from GCS as long as their returned timestamp is older than
    // the blacklist entry.
    gcsDelegate.deleteBuckets(bucketNames);
    for (String bucketName : bucketNames) {
      resourceCache.removeResourceId(new StorageResourceId(bucketName));
    }
  }

  /**
   * Removes objects from cache, if they exist.
   */
  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames)
      throws IOException {
    LOG.debug("deleteObjects({})", fullObjectNames);
    // TODO(user): Potentially include as blacklist entry in cache along with timestamp to clobber
    // incorrect/stale "list" results from GCS as long as their returned timestamp is older than
    // the blacklist entry.
    gcsDelegate.deleteObjects(fullObjectNames);
    for (StorageResourceId resourceId : fullObjectNames) {
      resourceCache.removeResourceId(resourceId);
    }
  }

  /**
   * Adds the copied destination items to the list cache, without their associated metadata;
   * supplementing with the cache will have to populate the metadata on-demand.
   */
  @Override
  public void copy(String srcBucketName, List<String> srcObjectNames,
      String dstBucketName, List<String> dstObjectNames)
      throws IOException {
    // TODO(user): Maybe catch exceptions and check their inner exceptions for
    // FileNotFoundExceptions and update the DirectoryListCache accordingly. For partial failures,
    // we probably still want to add the successful ones to the list cache.
    // TODO(user): Make GCS.copy return the list of destination StorageObjects that were
    // successfully created, so that we can pre-emptively populate the metadata into the cache.
    gcsDelegate.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
    for (String dstObjectName : dstObjectNames) {
      resourceCache.putResourceId(new StorageResourceId(dstBucketName, dstObjectName));
    }
  }

  /**
   * Adds destination to the cache.
   */
  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    gcsDelegate.compose(bucketName, sources, destination, contentType);
    resourceCache.putResourceId(new StorageResourceId(bucketName, destination));
  }

  /**
   * Helper for checking the list of {@code candidateEntries} against a {@code originalIds} to
   * possibly retrieve supplemental results from the DirectoryListCache.
   * This method will modify {@code originalIds} as it goes to include the StorageResourceIds
   * of CacheEntrys being returned.
   *
   * @return A list of CacheEntry that is a subset of {@code candidateEntries}, whose elements
   *     are not in the set of resourceIds corresponding to {@code originalIds}.
   */
  private List<CacheEntry> getSupplementalEntries(
      Set<StorageResourceId> originalIds, List<CacheEntry> candidateEntries) {
    List<CacheEntry> supplementalEntries = new ArrayList<>();
    for (CacheEntry entry : candidateEntries) {
      StorageResourceId entryId = entry.getResourceId();
      if (!originalIds.contains(entryId)) {
        supplementalEntries.add(entry);
        originalIds.add(entryId);
      }
    }
    return supplementalEntries;
  }

  /**
   * Helper for either pulling the existing GoogleCloudStorageItemInfo from each element of
   * {@code cacheEntries} or fetching the associated GoogleCloudStorageItemInfo on-demand, updating
   * the cache entry, then appending the new result to the return list. Items that fail to be
   * fetched will not be returned.
   */
  private List<GoogleCloudStorageItemInfo> extractOrRevalidateItemInfos(
      List<CacheEntry> cacheEntries)
      throws IOException {
    // TODO(user): Batch these.
    List<GoogleCloudStorageItemInfo> supplementalInfos = new ArrayList<>();
    for (CacheEntry entry : cacheEntries) {
      GoogleCloudStorageItemInfo itemInfo = entry.getItemInfo();
      if (itemInfo != null) {
        // The detailed info is already available; supplement it directly.
        LOG.info("Supplementing missing itemInfo with already-cached info: {}", itemInfo);
        supplementalInfos.add(itemInfo);
      } else {
        // We need to fetch the associated info from the gcsDelegate; in addition to
        // supplementing, we must update the cache with the fetched info.
        LOG.info("Populating missing itemInfo on-demand for entry: {}", entry.getResourceId());
        itemInfo = gcsDelegate.getItemInfo(entry.getResourceId());
        if (!itemInfo.exists()) {
          LOG.warn(
              "Possible stale CacheEntry; failed to fetch item info for: {} - removing from cache",
              entry.getResourceId());
          resourceCache.removeResourceId(entry.getResourceId());
        } else {
          entry.setItemInfo(itemInfo);
          supplementalInfos.add(itemInfo);
        }
      }
    }
    return supplementalInfos;
  }

  /**
   * Supplements the list returned by the delegate with cached bucket names; won't trigger
   * any fetching of metadata.
   */
  @Override
  public List<String> listBucketNames()
      throws IOException {
    LOG.debug("listBucketNames()");
    List<String> allBucketNames = gcsDelegate.listBucketNames();
    List<CacheEntry> cachedBuckets = resourceCache.getBucketList();
    if (cachedBuckets.isEmpty()) {
      return allBucketNames;
    } else {
      // Make a copy in case the delegate returned an immutable list.
      allBucketNames = new ArrayList<>(allBucketNames);
    }

    Set<StorageResourceId> bucketIds = new HashSet<>();
    for (String bucketName : allBucketNames) {
      bucketIds.add(new StorageResourceId(bucketName));
    }

    List<CacheEntry> missingCachedBuckets = getSupplementalEntries(bucketIds, cachedBuckets);
    for (CacheEntry supplement : missingCachedBuckets) {
      LOG.info("Supplementing missing matched StorageResourceId: {}", supplement.getResourceId());
      allBucketNames.add(supplement.getResourceId().getBucketName());
    }
    return allBucketNames;
  }

  /**
   * Supplements the list returned by the delegate with cached bucket infos; may trigger fetching
   * of any metadata not already available in the cache. If a delegate-returned item is also in the
   * cache and the cache doesn't already have the metadata, it will be opportunistically updated
   * with the retrieved metadata.
   */
  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo()
      throws IOException {
    LOG.debug("listBucketInfo()");
    List<GoogleCloudStorageItemInfo> allBucketInfos = gcsDelegate.listBucketInfo();
    List<CacheEntry> cachedBuckets = resourceCache.getBucketList();
    if (cachedBuckets.isEmpty()) {
      return allBucketInfos;
    } else {
      // Make a copy in case the delegate returned an immutable list.
      allBucketInfos = new ArrayList<>(allBucketInfos);
    }


    Set<StorageResourceId> bucketIdsSet = new HashSet<>();
    for (GoogleCloudStorageItemInfo itemInfo : allBucketInfos) {
      bucketIdsSet.add(itemInfo.getResourceId());
    }
    List<CacheEntry> missingCachedBuckets = getSupplementalEntries(bucketIdsSet, cachedBuckets);
    List<GoogleCloudStorageItemInfo> supplementalInfos =
        extractOrRevalidateItemInfos(missingCachedBuckets);

    allBucketInfos.addAll(supplementalInfos);
    return allBucketInfos;
  }

  /**
   * Supplements the list returned by the delegate with cached object names; will try to fetch
   * metadata for supplemental entries which were missing from the delegate's returned list.
   */
  @Override
  public List<String> listObjectNames(
      String bucketName, String objectNamePrefix, String delimiter)
      throws IOException {
    return listObjectNames(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  /**
   * Supplements the list returned by the delegate with cached object names; will try to fetch
   * metadata for supplemental entries which were missing from the delegate's returned list.
   */
  @Override
  public List<String> listObjectNames(
      String bucketName, String objectNamePrefix, String delimiter,
      long maxResults)
      throws IOException {
    LOG.debug("listObjectNames({}, {}, {}, {})", bucketName, objectNamePrefix,
        delimiter, maxResults);
    List<String> allObjectNames = gcsDelegate.listObjectNames(
        bucketName, objectNamePrefix, delimiter, maxResults);

    if (maxResults > 0 && allObjectNames.size() >= maxResults) {
      // Should not have allObjectNames.size() > maxResults, since we
      // passed maxResults to delegate.
      return allObjectNames;
    }

    // We pass 'null' for 'prefixes' because for now, we won't try to supplement match "prefixes";
    // in normal operation, the cache will also contain the "parent directory" objects for each
    // file, so they would be supplemented as exact matches anyway (if we have gs://bucket/foo/ and
    // gs://bucket/foo/bar, we won't need gs://bucket/foo/bar to generate the "prefix match"
    // gs://bucket/foo/, since the exact directory object already exists).
    // The only exception is if a *different* client created the directory object, so that
    // the local client created the file without creating the directory objects, and then
    // the list API fails to list either object. This is a case of cross-client inconsistency
    // not solved by this cache.
    List<CacheEntry> cachedObjects = resourceCache.getObjectList(
        bucketName, objectNamePrefix, delimiter, null);
    if (cachedObjects == null || cachedObjects.isEmpty()) {
      return allObjectNames;
    } else {
      // Make a copy in case the delegate returned an immutable list.
      allObjectNames = new ArrayList<>(allObjectNames);
    }

    Set<StorageResourceId> objectIds = new HashSet<>();
    for (String objectName : allObjectNames) {
      objectIds.add(new StorageResourceId(bucketName, objectName));
    }

    List<CacheEntry> missingCachedObjects = getSupplementalEntries(objectIds, cachedObjects);
    List<GoogleCloudStorageItemInfo> supplementalInfos =
        extractOrRevalidateItemInfos(missingCachedObjects);
    for (GoogleCloudStorageItemInfo item : supplementalInfos) {
      LOG.info("Supplementing missing matched StorageResourceId: {}", item.getResourceId());
      allObjectNames.add(item.getResourceId().getObjectName());
      if (maxResults > 0 && allObjectNames.size() >= maxResults) {
        break;
      }
    }
    return allObjectNames;
  }

  /**
   * Supplements the list returned by the delegate with cached object infos; may trigger fetching
   * of any metadata not already available in the cache. If a delegate-returned item is also in the
   * cache and the cache doesn't already have the metadata, it will be opportunistically updated
   * with the retrieved metadata.
   */
  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter)
      throws IOException {
    return listObjectInfo(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  /**
   * Supplements the list returned by the delegate with cached object infos; may trigger fetching
   * of any metadata not already available in the cache. If a delegate-returned item is also in the
   * cache and the cache doesn't already have the metadata, it will be opportunistically updated
   * with the retrieved metadata.
   */
  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter,
      long maxResults)
      throws IOException {
    LOG.debug("listObjectInfo({}, {}, {}, {})", bucketName, objectNamePrefix,
        delimiter, maxResults);
    List<GoogleCloudStorageItemInfo> allObjectInfos =
        gcsDelegate.listObjectInfo(bucketName, objectNamePrefix, delimiter,
            maxResults);
    if (maxResults > 0 && allObjectInfos.size() >= maxResults) {
        return allObjectInfos;
    }
    List<CacheEntry> cachedObjects = resourceCache.getObjectList(
        bucketName, objectNamePrefix, delimiter, null);
    if (cachedObjects == null || cachedObjects.isEmpty()) {
      return allObjectInfos;
    } else {
      // Make a copy in case the delegate returned an immutable list.
      allObjectInfos = new ArrayList<>(allObjectInfos);
    }

    // TODO(user): Refactor out more of the shared logic between the 4 list* methods.
    Set<StorageResourceId> objectIdsSet = new HashSet<>();
    for (GoogleCloudStorageItemInfo itemInfo : allObjectInfos) {
      objectIdsSet.add(itemInfo.getResourceId());
    }

    List<CacheEntry> missingCachedObjects = getSupplementalEntries(objectIdsSet, cachedObjects);
    List<GoogleCloudStorageItemInfo> supplementalInfos =
        extractOrRevalidateItemInfos(missingCachedObjects);

    if (maxResults <= 0
        || allObjectInfos.size() + supplementalInfos.size() <= maxResults) {
      allObjectInfos.addAll(supplementalInfos);
    } else {
      for (GoogleCloudStorageItemInfo item : supplementalInfos) {
        allObjectInfos.add(item);
        if (allObjectInfos.size() >= maxResults) {
          break;
        }
      }
    }

    return allObjectInfos;
  }

  /**
   * Pure pass-through.
   */
  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    LOG.debug("getItemInfos({})", resourceIds.toString());
    return gcsDelegate.getItemInfos(resourceIds);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    LOG.debug("updateItems({})", itemInfoList);
    return gcsDelegate.updateItems(itemInfoList);
  }

  /**
   * Pure pass-through.
   */
  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId)
      throws IOException {
    LOG.debug("getItemInfo({})", resourceId);
    // TODO(user): Maybe opportunistically update the cache with any retrieved info; it would take
    // more memory but potentially improve cache coherence. Here and in getItemInfos.
    return gcsDelegate.getItemInfo(resourceId);
  }

  /**
   * Pure pass-through.
   */
  @Override
  public void close() {
    gcsDelegate.close();
  }

  /**
   * Pure pass-through.
   */
  @Override
  public void waitForBucketEmpty(String bucketName)
      throws IOException {
    gcsDelegate.waitForBucketEmpty(bucketName);
  }

  /**
   * Retrieve our internal DirectoryListCache, for testing purposes only.
   */
  @VisibleForTesting
  DirectoryListCache getResourceCache() {
    return resourceCache;
  }
}
