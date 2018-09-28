/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import com.google.api.client.util.Clock;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of GoogleCloudStorage that allows injection of lag to list operations
 */
public class LaggedGoogleCloudStorage implements GoogleCloudStorage  {

  public static interface ListVisibilityCalculator {
    public static final ListVisibilityCalculator IMMEDIATELY_VISIBLE =
        new ListVisibilityCalculator() {
          @Override
          public boolean isObjectVisible(Clock clock, GoogleCloudStorageItemInfo itemInfo) {
            return true;
          }
        };

    public static final ListVisibilityCalculator DEFAULT_LAGGED =
        new ListVisibilityCalculator() {
          /**
           * Maximum lag that the this calculator should simulate
           */
          public static final int DEFAULT_MAX_LIST_LAG_MS = 500; // 0.5 second lag

          @Override
          public boolean isObjectVisible(Clock clock, GoogleCloudStorageItemInfo itemInfo) {
            long currentTime = clock.currentTimeMillis();
            // In order to avoid non-determinism in tests, we'll use the object name's hashcode mod
            // MAX_LIST_LAG_MS to generate a per-object delta between 0 and MAX_LIST_LAG_MS.
            long objectLag = Math.abs(
                itemInfo.getObjectName().hashCode() % DEFAULT_MAX_LIST_LAG_MS);
            return currentTime > itemInfo.getCreationTime() + objectLag;
          }
        };

    boolean isObjectVisible(Clock clock, GoogleCloudStorageItemInfo objectEntry);
  }

  private static final Function<GoogleCloudStorageItemInfo, String> ITEM_INFO_TO_NAME =
      new Function<GoogleCloudStorageItemInfo, String>() {
        @Override
        public String apply(GoogleCloudStorageItemInfo itemInfo) {
          return itemInfo.getObjectName();
        }
      };

  private final GoogleCloudStorage delegate;
  private final Clock clock;
  private final ListVisibilityCalculator listVisibilityCalculator;

  public LaggedGoogleCloudStorage(
      final GoogleCloudStorage delegate,
      final Clock clock,
      final ListVisibilityCalculator listVisibilityCalculator) {
    this.delegate = delegate;
    this.clock = clock;
    this.listVisibilityCalculator = listVisibilityCalculator;
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return delegate.getOptions();
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId) throws IOException {
    return delegate.create(resourceId);
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    return delegate.create(resourceId, options);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId) throws IOException {
    return delegate.open(resourceId);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return delegate.open(resourceId, readOptions);
  }

  @Override
  public void deleteObjects(
      List<StorageResourceId> fullObjectNames) throws IOException {
    delegate.deleteObjects(fullObjectNames);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    return delegate.listBucketInfo();
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    delegate.createEmptyObject(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    delegate.createEmptyObject(resourceId, options);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds)
      throws IOException {
    delegate.createEmptyObjects(resourceIds);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    delegate.createEmptyObjects(resourceIds, options);
  }

  @Override
  public void create(String bucketName) throws IOException {
    delegate.create(bucketName);
  }

  @Override
  public void create(String bucketName, CreateBucketOptions options) throws IOException {
    delegate.create(bucketName, options);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(
      List<StorageResourceId> resourceIds) throws IOException {
    return delegate.getItemInfos(resourceIds);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    return delegate.updateItems(itemInfoList);
  }

  @Override
  public void waitForBucketEmpty(String bucketName) throws IOException {
    delegate.waitForBucketEmpty(bucketName);
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    delegate.compose(bucketName, sources, destination, contentType);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources,
      final StorageResourceId destination,
      CreateObjectOptions options)
      throws IOException {
    return delegate.composeObjects(sources, destination, options);
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    delegate.deleteBuckets(bucketNames);
  }

  @Override
  public void copy(String srcBucketName, List<String> srcObjectNames,
      String dstBucketName, List<String> dstObjectNames) throws IOException {
    delegate.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  @Override
  public List<String> listBucketNames() throws IOException {
    return delegate.listBucketNames();
  }

  @Override
  public GoogleCloudStorageItemInfo getItemInfo(
      StorageResourceId resourceId) throws IOException {
    return delegate.getItemInfo(resourceId);
  }

  @Override
  public List<String> listObjectNames(String bucketName,
      String objectNamePrefix, String delimiter) throws IOException {
    return listObjectNames(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  @Override
  public List<String> listObjectNames(String bucketName,
      String objectNamePrefix, String delimiter,
      long maxResults) throws IOException {
    return Lists.transform(
        listObjectInfo(bucketName, objectNamePrefix, delimiter, maxResults),
        ITEM_INFO_TO_NAME);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter) throws IOException {
    return listObjectInfo(
        bucketName, objectNamePrefix, delimiter, GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(String bucketName,
      String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    // We don't know how many items will be trimmed by listVisibilityCalculator,
    // so we can't limit the number of items returned by our delegate.
    List<GoogleCloudStorageItemInfo> delegatedObjects =
        delegate.listObjectInfo(
            bucketName, objectNamePrefix, delimiter, GoogleCloudStorage.MAX_RESULTS_UNLIMITED);

    return getVisibleItems(delegatedObjects, maxResults);
  }

  @Override
  public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, String delimiter, String pageToken)
      throws IOException {
    ListPage<GoogleCloudStorageItemInfo> page =
        delegate.listObjectInfoPage(bucketName, objectNamePrefix, delimiter, pageToken);
    List<GoogleCloudStorageItemInfo> visibleItems =
        getVisibleItems(page.getItems(), GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
    return new ListPage<>(visibleItems, page.getNextPageToken());
  }

  private List<GoogleCloudStorageItemInfo> getVisibleItems(
      List<GoogleCloudStorageItemInfo> infos, long maxResults) {
    List<GoogleCloudStorageItemInfo> result = new ArrayList<>();
    for (GoogleCloudStorageItemInfo info : infos) {
      if (listVisibilityCalculator.isObjectVisible(clock, info)) {
        result.add(info);
        if (maxResults > 0 && result.size() >= maxResults) {
          break;
        }
      }
    }
    return result;
  }

  @Override
  public void close() {
    delegate.close();
  }
}
