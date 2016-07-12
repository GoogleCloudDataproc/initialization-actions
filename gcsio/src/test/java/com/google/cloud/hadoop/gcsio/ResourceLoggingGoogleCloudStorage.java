/**
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

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of @{link GoogleCloudStorage} that records when objects and buckets are
 * created. This can then be used to remove those resources in test teardown.
 */
public class ResourceLoggingGoogleCloudStorage implements GoogleCloudStorage {
  protected final GoogleCloudStorage delegateGcs;
  private List<StorageResourceId> createdResources = new ArrayList<>();

  public ResourceLoggingGoogleCloudStorage(GoogleCloudStorage delegateGcs) {
    this.delegateGcs = delegateGcs;
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return delegateGcs.getOptions();
  }

  public List<StorageResourceId> getCreatedResources() {
     return ImmutableList.copyOf(createdResources);
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId) throws IOException {
    WritableByteChannel channel = delegateGcs.create(resourceId);
    createdResources.add(resourceId);
    return channel;
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId,
      CreateObjectOptions options) throws IOException {
    WritableByteChannel channel = delegateGcs.create(resourceId, options);
    createdResources.add(resourceId);
    return channel;
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    delegateGcs.createEmptyObject(resourceId);
    createdResources.add(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId,
      CreateObjectOptions options) throws IOException {
    delegateGcs.createEmptyObject(resourceId, options);
    createdResources.add(resourceId);
  }

  @Override
  public void createEmptyObjects(
      List<StorageResourceId> resourceIds) throws IOException {
    delegateGcs.createEmptyObjects(resourceIds);
    createdResources.addAll(resourceIds);
  }

  @Override
  public void createEmptyObjects(
      List<StorageResourceId> resourceIds,
      CreateObjectOptions options) throws IOException {
    delegateGcs.createEmptyObjects(resourceIds, options);
    createdResources.addAll(resourceIds);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId) throws IOException {
    return delegateGcs.open(resourceId);
  }

  @Override
  public void create(String bucketName) throws IOException {
    delegateGcs.create(bucketName);
    createdResources.add(new StorageResourceId(bucketName));
  }

  @Override
  public void create(String bucketName, CreateBucketOptions options) throws IOException {
    delegateGcs.create(bucketName, options);
    createdResources.add(new StorageResourceId(bucketName));
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    delegateGcs.deleteBuckets(bucketNames);
  }

  @Override
  public void deleteObjects(
      List<StorageResourceId> fullObjectNames) throws IOException {
    delegateGcs.deleteObjects(fullObjectNames);
  }

  @Override
  public void copy(String srcBucketName, List<String> srcObjectNames,
      String dstBucketName, List<String> dstObjectNames) throws IOException {
    delegateGcs.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
    for (String dstObjectName : dstObjectNames) {
      createdResources.add(new StorageResourceId(dstBucketName, dstObjectName));
    }
  }

  @Override
  public List<String> listBucketNames() throws IOException {
    return delegateGcs.listBucketNames();
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    return delegateGcs.listBucketInfo();
  }

  @Override
  public List<String> listObjectNames(String bucketName,
      String objectNamePrefix, String delimiter) throws IOException {
    return listObjectNames(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  @Override
  public List<String> listObjectNames(String bucketName,
      String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    return delegateGcs.listObjectNames(bucketName, objectNamePrefix,
        delimiter, maxResults);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(String bucketName,
      String objectNamePrefix, String delimiter) throws IOException {
    return listObjectInfo(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(String bucketName,
      String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    return delegateGcs.listObjectInfo(bucketName, objectNamePrefix,
        delimiter, maxResults);
  }

  @Override
  public GoogleCloudStorageItemInfo getItemInfo(
      StorageResourceId resourceId) throws IOException {
    return delegateGcs.getItemInfo(resourceId);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(
      List<StorageResourceId> resourceIds) throws IOException {
    return delegateGcs.getItemInfos(resourceIds);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    return delegateGcs.updateItems(itemInfoList);
  }

  @Override
  public void close() {
    delegateGcs.close();
  }

  @Override
  public void waitForBucketEmpty(String bucketName) throws IOException {
    delegateGcs.waitForBucketEmpty(bucketName);
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    delegateGcs.compose(bucketName, sources, destination, contentType);
    createdResources.add(new StorageResourceId(bucketName, destination));
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources,
      final StorageResourceId destination,
      CreateObjectOptions options)
      throws IOException {
    createdResources.add(destination);
    return delegateGcs.composeObjects(sources, destination, options);
  }
}
