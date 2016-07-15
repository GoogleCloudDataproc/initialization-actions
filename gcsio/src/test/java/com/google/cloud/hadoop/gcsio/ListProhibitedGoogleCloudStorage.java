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

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * A @{link GoogleCloudStorage} that throws an @{code UnsupportedOperationException} on
 * operations that are inconsistent.
 */
public class ListProhibitedGoogleCloudStorage implements GoogleCloudStorage {
  protected final GoogleCloudStorage delegateGcs;

  public ListProhibitedGoogleCloudStorage(GoogleCloudStorage delegateGcs) {
    this.delegateGcs = delegateGcs;
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return delegateGcs.getOptions();
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId) throws IOException {
    return delegateGcs.create(resourceId);
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId,
      CreateObjectOptions options) throws IOException {
    return delegateGcs.create(resourceId, options);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    delegateGcs.createEmptyObject(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId,
      CreateObjectOptions options) throws IOException {
    delegateGcs.createEmptyObject(resourceId, options);
  }

  @Override
  public void createEmptyObjects(
      List<StorageResourceId> resourceIds) throws IOException {
    delegateGcs.createEmptyObjects(resourceIds);
  }

  @Override
  public void createEmptyObjects(
      List<StorageResourceId> resourceIds,
      CreateObjectOptions options) throws IOException {
    delegateGcs.createEmptyObjects(resourceIds, options);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId) throws IOException {
    return delegateGcs.open(resourceId);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return delegateGcs.open(resourceId, readOptions);
  }

  @Override
  public void create(String bucketName) throws IOException {
    delegateGcs.create(bucketName);
  }

  @Override
  public void create(String bucketName, CreateBucketOptions options) throws IOException {
    delegateGcs.create(bucketName, options);
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
  }

  @Override
  public List<String> listBucketNames() throws IOException {
    throw new UnsupportedOperationException(
        "Operation not supported in ListProhibitedGoogleCloudStorage.");
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    throw new UnsupportedOperationException(
        "Operation not supported in ListProhibitedGoogleCloudStorage.");
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
    throw new UnsupportedOperationException(
        "Operation not supported in ListProhibitedGoogleCloudStorage.");
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
    throw new UnsupportedOperationException(
        "Operation not supported in ListProhibitedGoogleCloudStorage.");
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
    throw new UnsupportedOperationException(
        "Operation not supported in ListProhibitedGoogleCloudStorage.");
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    delegateGcs.compose(bucketName, sources, destination, contentType);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources,
      final StorageResourceId destination,
      CreateObjectOptions options)
      throws IOException {
    return delegateGcs.composeObjects(sources, destination, options);
  }
}
