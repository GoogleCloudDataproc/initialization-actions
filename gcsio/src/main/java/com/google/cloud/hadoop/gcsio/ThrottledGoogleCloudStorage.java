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

import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.EnumSet;
import java.util.List;

/**
 * Throttled GCS implementation that will limit our bucket creation and delete operations
 * to N per second.
 */
public class ThrottledGoogleCloudStorage implements GoogleCloudStorage {

  /**
   * Operations that may be throttled.
   */
  public enum StorageOperation {
    CREATE_BUCKET,
    DELETE_BUCKETS,
    CREATE_OBJECT,
    COMPOSE_OBJECTS,
    DELETE_OBJECTS,
    OPEN_OBJECT,
    COPY_OBJECT,
    LIST_BUCKETS,
    LIST_OBJECTS,
    GET_ITEMINFO,
    UPDATE_ITEMINFO
  }

  private final RateLimiter rateLimiter;
  private final GoogleCloudStorage wrappedGcs;
  private final EnumSet<StorageOperation> throttledOperations;

  /**
   * Construct a ThrottledGoogleCloudStorage object that throttles all operations.
   * @param wrappedGcs The GCS to wrap with
   * @param rateLimiter The RateLimiter to control operations permitted per timespan.
   */
  public ThrottledGoogleCloudStorage(GoogleCloudStorage wrappedGcs,
      RateLimiter rateLimiter) {
    this(rateLimiter, wrappedGcs, EnumSet.allOf(StorageOperation.class));
  }
  /**
   * @param operationsPerSecond Operations per second to allow.
   * @param wrappedGcs The GoogleCloudStorage that we should delegate operations to.
   * @param throttledOperations The operations that should be throttled.
   */
  public ThrottledGoogleCloudStorage(
      double operationsPerSecond, GoogleCloudStorage wrappedGcs,
      EnumSet<StorageOperation> throttledOperations) {
    this(RateLimiter.create(operationsPerSecond), wrappedGcs, throttledOperations);
  }

  /**
   * @param rateLimiter The RateLimiter to control operations permitted per timespan.
   * @param wrappedGcs The GoogleCloudStorage that we should delegate operations to.
   * @param throttledOperations The operations that should be throttled.
   */
  public ThrottledGoogleCloudStorage(RateLimiter rateLimiter,
      GoogleCloudStorage wrappedGcs,
      EnumSet<StorageOperation> throttledOperations) {
    this.rateLimiter = rateLimiter;
    this.wrappedGcs = wrappedGcs;
    this.throttledOperations = throttledOperations;
  }

  private void throttle(StorageOperation operation) {
    throttle(operation, 1);
  }

  private void throttle(StorageOperation operation, int permits) {
    if (throttledOperations.contains(operation) && permits > 0) {
      rateLimiter.acquire(permits);
    }
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return wrappedGcs.getOptions();
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId) throws IOException {
    throttle(StorageOperation.CREATE_OBJECT);
    return wrappedGcs.create(resourceId);
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    throttle(StorageOperation.CREATE_OBJECT);
    return wrappedGcs.create(resourceId, options);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    throttle(StorageOperation.CREATE_OBJECT);
    wrappedGcs.createEmptyObject(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    throttle(StorageOperation.CREATE_OBJECT);
    wrappedGcs.createEmptyObject(resourceId, options);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds)
      throws IOException {
    throttle(StorageOperation.CREATE_OBJECT, resourceIds.size());
    wrappedGcs.createEmptyObjects(resourceIds);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    throttle(StorageOperation.CREATE_OBJECT, resourceIds.size());
    wrappedGcs.createEmptyObjects(resourceIds, options);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId) throws IOException {
    throttle(StorageOperation.OPEN_OBJECT);
    return wrappedGcs.open(resourceId);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    throttle(StorageOperation.OPEN_OBJECT);
    return wrappedGcs.open(resourceId, readOptions);
  }

  @Override
  public void create(String bucketName) throws IOException {
    throttle(StorageOperation.CREATE_BUCKET);
    wrappedGcs.create(bucketName);
  }

  @Override
  public void create(String bucketName, CreateBucketOptions options) throws IOException {
    throttle(StorageOperation.CREATE_BUCKET);
    wrappedGcs.create(bucketName, options);
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    // We're quota'd on delete, and base impl does a batch operation. We should
    // really wait once per bucket or something similar. BUT since this is used
    // for testing, I don't want to do anything but delegate actual logic.
    throttle(StorageOperation.DELETE_BUCKETS, bucketNames.size());
    wrappedGcs.deleteBuckets(bucketNames);
  }

  @Override
  public void deleteObjects(
      List<StorageResourceId> fullObjectNames) throws IOException {
    throttle(StorageOperation.DELETE_OBJECTS, fullObjectNames.size());
    wrappedGcs.deleteObjects(fullObjectNames);
  }

  @Override
  public void copy(String srcBucketName, List<String> srcObjectNames,
      String dstBucketName, List<String> dstObjectNames) throws IOException {
    throttle(StorageOperation.COPY_OBJECT, srcObjectNames.size());
    wrappedGcs.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  @Override
  public List<String> listBucketNames() throws IOException {
    throttle(StorageOperation.LIST_BUCKETS);
    return wrappedGcs.listBucketNames();
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    throttle(StorageOperation.LIST_BUCKETS);
    return wrappedGcs.listBucketInfo();
  }

  @Override
  public List<String> listObjectNames(String bucketName,
      String objectNamePrefix, String delimiter)
      throws IOException {
    return listObjectNames(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  @Override
  public List<String> listObjectNames(String bucketName,
      String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    throttle(StorageOperation.LIST_OBJECTS);
    return wrappedGcs.listObjectNames(
        bucketName, objectNamePrefix, delimiter, maxResults);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(String bucketName,
      String objectNamePrefix, String delimiter)
      throws IOException {
    return listObjectInfo(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(String bucketName,
      String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    throttle(StorageOperation.LIST_OBJECTS);
    return wrappedGcs.listObjectInfo(
        bucketName, objectNamePrefix, delimiter, maxResults);
  }

  @Override
  public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, String delimiter, String pageToken)
      throws IOException {
    throttle(StorageOperation.LIST_OBJECTS);
    return wrappedGcs.listObjectInfoPage(bucketName, objectNamePrefix, delimiter, pageToken);
  }

  @Override
  public GoogleCloudStorageItemInfo getItemInfo(
      StorageResourceId resourceId) throws IOException {
    throttle(StorageOperation.GET_ITEMINFO);
    return wrappedGcs.getItemInfo(resourceId);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(
      List<StorageResourceId> resourceIds) throws IOException {
    throttle(StorageOperation.GET_ITEMINFO, resourceIds.size());
    return wrappedGcs.getItemInfos(resourceIds);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    throttle(StorageOperation.UPDATE_ITEMINFO, itemInfoList.size());
    return wrappedGcs.updateItems(itemInfoList);
  }

  @Override
  public void close() {
    wrappedGcs.close();
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    throttle(StorageOperation.GET_ITEMINFO, sources.size());
    throttle(StorageOperation.COMPOSE_OBJECTS);
    wrappedGcs.compose(bucketName, sources, destination, contentType);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources,
      final StorageResourceId destination,
      CreateObjectOptions options)
      throws IOException {
    throttle(StorageOperation.GET_ITEMINFO, sources.size());
    throttle(StorageOperation.COMPOSE_OBJECTS);
    return wrappedGcs.composeObjects(sources, destination, options);
  }
}
