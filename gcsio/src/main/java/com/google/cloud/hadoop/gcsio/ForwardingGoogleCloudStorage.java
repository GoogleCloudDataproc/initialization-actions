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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A class that wraps a {@link GoogleCloudStorage} object, delegating all calls to it. */
public class ForwardingGoogleCloudStorage implements GoogleCloudStorage {

  /** Logger. */
  private static final Logger LOG = LoggerFactory.getLogger(ForwardingGoogleCloudStorage.class);

  /** The stored delegate. */
  private final GoogleCloudStorage delegate;

  /** The simple name of the delegate's class. Used for debug logging. */
  private final String delegateClassName;

  /**
   * Creates a new GoogleCloudStorageWrapper.
   *
   * @param delegate the {@link GoogleCloudStorage} to delegate calls to.
   */
  public ForwardingGoogleCloudStorage(GoogleCloudStorage delegate) {
    Preconditions.checkArgument(delegate != null, "delegate must not be null.");

    this.delegate = delegate;
    delegateClassName = delegate.getClass().getSimpleName();
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    LOG.debug("{}.getOptions()", delegateClassName);
    return delegate.getOptions();
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId) throws IOException {
    LOG.debug("{}.create({})", delegateClassName, resourceId);
    return delegate.create(resourceId);
  }

  @Override
  public WritableByteChannel create(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    LOG.debug("{}.create({}, {})", delegateClassName, resourceId, options);
    return delegate.create(resourceId, options);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    LOG.debug("{}.createEmptyObject({})", delegateClassName, resourceId);
    delegate.createEmptyObject(resourceId);
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    LOG.debug("{}.createEmptyObject({}, {})", delegateClassName, resourceId, options);
    delegate.createEmptyObject(resourceId, options);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds) throws IOException {
    LOG.debug("{}.createEmptyObjects({})", delegateClassName, resourceIds);
    delegate.createEmptyObjects(resourceIds);
  }

  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds, CreateObjectOptions options)
      throws IOException {
    LOG.debug("{}.createEmptyObjects({}, {})", delegateClassName, resourceIds, options);
    delegate.createEmptyObjects(resourceIds, options);
  }

  @Override
  public SeekableByteChannel open(StorageResourceId resourceId) throws IOException {
    LOG.debug("{}.open({})", delegateClassName, resourceId);
    return delegate.open(resourceId);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    LOG.debug("{}.open({}, {})", delegateClassName, resourceId, readOptions);
    return delegate.open(resourceId, readOptions);
  }

  @Override
  public void create(String bucketName) throws IOException {
    LOG.debug("{}.create({})", delegateClassName, bucketName);
    delegate.create(bucketName);
  }

  @Override
  public void create(String bucketName, CreateBucketOptions options) throws IOException {
    LOG.debug("{}.create({}, {})", delegateClassName, bucketName, options);
    delegate.create(bucketName, options);
  }

  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    LOG.debug("{}.deleteBuckets({})", delegateClassName, bucketNames);
    delegate.deleteBuckets(bucketNames);
  }

  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    LOG.debug("{}.deleteObjects({})", delegateClassName, fullObjectNames);
    delegate.deleteObjects(fullObjectNames);
  }

  @Override
  public void copy(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames)
      throws IOException {
    LOG.debug(
        "{}.copy({}, {}, {}, {})",
        delegateClassName,
        srcBucketName,
        srcObjectNames,
        dstBucketName,
        dstObjectNames);
    delegate.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  @Override
  public List<String> listBucketNames() throws IOException {
    LOG.debug("{}.listBucketNames()", delegateClassName);
    return delegate.listBucketNames();
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo() throws IOException {
    LOG.debug("{}.listBucketInfo()", delegateClassName);
    return delegate.listBucketInfo();
  }

  @Override
  public List<String> listObjectNames(String bucketName, String objectNamePrefix, String delimiter)
      throws IOException {
    LOG.debug(
        "{}.listObjectNames({}, {}, {})",
        delegateClassName,
        bucketName,
        objectNamePrefix,
        delimiter);
    return delegate.listObjectNames(bucketName, objectNamePrefix, delimiter);
  }

  @Override
  public List<String> listObjectNames(
      String bucketName, String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    LOG.debug(
        "{}.listObjectNames({}, {}, {}, {})",
        delegateClassName,
        bucketName,
        objectNamePrefix,
        delimiter,
        maxResults);
    return delegate.listObjectNames(bucketName, objectNamePrefix, delimiter, maxResults);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter) throws IOException {
    LOG.debug(
        "{}.listObjectInfo({}, {}, {})",
        delegateClassName,
        bucketName,
        objectNamePrefix,
        delimiter);
    return delegate.listObjectInfo(bucketName, objectNamePrefix, delimiter);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    LOG.debug(
        "{}.listObjectInfo({}, {}, {}, {})",
        delegateClassName,
        bucketName,
        objectNamePrefix,
        delimiter,
        maxResults);
    return delegate.listObjectInfo(bucketName, objectNamePrefix, delimiter, maxResults);
  }

  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId) throws IOException {
    LOG.debug("{}.getItemInfo({})", delegateClassName, resourceId);
    return delegate.getItemInfo(resourceId);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    LOG.debug("{}.getItemInfos({})", delegateClassName, resourceIds);
    return delegate.getItemInfos(resourceIds);
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    LOG.debug("{}.updateItems({})", delegateClassName, itemInfoList);
    return delegate.updateItems(itemInfoList);
  }

  @Override
  public void close() {
    LOG.debug("{}.close()", delegateClassName);
    delegate.close();
  }

  @Override
  public void waitForBucketEmpty(String bucketName) throws IOException {
    LOG.debug("{}.waitForBucketEmpty({})", delegateClassName, bucketName);
    delegate.waitForBucketEmpty(bucketName);
  }

  @Override
  public void compose(
      String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    LOG.debug(
        "{}.compose({}, {}, {}, {})",
        delegateClassName,
        bucketName,
        sources,
        destination,
        contentType);
    delegate.compose(bucketName, sources, destination, contentType);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources, StorageResourceId destination, CreateObjectOptions options)
      throws IOException {
    LOG.debug("{}.composeObjects({}, {}, {})", delegateClassName, sources, destination, options);
    return delegate.composeObjects(sources, destination, options);
  }

  /**
   * Gets the {@link GoogleCloudStorage} objected wrapped by this class.
   *
   * @return the {@link GoogleCloudStorage} objected wrapped by this class.
   */
  protected GoogleCloudStorage getDelegate() {
    return delegate;
  }
}
