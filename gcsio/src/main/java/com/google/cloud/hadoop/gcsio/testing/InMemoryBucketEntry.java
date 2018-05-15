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

package com.google.cloud.hadoop.gcsio.testing;

import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.base.MoreObjects;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * InMemoryBucketEntry represents a GCS bucket in-memory by containing mappings to objects in
 * the bucket alongside bucket-level metadata including storage location, storage class, etc.
 * It is intended to be used only internally by the InMemoryGoogleCloudStorage class.
 */
public class InMemoryBucketEntry {
  // Mapping from objectName to InMemoryObjectEntries which hold byte contents, metadata, and write
  // channels.
  private Map<String, InMemoryObjectEntry> objectLookup = new HashMap<>();

  // The metadata associated with this InMemoryBucketEntry compatible with GoogleCloudStorage; its
  // objectName is always null and size is zero.
  private GoogleCloudStorageItemInfo info;

  /**
   * @param bucketName The name representing the bucketName portion of a GCS path, e.g.
   *     gs://<bucketName>/<objectName>.
   */
  public InMemoryBucketEntry(
      String bucketName, long createTimeMillis, CreateBucketOptions options) {
    info = new GoogleCloudStorageItemInfo(
        new StorageResourceId(bucketName), createTimeMillis, 0,
        MoreObjects.firstNonNull(options.getLocation(), "us-central"),
        MoreObjects.firstNonNull(options.getStorageClass(), "inmemory-class"));
  }

  /**
   * Inserts an InMemoryObjectEntry representing {@code objectName}; replaces any existing object.
   */
  public synchronized void add(InMemoryObjectEntry obj) {
    assert obj.getBucketName().equals(info.getBucketName());
    objectLookup.put(obj.getObjectName(), obj);
  }

  /**
   * Retrieves a previously inserted {@code InMemoryObjectEntry}, or null if it doesn't exist.
   */
  public synchronized InMemoryObjectEntry get(String objectName) {
    return objectLookup.get(objectName);
  }

  /**
   * Removes a previously inserted InMemoryObjectEntry and returns it; returns null if it didn't
   * exist.
   */
  public synchronized InMemoryObjectEntry remove(String objectName) {
    return objectLookup.remove(objectName);
  }

  /**
   * Gets the {@code GoogleCloudStorageItemInfo} associated with this BucketEntry; the 'size'
   * of a bucket is always 0.
   */
  public synchronized GoogleCloudStorageItemInfo getInfo() {
    return info;
  }

  /**
   * Returns the Set containing all objectNames previously inserted into this bucket.
   */
  public synchronized Set<String> getObjectNames() {
    return objectLookup.keySet();
  }

  /**
   * Returns the number of objects in this bucket.
   */
  public synchronized int size() {
    return objectLookup.size();
  }
}
