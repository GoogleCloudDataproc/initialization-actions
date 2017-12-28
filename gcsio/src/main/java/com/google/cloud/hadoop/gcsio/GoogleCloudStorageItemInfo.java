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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * Contains information about an item in Google Cloud Storage.
 */
public class GoogleCloudStorageItemInfo {
  /**
   * Convenience interface for classes which want to implement Provider of
   * GoogleCloudStorageItemInfo for callers which may not know the concrete type of an object, but
   * want to check if the object happens to be an instance of this InfoProvider.
   */
  public static interface Provider {
    GoogleCloudStorageItemInfo getItemInfo();
  }

  // Info about the root of GCS namespace.
  public static final GoogleCloudStorageItemInfo ROOT_INFO =
      new GoogleCloudStorageItemInfo(StorageResourceId.ROOT, 0, 0, null, null);

  // Instead of returning null metadata, we'll return this map.
  private static final ImmutableMap<String, byte[]> EMPTY_METADATA = ImmutableMap.of();

  // The Bucket and maybe StorageObject names of the GCS "item" referenced by this object. Not null.
  private final StorageResourceId resourceId;

  // Creation time of this item.
  // Time is expressed as milliseconds since January 1, 1970 UTC.
  private final long creationTime;

  // Size of an object (number of bytes).
  // Size is -1 for items that do not exist.
  private final long size;

  // Location of this item.
  private final String location;

  // Storage class of this item.
  private final String storageClass;

  // Content-Type of this item
  private final String contentType;

  // User-supplied metadata.
  private final Map<String, byte[]> metadata;
  private final long contentGeneration;
  private final long metaGeneration;
  private final VerificationAttributes verificationAttributes;

  /**
   * Constructs an instance of GoogleCloudStorageItemInfo.
   *
   * @param resourceId identifies either root, a Bucket, or a StorageObject
   * @param creationTime Time when object was created (milliseconds since January 1, 1970 UTC).
   * @param size Size of the given object (number of bytes) or -1 if the object does not exist.
   */
  public GoogleCloudStorageItemInfo(StorageResourceId resourceId,
      long creationTime, long size, String location, String storageClass) {
    this(
        resourceId,
        creationTime,
        size,
        location,
        storageClass,
        null,
        ImmutableMap.<String, byte[]>of(),
        0 /* content generation */,
        0 /* meta generation */);
  }

  /**
   * Constructs an instance of GoogleCloudStorageItemInfo.
   *
   * @param resourceId identifies either root, a Bucket, or a StorageObject
   * @param creationTime Time when object was created (milliseconds since January 1, 1970 UTC).
   * @param size Size of the given object (number of bytes) or -1 if the object does not exist.
   * @param metadata User-supplied object metadata for this object.
   */
  public GoogleCloudStorageItemInfo(
      StorageResourceId resourceId,
      long creationTime,
      long size,
      String location,
      String storageClass,
      String contentType,
      Map<String, byte[]> metadata,
      long contentGeneration,
      long metaGeneration) {
    this(resourceId,
        creationTime,
        size,
        location,
        storageClass,
        contentType,
        metadata,
        contentGeneration,
        metaGeneration,
        new VerificationAttributes(null, null));
  }

  /**
   * Constructs an instance of GoogleCloudStorageItemInfo.
   *
   * @param resourceId identifies either root, a Bucket, or a StorageObject
   * @param creationTime Time when object was created (milliseconds since January 1, 1970 UTC).
   * @param size Size of the given object (number of bytes) or -1 if the object does not exist.
   * @param metadata User-supplied object metadata for this object.
   */
  public GoogleCloudStorageItemInfo(
      StorageResourceId resourceId,
      long creationTime,
      long size,
      String location,
      String storageClass,
      String contentType,
      Map<String, byte[]> metadata,
      long contentGeneration,
      long metaGeneration,
      VerificationAttributes verificationAttributes) {
    Preconditions.checkArgument(resourceId != null,
        "resourceId must not be null! Use StorageResourceId.ROOT to represent GCS root.");
    this.resourceId = resourceId;
    this.creationTime = creationTime;
    this.size = size;
    this.location = location;
    this.storageClass = storageClass;
    this.contentType = contentType;
    if (metadata == null) {
      this.metadata = EMPTY_METADATA;
    } else {
      this.metadata = metadata;
    }
    this.contentGeneration = contentGeneration;
    this.metaGeneration = metaGeneration;
    this.verificationAttributes = verificationAttributes;
  }

  /**
   * Gets bucket name of this item.
   */
  public String getBucketName() {
    return resourceId.getBucketName();
  }

  /**
   * Gets object name of this item.
   */
  public String getObjectName() {
    return resourceId.getObjectName();
  }

  /**
   * Gets the resourceId that holds the (possibly null) bucketName and objectName of this object.
   */
  public StorageResourceId getResourceId() {
    return resourceId;
  }

  /**
   * Gets creation time of this item.
   *
   * Time is expressed as milliseconds since January 1, 1970 UTC.
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Gets size of this item (number of bytes). Returns -1 if the object
   * does not exist.
   */
  public long getSize() {
    return size;
  }

  /**
   * Gets location of this item.
   *
   * Note: Location is only supported for buckets. The value is always null for objects.
   */
  public String getLocation() {
    return location;
  }

  /**
   * Gets storage class of this item.
   *
   * Note: Storage-class is only supported for buckets. The value is always null for objects.
   */
  public String getStorageClass() {
    return storageClass;
  }

  /**
   * Gets the content-type of this item, or null if unknown or inapplicable.
   *
   * Note: content-type is only supported for objects, and will always be null for buckets.
   */
  public String getContentType() {
    return contentType;
  }

  /**
   * Gets user-supplied metadata for this item.
   *
   * Note: metadata is only supported for objects. This value is always an empty map for buckets.
   */
  public Map<String, byte[]> getMetadata() {
    return metadata;
  }

  /**
   * Indicates whether this item is a bucket. Root is not considered to be a bucket.
   */
  public boolean isBucket() {
    return resourceId.isBucket();
  }

  /**
   * Indicates whether this item refers to the GCS root (gs://).
   */
  public boolean isRoot() {
    return resourceId.isRoot();
  }

  /**
   * Indicates whether this item exists.
   */
  public boolean exists() {
    return size >= 0;
  }

  /**
   * Get the content generation of the object.
   */
  public long getContentGeneration() {
    return contentGeneration;
  }

  /**
   * Get the meta generation of the object.
   */
  public long getMetaGeneration() {
    return metaGeneration;
  }

  /**
   * Get object validation attributes.
   */
  public VerificationAttributes getVerificationAttributes() {
    return verificationAttributes;
  }

  /**
   * Helper for checking logical equality of metadata maps, checking equality of keySet() between
   * this.metadata and otherMetadata, and then using Arrays.equals to compare contents of
   * corresponding byte arrays.
   */
  public boolean metadataEquals(Map<String, byte[]> otherMetadata) {
    if (metadata == otherMetadata) {
      // Fast-path for common cases where the same actual default metadata instance may be used in
      // multiple different item infos.
      return true;
    }
    if ((metadata == null && otherMetadata != null)
        || (metadata != null && otherMetadata == null)) {
      return false;
    }
    if (!metadata.keySet().equals(otherMetadata.keySet())) {
      return false;
    }

    // Compare each byte[] with Arrays.equals.
    for (String key : metadata.keySet()) {
      if (!Arrays.equals(metadata.get(key), otherMetadata.get(key))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets string representation of this instance.
   */
  @Override
  public String toString() {
    if (exists()) {
      return String.format("%s: created on: %s",
          resourceId, (new Date(creationTime)).toString());
    } else {
      return String.format("%s: exists: no", resourceId.toString());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GoogleCloudStorageItemInfo) {
      GoogleCloudStorageItemInfo other = (GoogleCloudStorageItemInfo) obj;
      return resourceId.equals(other.resourceId)
          && creationTime == other.creationTime
          && size == other.size
          && Objects.equals(location, other.location)
          && Objects.equals(storageClass, other.storageClass)
          && Objects.equals(verificationAttributes, other.verificationAttributes)
          && metaGeneration == other.metaGeneration
          && contentGeneration == other.contentGeneration
          && metadataEquals(other.getMetadata());
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + resourceId.hashCode();
    result = prime * result + (int) creationTime;
    result = prime * result + (int) size;
    result = prime * result + Objects.hashCode(location);
    result = prime * result + Objects.hashCode(storageClass);
    result = prime * result + Objects.hashCode(verificationAttributes);
    result = prime * result + (int) metaGeneration;
    result = prime * result + (int) contentGeneration;
    result = prime * result + metadata.hashCode();
    return result;
  }
}
