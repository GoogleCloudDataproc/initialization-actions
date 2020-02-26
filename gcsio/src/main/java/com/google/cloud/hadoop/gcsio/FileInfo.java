/*
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Contains information about a file or a directory.
 *
 * Note:
 * This class wraps GoogleCloudStorageItemInfo, adds file system specific information and hides
 * bucket/object specific information. The wrapped type should not be visible to callers of
 * GoogleCloudStorageFileSystem because it exposes non-file system information (eg, buckets).
 */
public class FileInfo {

  // Info about the root path.
  public static final FileInfo ROOT_INFO =
      new FileInfo(GoogleCloudStorageFileSystem.GCS_ROOT, GoogleCloudStorageItemInfo.ROOT_INFO);

  // Path of this file or directory.
  private final URI path;

  // Information about the underlying GCS item.
  private final GoogleCloudStorageItemInfo itemInfo;

  // Custom file attributes, including those used for storing custom modification times, etc
  private final Map<String, byte[]> attributes;

  /**
   * Constructs an instance of FileInfo.
   *
   * @param itemInfo Information about the underlying item.
   */
  private FileInfo(URI path, GoogleCloudStorageItemInfo itemInfo) {
    this.itemInfo = itemInfo;

    // Construct the path once.
    this.path = path;
    Preconditions.checkArgument(itemInfo.getMetadata() != null);

    this.attributes = itemInfo.getMetadata();
  }

  /**
   * Gets the path of this file or directory.
   */
  public URI getPath() {
    return path;
  }

  /**
   * Indicates whether this item is a directory.
   */
  public boolean isDirectory() {
    return itemInfo.isDirectory();
  }

  /**
   * Indicates whether this instance has information about the unique, shared root of the
   * underlying storage system.
   */
  public boolean isGlobalRoot() {
    return itemInfo.isGlobalRoot();
  }

  /**
   * Gets creation time of this item.
   *
   * Time is expressed as milliseconds since January 1, 1970 UTC.
   */
  public long getCreationTime() {
    return itemInfo.getCreationTime();
  }

  /**
   * Gets the size of this file or directory.
   *
   * For files, size is in number of bytes.
   * For directories size is 0.
   * For items that do not exist, size is -1.
   */
  public long getSize() {
    return itemInfo.getSize();
  }

  /**
   * Gets the modification time of this file if one is set, otherwise the value of
   * {@link #getCreationTime()} is returned.
   *
   * Time is expressed as milliseconds since January 1, 1970 UTC.
   */
  public long getModificationTime() {
    return itemInfo.getModificationTime();
  }

  /**
   * Retrieve file attributes for this file.
   * @return A map of file attributes
   */
  public Map<String, byte[]> getAttributes() {
    return attributes;
  }

  /**
   * Indicates whether this file or directory exists.
   */
  public boolean exists() {
    return itemInfo.exists();
  }

  /**
   * Gets string representation of this instance.
   */
  public String toString() {
    return getPath() + (exists() ? ": created on: " + new Date(getCreationTime()) : ": exists: no");
  }

  /**
   * Gets information about the underlying item.
   */
  public GoogleCloudStorageItemInfo getItemInfo() {
    return itemInfo;
  }

  /**
   * Converts the given resourceId to look like a directory path. If the path already looks like a
   * directory path then this call is a no-op.
   *
   * @param resourceId StorageResourceId to convert.
   * @return A resourceId with a directory path corresponding to the given resourceId.
   */
  public static StorageResourceId convertToDirectoryPath(StorageResourceId resourceId) {
    if (resourceId.isStorageObject()) {
      if (!StringPaths.isDirectoryPath(resourceId.getObjectName())) {
        resourceId =
            new StorageResourceId(
                resourceId.getBucketName(),
                StringPaths.toDirectoryPath(resourceId.getObjectName()));
      }
    }
    return resourceId;
  }

  /**
   * Converts the given path to look like a directory path. If the path already looks like a
   * directory path then this call is a no-op.
   *
   * @param path Path to convert.
   * @return Directory path for the given path.
   */
  public static URI convertToDirectoryPath(URI path) {
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ true);

    if (resourceId.isStorageObject()) {
      if (!StringPaths.isDirectoryPath(resourceId.getObjectName())) {
        resourceId = convertToDirectoryPath(resourceId);
        path = UriPaths.fromResourceId(resourceId, /* allowEmptyObjectName= */ false);
      }
    }
    return path;
  }

  /**
   * Handy factory method for constructing a FileInfo from a GoogleCloudStorageItemInfo while
   * potentially returning a singleton instead of really constructing an object for cases like ROOT.
   */
  public static FileInfo fromItemInfo(GoogleCloudStorageItemInfo itemInfo) {
    if (itemInfo.isRoot()) {
      return ROOT_INFO;
    }
    URI path = UriPaths.fromResourceId(itemInfo.getResourceId(), /* allowEmptyObjectName= */ true);
    return new FileInfo(path, itemInfo);
  }

  /**
   * Handy factory method for constructing a list of FileInfo from a list of
   * GoogleCloudStorageItemInfo.
   */
  public static List<FileInfo> fromItemInfos(List<GoogleCloudStorageItemInfo> itemInfos) {
    List<FileInfo> fileInfos = new ArrayList<>(itemInfos.size());
    for (GoogleCloudStorageItemInfo itemInfo : itemInfos) {
      fileInfos.add(fromItemInfo(itemInfo));
    }
    return fileInfos;
  }

  /**
   * Indicates whether the given path looks like a directory path.
   *
   * @param path Path to inspect.
   * @return Whether the path looks like a directory path.
   */
  public static boolean isDirectoryPath(URI path) {
    return (path != null) && path.toString().endsWith(GoogleCloudStorage.PATH_DELIMITER);
  }
}
