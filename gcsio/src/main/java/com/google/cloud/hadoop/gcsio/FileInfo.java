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

import com.google.api.client.util.Clock;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.flogger.GoogleLogger;
import com.google.common.primitives.Longs;
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

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Info about the root path.
  public static final FileInfo ROOT_INFO = new FileInfo(
      GoogleCloudStorageFileSystem.GCS_ROOT, GoogleCloudStorageItemInfo.ROOT_INFO);

  // The metadata key used for storing a custom modification time.
  // The name and value of this attribute count towards storage pricing.
  public static final String FILE_MODIFICATION_TIMESTAMP_KEY = "system.gcsfs_mts";

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
    return isDirectory(itemInfo);
  }

  /**
   * Indicates whether {@code itemInfo} is a directory; static version of {@link #isDirectory()}
   * to avoid having to create a FileInfo object just to use this logic.
   */
  static boolean isDirectory(GoogleCloudStorageItemInfo itemInfo) {
    return isGlobalRoot(itemInfo)
        || itemInfo.isBucket()
        || objectHasDirectoryPath(itemInfo.getObjectName());
  }

  /**
   * Indicates whether this instance has information about the unique, shared root of the
   * underlying storage system.
   */
  public boolean isGlobalRoot() {
    return isGlobalRoot(itemInfo);
  }

  /**
   * Static version of {@link #isGlobalRoot()} to allow sharing this logic without creating
   * unnecessary FileInfo instances.
   */
  static boolean isGlobalRoot(GoogleCloudStorageItemInfo itemInfo) {
    return itemInfo.isRoot()
        && itemInfo.exists();
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
    if (attributes.containsKey(FILE_MODIFICATION_TIMESTAMP_KEY)
        && attributes.get(FILE_MODIFICATION_TIMESTAMP_KEY) != null) {
      try {
        return Longs.fromByteArray(attributes.get(FILE_MODIFICATION_TIMESTAMP_KEY));
      } catch (IllegalArgumentException iae) {
        logger.atFine().log(
            "Failed to parse modification time '%s' millis for object %s",
            attributes.get(FILE_MODIFICATION_TIMESTAMP_KEY), itemInfo.getObjectName());
      }
    }
    return getCreationTime();
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
    if (exists()) {
      return String.format("%s: created on: %s",
          getPath(), (new Date(getCreationTime())).toString());
    } else {
      return String.format("%s: exists: no", getPath());
    }
  }

  /**
   * Gets information about the underlying item.
   */
  public GoogleCloudStorageItemInfo getItemInfo() {
    return itemInfo;
  }

  /**
   * Indicates whether the given object name looks like a directory path.
   *
   * @param objectName Name of the object to inspect.
   * @return Whether the given object name looks like a directory path.
   */
  static boolean objectHasDirectoryPath(String objectName) {
    return StorageResourceId.objectHasDirectoryPath(objectName);
  }

  /**
   * Converts the given object name to look like a directory path.
   * If the object name already looks like a directory path then
   * this call is a no-op.
   *
   * If the object name is null or empty, it is returned as-is.
   *
   * @param objectName Name of the object to inspect.
   * @return Directory path for the given path.
   */
  static String convertToDirectoryPath(String objectName) {
    return StorageResourceId.convertToDirectoryPath(objectName);
  }

  /**
   * Add a key and value representing the current time, as determined by the passed clock, to the
   * passed attributes dictionary.
   * @param attributes The file attributes map to update
   * @param clock The clock to retrieve the current time from
   */
  public static void addModificationTimeToAttributes(Map<String, byte[]> attributes, Clock clock) {
    attributes.put(FILE_MODIFICATION_TIMESTAMP_KEY, Longs.toByteArray(clock.currentTimeMillis()));
  }

  /**
   * Converts the given object name to look like a file path.
   * If the object name already looks like a file path then
   * this call is a no-op.
   *
   * If the object name is null or empty, it is returned as-is.
   *
   * @param objectName Name of the object to inspect.
   * @return File path for the given path.
   */
  public static String convertToFilePath(String objectName) {
    if (!Strings.isNullOrEmpty(objectName)) {
      if (objectHasDirectoryPath(objectName)) {
        objectName = objectName.substring(0, objectName.length() - 1);
      }
    }
    return objectName;
  }

  /**
   * Handy factory method for constructing a FileInfo from a GoogleCloudStorageItemInfo while
   * potentially returning a singleton instead of really constructing an object for cases like ROOT.
   */
  public static FileInfo fromItemInfo(PathCodec pathCodec, GoogleCloudStorageItemInfo itemInfo) {
    if (itemInfo.isRoot()) {
      return ROOT_INFO;
    }
    URI path = pathCodec.getPath(itemInfo.getBucketName(), itemInfo.getObjectName(), true);
    return new FileInfo(path, itemInfo);
  }

  /**
   * Handy factory method for constructing a list of FileInfo from a list of
   * GoogleCloudStorageItemInfo.
   */
  public static List<FileInfo> fromItemInfos(
      PathCodec pathCodec, List<GoogleCloudStorageItemInfo> itemInfos) {
    List<FileInfo> fileInfos = new ArrayList<>(itemInfos.size());
    for (GoogleCloudStorageItemInfo itemInfo : itemInfos) {
      fileInfos.add(fromItemInfo(pathCodec, itemInfo));
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

  /**
   * Converts the given resourceId to look like a directory path.
   * If the path already looks like a directory path then
   * this call is a no-op.
   *
   * @param resourceId StorageResourceId to convert.
   * @return A resourceId with a directory path corresponding to the given resourceId.
   */
  public static StorageResourceId convertToDirectoryPath(StorageResourceId resourceId) {
    if (resourceId.isStorageObject()) {
      if (!objectHasDirectoryPath(resourceId.getObjectName())) {
        resourceId = new StorageResourceId(
            resourceId.getBucketName(), convertToDirectoryPath(resourceId.getObjectName()));
      }
    }
    return resourceId;
  }

  /**
   * Converts the given path to look like a directory path.
   * If the path already looks like a directory path then
   * this call is a no-op.
   *
   * @param path Path to convert.
   * @return Directory path for the given path.
   */
  public static URI convertToDirectoryPath(PathCodec pathCodec, URI path) {
    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, true);

    if (resourceId.isStorageObject()) {
      if (!objectHasDirectoryPath(resourceId.getObjectName())) {
        resourceId = convertToDirectoryPath(resourceId);
        path = pathCodec.getPath(
            resourceId.getBucketName(), resourceId.getObjectName(), false /* allow empty name */);
      }
    }
    return path;
  }
}
