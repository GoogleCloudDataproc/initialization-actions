/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.base.Preconditions;
import com.google.common.flogger.GoogleLogger;
import java.net.URI;
import java.net.URISyntaxException;

/** A PathCodec that performs URI path encoding and decoding on GCS object names. */
class UriEncodingPathCodec implements PathCodec {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @Override
  public StorageResourceId validatePathAndGetId(URI path, boolean allowEmptyObjectName) {
    logger.atFine().log("validatePathAndGetId('%s', %s)", path, allowEmptyObjectName);
    Preconditions.checkNotNull(path);

    if (!GoogleCloudStorageFileSystem.SCHEME.equals(path.getScheme())) {
      throw new IllegalArgumentException(
          String.format(
              "Google Cloud Storage path supports only '%s' scheme, instead got '%s' from '%s'.",
              GoogleCloudStorageFileSystem.SCHEME, path.getScheme(), path));
    }

    if (path.equals(GoogleCloudStorageFileSystem.GCS_ROOT)) {
      return StorageResourceId.ROOT;
    }

    String bucketName = GoogleCloudStorageFileSystem.validateBucketName(path.getAuthority());
    // Note that we're using getPath here instead of rawPath, etc. This is because it is assumed
    // that the path was properly encoded in getPath (or another similar method):
    String objectName =
        GoogleCloudStorageFileSystem.validateObjectName(path.getPath(), allowEmptyObjectName);

    return isNullOrEmpty(objectName)
        ? new StorageResourceId(bucketName)
        : new StorageResourceId(bucketName, objectName);
  }

  @Override
  public URI getPath(String bucketName, String objectName, boolean allowEmptyObjectName) {
    if (allowEmptyObjectName && bucketName == null && objectName == null) {
      return GoogleCloudStorageFileSystem.GCS_ROOT;
    }

    String authority = GoogleCloudStorageFileSystem.validateBucketName(bucketName);
    String path =
        GoogleCloudStorage.PATH_DELIMITER
            + GoogleCloudStorageFileSystem.validateObjectName(objectName, allowEmptyObjectName);

    try {
      return new URI(
          GoogleCloudStorageFileSystem.SCHEME,
          authority,
          path,
          /* query= */ null,
          /* fragment= */ null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid bucket name (%s) or object name (%s)", bucketName, objectName), e);
    }
  }
}
