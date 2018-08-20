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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.flogger.GoogleLogger;
import java.net.URI;
import java.net.URISyntaxException;

/**
* A GoogleCloudStorageFileSystem PathCodec that was in use until version 1.4.4.
*/
class LegacyPathCodec implements PathCodec {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @Override
  public StorageResourceId validatePathAndGetId(URI path, boolean allowEmptyObjectName) {
    logger.atFine().log("validatePathAndGetId('%s', %s)", path, allowEmptyObjectName);
    Preconditions.checkNotNull(path);

    if (!GoogleCloudStorageFileSystem.SCHEME.equals(path.getScheme())) {
      throw new IllegalArgumentException(
          "Google Cloud Storage path supports only '"
              + GoogleCloudStorageFileSystem.SCHEME + "' scheme, instead got '"
              + path.getScheme() + "' from '" + path + "'.");
    }

    String bucketName;
    String objectName;

    if (path.equals(GoogleCloudStorageFileSystem.GCS_ROOT)) {
      return StorageResourceId.ROOT;
    } else {
      bucketName = path.getAuthority();

      // We want not only the raw path, but also any "query" or "fragment" at the end; URI doesn't
      // have a method for "everything past the authority", so instead we start with the entire
      // scheme-specific part and strip off the authority.
      String schemeSpecificPart = path.getRawSchemeSpecificPart();
      Preconditions.checkState(schemeSpecificPart.startsWith("//" + bucketName),
          "Expected schemeSpecificStart to start with '//%s', instead got '%s'",
          bucketName, schemeSpecificPart);
      objectName = schemeSpecificPart.substring(2 + bucketName.length());

      bucketName =
          GoogleCloudStorageFileSystem.validateBucketName(bucketName);
      objectName =
          GoogleCloudStorageFileSystem.validateObjectName(objectName, allowEmptyObjectName);

      // TODO(user): Pull the logic for checking empty object name out of validateObjectName into
      // here.
      if (Strings.isNullOrEmpty(objectName)) {
        return new StorageResourceId(bucketName);
      } else {
        return new StorageResourceId(bucketName, objectName);
      }
    }
  }

  @Override
  public URI getPath(String bucketName, String objectName, boolean allowEmptyObjectName) {
    if (allowEmptyObjectName && (bucketName == null) && (objectName == null)) {
      return GoogleCloudStorageFileSystem.GCS_ROOT;
    }

    bucketName =
        GoogleCloudStorageFileSystem.validateBucketName(bucketName);
    objectName =
        GoogleCloudStorageFileSystem.validateObjectName(objectName, allowEmptyObjectName);

    URI pathUri = null;
    String path = GoogleCloudStorageFileSystem.SCHEME + "://"
        + bucketName + GoogleCloudStorage.PATH_DELIMITER + objectName;
    try {
      pathUri = new URI(path);
    } catch (URISyntaxException e) {
      // This should be very rare given the earlier checks.
      String msg = String.format("Invalid bucket name (%s) or object name (%s)",
          bucketName, objectName);
      throw new IllegalArgumentException(msg, e);
    }

    return pathUri;
  }
}
