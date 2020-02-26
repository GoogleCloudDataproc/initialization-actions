/*
 * Copyright 2020 Google LLC. All Rights Reserved.
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.flogger.GoogleLogger;

/** Utility methods for String GCS paths */
public final class StringPaths {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private StringPaths() {}

  // 14x faster (20ns vs 280ns) than "^[a-z0-9_.-]+$" regex
  private static final CharMatcher BUCKET_NAME_CHAR_MATCHER =
      CharMatcher.ascii()
          .and(
              CharMatcher.inRange('0', '9')
                  .or(CharMatcher.inRange('a', 'z'))
                  .or(CharMatcher.anyOf("_.-")))
          .precomputed();

  /**
   * Validate the given bucket name to make sure that it can be used as a part of a file system
   * path.
   *
   * <p>Note: this is not designed to duplicate the exact checks that GCS would perform on the
   * server side. We make some checks that are relevant to using GCS as a file system.
   *
   * @param bucketName Bucket name to check.
   */
  static String validateBucketName(String bucketName) {
    // If the name ends with '/', remove it.
    bucketName = toFilePath(bucketName);

    if (Strings.isNullOrEmpty(bucketName)) {
      throw new IllegalArgumentException("GCS bucket name cannot be empty.");
    }

    if (!BUCKET_NAME_CHAR_MATCHER.matchesAllOf(bucketName)) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid GCS bucket name '%s': bucket name must contain only 'a-z0-9_.-' characters.",
              bucketName));
    }

    return bucketName;
  }

  /**
   * Validate the given object name to make sure that it can be used as a part of a file system
   * path.
   *
   * <p>Note: this is not designed to duplicate the exact checks that GCS would perform on the
   * server side. We make some checks that are relevant to using GCS as a file system.
   *
   * @param objectName Object name to check.
   * @param allowEmptyObjectName If true, a missing object name is not considered invalid.
   */
  static String validateObjectName(String objectName, boolean allowEmptyObjectName) {
    logger.atFine().log("validateObjectName('%s', %s)", objectName, allowEmptyObjectName);

    if (isNullOrEmpty(objectName) || objectName.equals(PATH_DELIMITER)) {
      if (allowEmptyObjectName) {
        objectName = "";
      } else {
        throw new IllegalArgumentException(
            String.format(
                "GCS path must include non-empty object name [objectName='%s',"
                    + " allowEmptyObjectName=%s]",
                objectName, allowEmptyObjectName));
      }
    }

    // We want objectName to look like a traditional file system path,
    // therefore, disallow objectName with consecutive '/' chars.
    for (int i = 0; i < (objectName.length() - 1); i++) {
      if (objectName.charAt(i) == '/' && objectName.charAt(i + 1) == '/') {
        throw new IllegalArgumentException(
            String.format("GCS path must not have consecutive '/' characters: '%s'", objectName));
      }
    }

    // Remove leading '/' if it exists.
    if (objectName.startsWith(PATH_DELIMITER)) {
      objectName = objectName.substring(1);
    }

    logger.atFine().log("validateObjectName -> '%s'", objectName);
    return objectName;
  }

  /**
   * Helper for standardizing the way various human-readable messages in logs/exceptions that refer
   * to a bucket/object pair.
   */
  public static String fromComponents(String bucketName, String objectName) {
    if (bucketName == null && objectName != null) {
      throw new IllegalArgumentException(
          String.format("Invalid bucketName/objectName pair: gs://%s/%s", bucketName, objectName));
    }
    // TODO(user): Unify this method with other methods that convert bucketName/objectName
    // to a URI; maybe use the single slash for compatibility.
    StringBuilder result = new StringBuilder("gs://");
    if (bucketName != null) {
      result.append(bucketName);
    }
    if (objectName != null) {
      result.append('/').append(objectName);
    }
    return result.toString();
  }

  /**
   * Indicates whether the given object name looks like a directory path.
   *
   * @param path Name of the object to inspect.
   * @return Whether the given object name looks like a directory path.
   */
  public static boolean isDirectoryPath(String path) {
    return !Strings.isNullOrEmpty(path) && path.endsWith(GoogleCloudStorage.PATH_DELIMITER);
  }

  /**
   * Converts the given object name to look like a file path. If the object name already looks like
   * a file path then this call is a no-op.
   *
   * <p>If the object name is null or empty, it is returned as-is.
   *
   * @param path Name of the object to inspect.
   * @return File path for the given path.
   */
  public static String toFilePath(String path) {
    return !Strings.isNullOrEmpty(path) && StringPaths.isDirectoryPath(path)
        ? path.substring(0, path.length() - 1)
        : path;
  }

  /**
   * Converts the given object name to look like a directory path. If the object name already looks
   * like a directory path then this call is a no-op.
   *
   * <p>If the object name is null or empty, it is returned as-is.
   *
   * @param path Name of the object to inspect.
   * @return Directory path for the given path.
   */
  static String toDirectoryPath(String path) {
    return Strings.isNullOrEmpty(path) || isDirectoryPath(path)
        ? path
        : path + GoogleCloudStorage.PATH_DELIMITER;
  }
}
