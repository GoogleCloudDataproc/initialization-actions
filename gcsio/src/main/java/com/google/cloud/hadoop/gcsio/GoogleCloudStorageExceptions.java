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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Miscellaneous helper methods for standardizing the types of exceptions thrown by the various
 * GCS-based FileSystems.
 */
public final class GoogleCloudStorageExceptions {

  private GoogleCloudStorageExceptions() {}

  /** Creates FileNotFoundException with suitable message for a GCS bucket or object. */
  public static FileNotFoundException createFileNotFoundException(
      String bucketName, String objectName, @Nullable IOException cause) {
    checkArgument(!isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    FileNotFoundException fileNotFoundException =
        new FileNotFoundException(
            String.format(
                "Item not found: '%s'. Note, it is possible that the live version"
                    + " is still available but the requested generation is deleted.",
                StringPaths.fromComponents(bucketName, nullToEmpty(objectName))));
    if (cause != null) {
      fileNotFoundException.initCause(cause);
    }
    return fileNotFoundException;
  }

  public static FileNotFoundException createFileNotFoundException(
      StorageResourceId resourceId, @Nullable IOException cause) {
    return createFileNotFoundException(
        resourceId.getBucketName(), resourceId.getObjectName(), cause);
  }

  /**
   * Creates a composite IOException out of multiple IOExceptions. If there is only a single {@code
   * innerException}, it will be returned as-is without wrapping into an outer exception.
   */
  public static IOException createCompositeException(Collection<IOException> innerExceptions) {
    checkArgument(
        innerExceptions != null && !innerExceptions.isEmpty(),
        "innerExceptions (%s) must be not null and contain at least one element",
        innerExceptions);

    Iterator<IOException> innerExceptionIterator = innerExceptions.iterator();

    if (innerExceptions.size() == 1) {
      return innerExceptionIterator.next();
    }

    IOException combined = new IOException("Multiple IOExceptions.");
    while (innerExceptionIterator.hasNext()) {
      combined.addSuppressed(innerExceptionIterator.next());
    }
    return combined;
  }

  public static GoogleJsonResponseException createJsonResponseException(
      GoogleJsonError e, HttpHeaders responseHeaders) {
    return new GoogleJsonResponseException(
        new GoogleJsonResponseException.Builder(e.getCode(), e.getMessage(), responseHeaders), e);
  }
}
