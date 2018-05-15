/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import java.net.URI;

/**
 * Methods for converting between GCS buckets and objects and URIs that
 * GoogleCloudStorageFileSystem can use to reference objects.
 */
public interface PathCodec {
  /**
   * Validates the given URI and if valid, returns the associated StorageResourceId.
   *
   * @param path The GCS URI to validate.
   * @param allowEmptyObjectName If true, a missing object name is not considered invalid.
   * @return a StorageResourceId that may be the GCS root, a Bucket, or a StorageObject.
   */
  public StorageResourceId validatePathAndGetId(URI path, boolean allowEmptyObjectName);

  /**
   * Constructs and returns full path for the given object name.
   */
  public URI getPath(String bucketName, String objectName, boolean allowEmptyObjectName);
}
