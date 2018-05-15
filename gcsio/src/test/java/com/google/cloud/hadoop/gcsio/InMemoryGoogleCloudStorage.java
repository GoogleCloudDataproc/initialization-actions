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

package com.google.cloud.hadoop.gcsio;

import com.google.api.client.util.Clock;

/**
 * InMemoryGoogleCloudStorage overrides the public methods of GoogleCloudStorage by implementing
 * all the equivalent bucket/object semantics with local in-memory storage.
 *
 * @deprecated Switch to {@link com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage}.
 */
@Deprecated
public class InMemoryGoogleCloudStorage
    extends com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage {

  public InMemoryGoogleCloudStorage() {
    super();
  }

  public InMemoryGoogleCloudStorage(GoogleCloudStorageOptions options) {
    super(options);
  }

  public InMemoryGoogleCloudStorage(GoogleCloudStorageOptions storageOptions, Clock clock) {
    super(storageOptions, clock);
  }
}
