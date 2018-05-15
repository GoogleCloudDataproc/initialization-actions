/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import java.util.Map;

/**
 * Item information that can be updated without re-writing an entire object.
 */
public class UpdatableItemInfo {

  private final StorageResourceId storageResourceId;
  private final Map<String, byte[]> metadata;

  /**
   * @param storageResourceId The StorageResourceId for which these records pertain
   * @param metadata The object metadata for this object.
   */
  public UpdatableItemInfo(StorageResourceId storageResourceId, Map<String, byte[]> metadata) {
    this.storageResourceId = storageResourceId;
    this.metadata = metadata;
  }

  public StorageResourceId getStorageResourceId() {
    return storageResourceId;
  }

  public Map<String, byte[]> getMetadata() {
    return metadata;
  }
}
