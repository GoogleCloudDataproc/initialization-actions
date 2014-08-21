/**
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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Options for creating objects in GCS.
 */
public class CreateObjectOptions {

  public static final CreateObjectOptions DEFAULT =
      new CreateObjectOptions(true, ImmutableMap.<String, String>of());

  private final boolean overwriteExisting;
  private final Map<String, String> metadata;

  /**
   * Construct a new CreateObjectOptions with empty metadata.
   *
   * @param overwriteExisting True to overwrite any existing objects with the same name.
   */
  public CreateObjectOptions(boolean overwriteExisting) {
    this(overwriteExisting, ImmutableMap.<String, String>of());
  }

  /**
   * @param overwriteExisting True to overwrite any existing objects with the same name.
   * @param metadata A dictionary of metadata to apply to created objects.
   */
  public CreateObjectOptions(boolean overwriteExisting, Map<String, String> metadata) {
    this.overwriteExisting = overwriteExisting;
    this.metadata = metadata;
  }

  /**
   * Get the value of overwriteExisting.
   */
  public boolean overwriteExisting() {
    return overwriteExisting;
  }

  /**
   * Custom metadata to apply to this object.
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }
}
