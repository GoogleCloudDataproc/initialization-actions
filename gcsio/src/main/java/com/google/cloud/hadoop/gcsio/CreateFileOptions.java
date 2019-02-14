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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Options that can be specified when creating a file in the {@code GoogleCloudFileSystem}.
 */
public class CreateFileOptions {
  public static final ImmutableMap<String, byte[]> EMPTY_ATTRIBUTES = ImmutableMap.of();
  public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
  public static final CreateFileOptions DEFAULT =
      new CreateFileOptions(/* overwriteExisting= */ true, DEFAULT_CONTENT_TYPE, EMPTY_ATTRIBUTES);

  private final boolean overwriteExisting;
  private final String contentType;
  private final Map<String, byte[]> attributes;
  private final boolean checkNoDirectoryConflict;
  private final boolean ensureParentDirectoriesExist;
  private final long existingGenerationId;

  /**
   * Create a file with empty attributes and optionally overwriting any existing file.
   *
   * @param overwriteExisting True to overwrite an existing file with the same name
   */
  public CreateFileOptions(boolean overwriteExisting) {
    this(overwriteExisting, DEFAULT_CONTENT_TYPE, EMPTY_ATTRIBUTES);
  }

  /**
   * Create a file with empty attributes, and optionally overwriting any existing file with one
   * having the given content-type.
   *
   * @param overwriteExisting True to overwrite an existing file with the same name
   * @param contentType content-type for the created file
   */
  public CreateFileOptions(boolean overwriteExisting, String contentType) {
    this(overwriteExisting, contentType, EMPTY_ATTRIBUTES);
  }

  /**
   * Create a file with specified attributes and optionally overwriting an existing file.
   *
   * @param overwriteExisting True to overwrite an existing file with the same name
   * @param attributes File attributes to apply to the file at creation
   */
  public CreateFileOptions(boolean overwriteExisting, Map<String, byte[]> attributes) {
    this(overwriteExisting, DEFAULT_CONTENT_TYPE, attributes);
  }

  /**
   * Create a file with specified attributes, and optionally overwriting an existing file with one
   * having the given content-type.
   *
   * @param overwriteExisting True to overwrite an existing file with the same name
   * @param contentType content-type for the created file
   * @param attributes File attributes to apply to the file at creation
   */
  public CreateFileOptions(boolean overwriteExisting, String contentType,
      Map<String, byte[]> attributes) {
    this(overwriteExisting, contentType, attributes, true, true,
        StorageResourceId.UNKNOWN_GENERATION_ID);
  }

  /**
   * Create a file with specified attributes, and optionally overwriting an existing file with one
   * having the given content-type.
   *
   * @param overwriteExisting True to overwrite an existing file with the same name
   * @param contentType content-type for the created file
   * @param attributes File attributes to apply to the file at creation
   * @param checkNoDirectoryConflict If true, makes sure there isn't already a directory object
   *     of the same name. If false, you run the risk of creating hard-to-cleanup/access files
   *     whose names collide with directory names. If already sure no such directory exists,
   *     then this is safe to set for improved performance.
   * @param ensureParentDirectoriesExist If true, ensures parent directories exist, creating
   *     them on-demand if they don't. If false, you run the risk of creating objects without
   *     parent directories, which may degrade or break the behavior of some filesystem
   *     functionality. If already sure parent directories exist, then this is safe to set for
   *     improved performance.
   * @param existingGenerationId Ignored if set to StorageResourceId.UNKNOWN_GENERATION_ID, but
   *     otherwise this is used instead of {@code overwriteExisting}, where 0 indicates no
   *     existing object, and otherwise an existing object will only be overwritten by the newly
   *     created file if its generation matches this provided generationId.
   */
  public CreateFileOptions(
      boolean overwriteExisting,
      String contentType,
      Map<String, byte[]> attributes,
      boolean checkNoDirectoryConflict,
      boolean ensureParentDirectoriesExist,
      long existingGenerationId) {
    Preconditions.checkArgument(!attributes.containsKey("Content-Type"),
        "The Content-Type attribute must be provided explicitly via the 'contentType' parameter");
    this.overwriteExisting = overwriteExisting;
    this.contentType = contentType;
    this.attributes = attributes;
    this.checkNoDirectoryConflict = checkNoDirectoryConflict;
    this.ensureParentDirectoriesExist = ensureParentDirectoriesExist;
    this.existingGenerationId = existingGenerationId;
  }

  /**
   * Get the value of overwriteExisting.
   */
  public boolean overwriteExisting() {
    return overwriteExisting;
  }

  /**
   * Content-type to set when creating a file.
   */
  public String getContentType() {
    return contentType;
  }

  /**
   * Extended attributes to set when creating a file.
   */
  public Map<String, byte[]> getAttributes() {
    return attributes;
  }

  /**
   * If true, makes sure there isn't already a directory object of the same name.
   * If false, you run the risk of creating hard-to-cleanup/access files whose names collide with
   * directory names. If already sure no such directory exists, then this is safe to set for
   * improved performance.
   */
  public boolean checkNoDirectoryConflict() {
    return checkNoDirectoryConflict;
  }

  /**
   * If true, ensures parent directories exist, creating them on-demand if they don't.
   * If false, you run the risk of creating objects without parent directories, which may degrade
   * or break the behavior of some filesystem functionality. If already sure parent directories
   * exist, then this is safe to set for improved performance.
   */
  public boolean ensureParentDirectoriesExist() {
    return ensureParentDirectoriesExist;
  }

  /**
   * Generation of existing object. Ignored if set to StorageResourceId.UNKNOWN_GENERATION_ID, but
   * otherwise this is used instead of {@code overwriteExisting}, where 0 indicates no
   * existing object, and otherwise an existing object will only be overwritten by the newly
   * created file if its generation matches this provided generationId.
   */
  public long getExistingGenerationId() {
    return existingGenerationId;
  }
}
