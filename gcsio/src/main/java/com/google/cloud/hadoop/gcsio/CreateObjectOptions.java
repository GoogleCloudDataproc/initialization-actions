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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Options for creating objects in GCS.
 */
public class CreateObjectOptions {
  public static final ImmutableMap<String, byte[]> EMPTY_METADATA = ImmutableMap.of();
  public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
  public static final String DEFAULT_CONTENT_ENCODING = null;
  public static final CreateObjectOptions DEFAULT = new CreateObjectOptions(true);

  private final boolean overwriteExisting;
  private final String contentType;
  private final String contentEncoding;
  private final Map<String, byte[]> metadata;
  private final boolean requireMetadataMatchForEmptyObjects;

  /**
   * Construct a new CreateObjectOptions with empty metadata and the default content-type.
   * Since no metadata was explicitly provided, we will set metadata-match for
   * empty objects to false.
   *
   * @param overwriteExisting True to overwrite any existing objects with the same name.
   */
  public CreateObjectOptions(boolean overwriteExisting) {
    this(overwriteExisting, DEFAULT_CONTENT_TYPE, DEFAULT_CONTENT_ENCODING, EMPTY_METADATA, false);
  }

  /**
   * Construct a new CreateObjectOptions with the specified metadata, and default content-type.
   * Since metadata was explicitly provided, even if empty, we will set metadata-match for
   * empty objects to true.
   * 
   *
   * @param overwriteExisting True to overwrite any existing objects with the same name.
   * @param metadata A dictionary of metadata to apply to created objects.
   */
  public CreateObjectOptions(boolean overwriteExisting, Map<String, byte[]> metadata) {
    this(overwriteExisting, DEFAULT_CONTENT_TYPE, metadata);
  }

  /**
   * Construct a new CreateObjectOptions with the spec metadata and content-type.
   * Since metadata was explicitly provided, even if empty, we will set metadata-match for
   * empty objects to true.
   *
   * @param overwriteExisting True to overwrite any existing objects with the same name
   * @param contentType content-type for the created file
   * @param metadata A dictionary of metadata to apply to created objects
   */
  public CreateObjectOptions(
      boolean overwriteExisting, String contentType, Map<String, byte[]> metadata) {
    this(overwriteExisting, contentType, DEFAULT_CONTENT_ENCODING, metadata, true);
  }

  /**
   * Construct a new CreateObjectOptions with the spec metadata and content-type.
   *
   * @param overwriteExisting True to overwrite any existing objects with the same name
   * @param contentType content-type for the created file
   * @param contentEncoding content-encoding for the created file
   * @param metadata A dictionary of metadata to apply to created objects
   * @param requireMetadataMatchForEmptyObjects if true, when creating an empty object and certain
   *     types of errors occur, any existing object is checked for an exact metadata match to the
   *     metadata in this CreateObjectOptions before accepting the creation as successful. If false,
   *     then on error for creating empty objects, as long as an appropriate empty object already
   *     exists, even if it holds different metadata than provided in this CreateObjectOptions
   *     instance, it may be considered created successfully.
   */
  public CreateObjectOptions(
      boolean overwriteExisting,
      String contentType,
      String contentEncoding,
      Map<String, byte[]> metadata,
      boolean requireMetadataMatchForEmptyObjects) {
    checkArgument(
        !metadata.containsKey("Content-Type"),
        "The Content-Type must be provided explicitly via the 'contentType' parameter");
    checkArgument(
        !metadata.containsKey("Content-Encoding"),
        "The Content-Encoding must be provided explicitly via the 'contentEncoding' parameter");
    this.overwriteExisting = overwriteExisting;
    this.contentType = contentType;
    this.contentEncoding = contentEncoding;
    this.metadata = metadata;
    this.requireMetadataMatchForEmptyObjects = requireMetadataMatchForEmptyObjects;
  }

  /**
   * Get the value of overwriteExisting.
   */
  public boolean overwriteExisting() {
    return overwriteExisting;
  }

  /**
   * Content type to set when creating a file.
   */
  public String getContentType() {
    return contentType;
  }

  /** Content type to set when creating a file. */
  public String getContentEncoding() {
    return contentEncoding;
  }

  /**
   * Custom metadata to apply to this object.
   */
  public Map<String, byte[]> getMetadata() {
    return metadata;
  }

  /**
   * See constructor param for details.
   */
  public boolean getRequireMetadataMatchForEmptyObjects() {
    return requireMetadataMatchForEmptyObjects;
  }
}
