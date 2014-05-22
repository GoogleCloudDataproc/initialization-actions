/**
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


import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Configuration options for the GoogleCloudStorage class.
 */
public class GoogleCloudStorageOptions {

  /**
   * Default number of items to return per call to the list* GCS RPCs.
   */
  public static final long MAX_LIST_ITEMS_PER_CALL_DEFAULT = 1024;
  /**
   * Default setting for enabling auto-repair of implicit directories.
   */
  public static final boolean AUTO_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT = true;

  /**
   * Default setting for maximum number of requests per GCS batch.
   */
  public static final long MAX_REQUESTS_PER_BATCH_DEFAULT = 1000;

  /**
   * Mutable builder for the GoogleCloudStorageOptions class.
   */
  public static class Builder {
    private boolean autoRepairImplicitDirectoriesEnabled =
        AUTO_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT;
    private String projectId = null;
    private String appName = null;
    private long maxListItemsPerCall = MAX_LIST_ITEMS_PER_CALL_DEFAULT;

    // According to https://developers.google.com/storage/docs/json_api/v1/how-tos/batch, there is a
    // maximum of 1000 requests per batch; it should not generally be necessary to modify this value
    // manually, except possibly for testing purposes.
    private long maxRequestsPerBatch = MAX_REQUESTS_PER_BATCH_DEFAULT;

    private AsyncWriteChannelOptions.Builder writeChannelOptionsBuilder =
        new AsyncWriteChannelOptions.Builder();

    public Builder setAutoRepairImplicitDirectoriesEnabled(
        boolean autoRepairImplicitDirectoriesEnabled) {
      this.autoRepairImplicitDirectoriesEnabled = autoRepairImplicitDirectoriesEnabled;
      return this;
    }

    public Builder setProjectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder setAppName(String appName) {
      this.appName = appName;
      return this;
    }

    public Builder setMaxListItemsPerCall(long maxListItemsPerCall) {
      this.maxListItemsPerCall = maxListItemsPerCall;
      return this;
    }

    public Builder setMaxRequestsPerBatch(long maxRequestsPerBatch) {
      this.maxRequestsPerBatch = maxRequestsPerBatch;
      return this;
    }

    public Builder setWriteChannelOptionsBuilder(
        AsyncWriteChannelOptions.Builder builder) {
      writeChannelOptionsBuilder = builder;
      return this;
    }

    public AsyncWriteChannelOptions.Builder getWriteChannelOptionsBuilder() {
      return writeChannelOptionsBuilder;
    }

    public GoogleCloudStorageOptions build() {
      return new GoogleCloudStorageOptions(
          autoRepairImplicitDirectoriesEnabled,
          projectId,
          appName,
          maxListItemsPerCall,
          maxRequestsPerBatch,
          writeChannelOptionsBuilder.build());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final boolean autoRepairImplicitDirectoriesEnabled;
  private final String projectId;
  private final String appName;
  private final AsyncWriteChannelOptions writeChannelOptions;
  private final long maxListItemsPerCall;
  private final long maxRequestsPerBatch;

  public GoogleCloudStorageOptions(boolean autoRepairImplicitDirectoriesEnabled,
      String projectId, String appName, long maxListItemsPerCall, long maxRequestsPerBatch,
      AsyncWriteChannelOptions writeChannelOptions) {
    this.autoRepairImplicitDirectoriesEnabled = autoRepairImplicitDirectoriesEnabled;
    this.projectId = projectId;
    this.appName = appName;
    this.writeChannelOptions = writeChannelOptions;
    this.maxListItemsPerCall = maxListItemsPerCall;
    this.maxRequestsPerBatch = maxRequestsPerBatch;
  }

  public boolean isAutoRepairImplicitDirectoriesEnabled() {
    return autoRepairImplicitDirectoriesEnabled;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getAppName() {
    return appName;
  }

  public long getMaxListItemsPerCall() {
    return maxListItemsPerCall;
  }

  public AsyncWriteChannelOptions getWriteChannelOptions() {
    return writeChannelOptions;
  }

  public long getMaxRequestsPerBatch() {
    return maxRequestsPerBatch;
  }

  public void throwIfNotValid() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(projectId),
        "projectId must not be null or empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(appName),
        "appName must not be null or empty");
  }
}
