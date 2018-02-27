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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import javax.annotation.Nullable;

/**
 * Configuration options for the GoogleCloudStorage class.
 */
public class GoogleCloudStorageOptions {

  /** Operational modes of GCS Requester Pays feature. */
  public static enum RequesterPaysMode {
    CUSTOM,
    DISABLED,
    ENABLED
  }

  /** Default value for {@link GoogleCloudStorageOptions#requesterPaysMode}. */
  public static final RequesterPaysMode REQUESTER_PAYS_MODE_DEFAULT = RequesterPaysMode.DISABLED;

  /**
   * Default number of items to return per call to the list* GCS RPCs.
   */
  public static final long MAX_LIST_ITEMS_PER_CALL_DEFAULT = 1024;
  /**
   * Default setting for enabling auto-repair of implicit directories.
   */
  public static final boolean AUTO_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT = true;

  /**
   * Default setting for enabling inferring of implicit directories.
   */
  public static final boolean INFER_IMPLICIT_DIRECTORIES_DEFAULT = true;

  /**
   * Default setting for maximum number of requests per GCS batch.
   */
  public static final long MAX_REQUESTS_PER_BATCH_DEFAULT = 1000;

  /**
   * Default setting for whether or not to create a marker file when beginning file creation.
   */
  public static final boolean CREATE_EMPTY_MARKER_OBJECT_DEFAULT = false;

  /**
   * Default setting for the length of time to wait for empty objects to appear if we believe
   * we are in a race with multiple workers.
   */
  public static final int MAX_WAIT_MILLIS_FOR_EMPTY_OBJECT_CREATION = 3_000;

  /**
   * Mutable builder for the GoogleCloudStorageOptions class.
   */
  public static class Builder {
    private boolean autoRepairImplicitDirectoriesEnabled =
        AUTO_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT;
    private boolean inferImplicitDirectoriesEnabled =
        INFER_IMPLICIT_DIRECTORIES_DEFAULT;
    private String projectId = null;
    private RequesterPaysMode requesterPaysMode = REQUESTER_PAYS_MODE_DEFAULT;
    private String requesterPaysProjectId = null;
    private ImmutableSet<String> requesterPaysBuckets = ImmutableSet.of();
    private String appName = null;
    private HttpTransportFactory.HttpTransportType transportType =
        HttpTransportFactory.DEFAULT_TRANSPORT_TYPE;
    private String proxyAddress = null;
    private long maxListItemsPerCall = MAX_LIST_ITEMS_PER_CALL_DEFAULT;
    private boolean createMarkerObjects = CREATE_EMPTY_MARKER_OBJECT_DEFAULT;
    private int maxWaitMillisForEmptyObjectCreation = MAX_WAIT_MILLIS_FOR_EMPTY_OBJECT_CREATION;

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

    public Builder setInferImplicitDirectoriesEnabled(
        boolean inferImplicitDirectoriesEnabled) {
      this.inferImplicitDirectoriesEnabled = inferImplicitDirectoriesEnabled;
      return this;
    }

    public Builder setProjectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder setRequesterPaysMode(RequesterPaysMode requesterPaysMode) {
      this.requesterPaysMode = requesterPaysMode;
      return this;
    }

    public Builder setRequesterPaysProjectId(String requesterPaysProjectId) {
      this.requesterPaysProjectId = requesterPaysProjectId;
      return this;
    }

    public Builder setRequesterPaysBuckets(Collection<String> requesterPaysBuckets) {
      this.requesterPaysBuckets =
          requesterPaysBuckets == null
              ? ImmutableSet.<String>of()
              : ImmutableSet.copyOf(requesterPaysBuckets);
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

    public Builder setCreateMarkerObjects(boolean createMarkerObjects) {
      this.createMarkerObjects = createMarkerObjects;
      return this;
    }

    public Builder setTransportType(HttpTransportFactory.HttpTransportType transportType) {
      this.transportType = transportType;
      return this;
    }

    public Builder setProxyAddress(String proxyAddress) {
      this.proxyAddress = proxyAddress;
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

    public Builder setMaxWaitMillisForEmptyObjectCreation(
        int maxWaitMillisForEmptyObjectCreation) {
      this.maxWaitMillisForEmptyObjectCreation = maxWaitMillisForEmptyObjectCreation;
      return this;
    }

    public GoogleCloudStorageOptions build() {
      return new GoogleCloudStorageOptions(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final boolean autoRepairImplicitDirectoriesEnabled;
  private final boolean inferImplicitDirectoriesEnabled;
  private final String projectId;
  private final RequesterPaysMode requesterPaysMode;
  private final String requesterPaysProjectId;
  private final ImmutableSet<String> requesterPaysBuckets;
  private final String appName;
  private final HttpTransportFactory.HttpTransportType transportType;
  private final String proxyAddress;
  private final AsyncWriteChannelOptions writeChannelOptions;
  private final long maxListItemsPerCall;
  private final long maxRequestsPerBatch;
  private final boolean createMarkerFile;
  private final int maxWaitMillisForEmptyObjectCreation;

  protected GoogleCloudStorageOptions(Builder builder) {
    this.autoRepairImplicitDirectoriesEnabled = builder.autoRepairImplicitDirectoriesEnabled;
    this.inferImplicitDirectoriesEnabled = builder.inferImplicitDirectoriesEnabled;
    this.projectId = builder.projectId;
    this.requesterPaysMode =
        checkNotNull(builder.requesterPaysMode, "RequesterPaysMode could not be null");
    this.requesterPaysProjectId = builder.requesterPaysProjectId;
    this.requesterPaysBuckets = builder.requesterPaysBuckets;
    this.appName = builder.appName;
    this.writeChannelOptions = builder.getWriteChannelOptionsBuilder().build();
    this.maxListItemsPerCall = builder.maxListItemsPerCall;
    this.maxRequestsPerBatch = builder.maxRequestsPerBatch;
    this.createMarkerFile = builder.createMarkerObjects;
    this.transportType = builder.transportType;
    this.proxyAddress = builder.proxyAddress;
    this.maxWaitMillisForEmptyObjectCreation = builder.maxWaitMillisForEmptyObjectCreation;
  }

  public GoogleCloudStorageOptions(
      boolean autoRepairImplicitDirectoriesEnabled,
      boolean inferImplicitDirectoriesEnabled,
      String projectId,
      String appName,
      long maxListItemsPerCall,
      long maxRequestsPerBatch,
      boolean createMarkerFile,
      HttpTransportFactory.HttpTransportType transportType,
      String proxyAddress,
      AsyncWriteChannelOptions writeChannelOptions) {
    this(new Builder()
        .setProjectId(projectId)
        .setAppName(appName)
        .setWriteChannelOptionsBuilder(
            new AsyncWriteChannelOptions.Builder()
                .setUploadBufferSize(writeChannelOptions.getUploadBufferSize())
                .setDirectUploadEnabled(writeChannelOptions.isDirectUploadEnabled()))
        .setAutoRepairImplicitDirectoriesEnabled(autoRepairImplicitDirectoriesEnabled)
        .setInferImplicitDirectoriesEnabled(inferImplicitDirectoriesEnabled)
        .setMaxListItemsPerCall(maxListItemsPerCall)
        .setMaxRequestsPerBatch(maxRequestsPerBatch)
        .setTransportType(transportType)
        .setCreateMarkerObjects(createMarkerFile)
        .setProxyAddress(proxyAddress));
  }

  public boolean isAutoRepairImplicitDirectoriesEnabled() {
    return autoRepairImplicitDirectoriesEnabled;
  }

  public boolean isInferImplicitDirectoriesEnabled() {
    return inferImplicitDirectoriesEnabled;
  }

  @Nullable
  public String getProjectId() {
    return projectId;
  }

  public RequesterPaysMode getRequesterPaysMode() {
    return requesterPaysMode;
  }

  @Nullable
  public String getRequesterPaysProjectId() {
    return requesterPaysProjectId;
  }

  public ImmutableSet<String> getRequesterPaysBuckets() {
    return requesterPaysBuckets;
  }

  public String getAppName() {
    return appName;
  }

  public HttpTransportFactory.HttpTransportType getTransportType() {
    return transportType;
  }

  public String getProxyAddress() {
    return proxyAddress;
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

  public boolean isMarkerFileCreationEnabled() {
    return createMarkerFile;
  }

  public int getMaxWaitMillisForEmptyObjectCreation() {
    return maxWaitMillisForEmptyObjectCreation;
  }

  public void throwIfNotValid() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(appName),
        "appName must not be null or empty");
  }
}
