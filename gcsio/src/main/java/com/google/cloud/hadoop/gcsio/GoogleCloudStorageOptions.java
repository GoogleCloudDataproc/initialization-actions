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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.services.storage.Storage;
import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.gcsio.cooplock.CooperativeLockingOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import javax.annotation.Nullable;

/** Configuration options for the GoogleCloudStorage class. */
@AutoValue
public abstract class GoogleCloudStorageOptions {

  /** Default root URL for Cloud Storage API endpoint. */
  public static final String STORAGE_ROOT_URL_DEFAULT = Storage.DEFAULT_ROOT_URL;

  /** Default setting for enabling auto-repair of implicit directories. */
  public static final boolean AUTO_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT = true;

  /** Default setting for enabling inferring of implicit directories. */
  public static final boolean INFER_IMPLICIT_DIRECTORIES_DEFAULT = true;

  /**
   * Default setting for the length of time to wait for empty objects to appear if we believe we are
   * in a race with multiple workers.
   */
  public static final int MAX_WAIT_MILLIS_FOR_EMPTY_OBJECT_CREATION = 3_000;

  /** Default number of items to return per call to the list* GCS RPCs. */
  public static final long MAX_LIST_ITEMS_PER_CALL_DEFAULT = 1024;

  /** Default setting for maximum number of requests per GCS batch. */
  public static final long MAX_REQUESTS_PER_BATCH_DEFAULT = 30;

  /** Default setting for number of threads to execute GCS batch requests. */
  public static final int BATCH_THREADS_DEFAULT = 0;

  /** Default setting for maximum number of GCS HTTP request retires. */
  public static final int MAX_HTTP_REQUEST_RETRIES = 10;

  /** Default setting for connect timeout (in millisecond) of GCS HTTP request. */
  public static final int HTTP_REQUEST_CONNECT_TIMEOUT = 20 * 1000;

  /** Default setting for read timeout (in millisecond) of GCS HTTP request. */
  public static final int HTTP_REQUEST_READ_TIMEOUT = 20 * 1000;

  /** Default setting for whether or not to use rewrite request for copy operation. */
  public static final boolean COPY_WITH_REWRITE_DEFAULT = false;

  /** Default setting for max number of bytes rewritten per rewrite request/call. */
  public static final int MAX_BYTES_REWRITTEN_PER_CALL_DEFAULT = 0;

  /** Default setting for maximum number of requests per GCS batch for copy operations. */
  public static final long COPY_MAX_REQUESTS_PER_BATCH_DEFAULT = MAX_REQUESTS_PER_BATCH_DEFAULT;

  /** Default setting for number of threads to execute GCS batch requests for copy operations. */
  public static final int COPY_BATCH_THREADS_DEFAULT = BATCH_THREADS_DEFAULT;

  public static final GoogleCloudStorageOptions DEFAULT = builder().build();

  /** @deprecated use {@link #builder()} instead */
  @Deprecated
  public static Builder newBuilder() {
    return builder();
  }

  public static Builder builder() {
    return new AutoValue_GoogleCloudStorageOptions.Builder()
        .setStorageRootUrl(STORAGE_ROOT_URL_DEFAULT)
        .setAutoRepairImplicitDirectoriesEnabled(AUTO_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT)
        .setInferImplicitDirectoriesEnabled(INFER_IMPLICIT_DIRECTORIES_DEFAULT)
        .setMaxWaitMillisForEmptyObjectCreation(MAX_WAIT_MILLIS_FOR_EMPTY_OBJECT_CREATION)
        .setMaxListItemsPerCall(MAX_LIST_ITEMS_PER_CALL_DEFAULT)
        .setMaxRequestsPerBatch(MAX_REQUESTS_PER_BATCH_DEFAULT)
        .setBatchThreads(BATCH_THREADS_DEFAULT)
        .setMaxHttpRequestRetries(MAX_HTTP_REQUEST_RETRIES)
        .setHttpRequestConnectTimeout(HTTP_REQUEST_CONNECT_TIMEOUT)
        .setHttpRequestReadTimeout(HTTP_REQUEST_READ_TIMEOUT)
        .setTransportType(HttpTransportFactory.DEFAULT_TRANSPORT_TYPE)
        .setCopyWithRewriteEnabled(COPY_WITH_REWRITE_DEFAULT)
        .setMaxBytesRewrittenPerCall(MAX_BYTES_REWRITTEN_PER_CALL_DEFAULT)
        .setCopyMaxRequestsPerBatch(COPY_MAX_REQUESTS_PER_BATCH_DEFAULT)
        .setCopyBatchThreads(COPY_BATCH_THREADS_DEFAULT)
        .setReadChannelOptions(GoogleCloudStorageReadOptions.DEFAULT)
        .setWriteChannelOptions(AsyncWriteChannelOptions.DEFAULT)
        .setRequesterPaysOptions(RequesterPaysOptions.DEFAULT)
        .setCooperativeLockingOptions(CooperativeLockingOptions.DEFAULT);
  }

  public abstract String getStorageRootUrl();

  @Nullable
  public abstract String getProjectId();

  @Nullable
  public abstract String getAppName();

  public abstract boolean isAutoRepairImplicitDirectoriesEnabled();

  public abstract boolean isInferImplicitDirectoriesEnabled();

  public abstract int getMaxWaitMillisForEmptyObjectCreation();

  public abstract long getMaxListItemsPerCall();

  public abstract long getMaxRequestsPerBatch();

  public abstract int getBatchThreads();

  public abstract long getCopyMaxRequestsPerBatch();

  public abstract int getCopyBatchThreads();

  public abstract int getMaxHttpRequestRetries();

  public abstract int getHttpRequestConnectTimeout();

  public abstract int getHttpRequestReadTimeout();

  public abstract HttpTransportFactory.HttpTransportType getTransportType();

  @Nullable
  public abstract String getProxyAddress();

  @Nullable
  public abstract String getProxyUsername();

  @Nullable
  public abstract String getProxyPassword();

  public abstract boolean isCopyWithRewriteEnabled();

  public abstract long getMaxBytesRewrittenPerCall();

  public abstract GoogleCloudStorageReadOptions getReadChannelOptions();

  public abstract AsyncWriteChannelOptions getWriteChannelOptions();

  public abstract RequesterPaysOptions getRequesterPaysOptions();

  public abstract CooperativeLockingOptions getCooperativeLockingOptions();

  public abstract Builder toBuilder();

  public void throwIfNotValid() {
    checkArgument(!isNullOrEmpty(getAppName()), "appName must not be null or empty");
  }

  /** Mutable builder for the {@link GoogleCloudStorageOptions} class. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setStorageRootUrl(String rootUrl);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setAppName(String appName);

    public abstract Builder setAutoRepairImplicitDirectoriesEnabled(boolean autoRepair);

    public abstract Builder setInferImplicitDirectoriesEnabled(boolean inferImplicitDirectories);

    public abstract Builder setMaxWaitMillisForEmptyObjectCreation(int durationMillis);

    public abstract Builder setMaxListItemsPerCall(long maxListItemsPerCall);

    // According to https://developers.google.com/storage/docs/json_api/v1/how-tos/batch
    // there is a maximum of 1000 requests per batch.
    public abstract Builder setMaxRequestsPerBatch(long maxRequestsPerBatch);

    public abstract Builder setBatchThreads(int batchThreads);

    public abstract Builder setMaxHttpRequestRetries(int maxHttpRequestRetries);

    public abstract Builder setHttpRequestConnectTimeout(int httpRequestConnectTimeout);

    public abstract Builder setHttpRequestReadTimeout(int httpRequestReadTimeout);

    public abstract Builder setTransportType(HttpTransportFactory.HttpTransportType transportType);

    public abstract Builder setProxyAddress(String proxyAddress);

    public abstract Builder setProxyUsername(String proxyUsername);

    public abstract Builder setProxyPassword(String proxyPassword);

    public abstract Builder setCopyWithRewriteEnabled(boolean copyWithRewrite);

    public abstract Builder setMaxBytesRewrittenPerCall(long bytes);

    public abstract Builder setCopyMaxRequestsPerBatch(long copyMaxRequestsPerBatch);

    public abstract Builder setCopyBatchThreads(int copyBatchThreads);

    public abstract Builder setReadChannelOptions(GoogleCloudStorageReadOptions readChannelOptions);

    public abstract Builder setWriteChannelOptions(AsyncWriteChannelOptions writeChannelOptions);

    public abstract Builder setRequesterPaysOptions(RequesterPaysOptions requesterPaysOptions);

    public abstract Builder setCooperativeLockingOptions(
        CooperativeLockingOptions cooperativeLockingOptions);

    abstract GoogleCloudStorageOptions autoBuild();

    public GoogleCloudStorageOptions build() {
      GoogleCloudStorageOptions instance = autoBuild();
      checkArgument(
          instance.getMaxBytesRewrittenPerCall() <= 0
              || instance.getMaxBytesRewrittenPerCall() % (1024 * 1024) == 0,
          "maxBytesRewrittenPerCall must be an integral multiple of 1 MiB (1048576), but was: %s",
          instance.getMaxBytesRewrittenPerCall());
      checkArgument(
          instance.getProxyAddress() != null
              || (instance.getProxyUsername() == null && instance.getProxyPassword() == null),
          "if proxyAddress is null then proxyUsername and proxyPassword should be null too");
      checkArgument(
          (instance.getProxyUsername() == null) == (instance.getProxyPassword() == null),
          "both proxyUsername and proxyPassword should be null or not null together");
      return instance;
    }
  }
}
