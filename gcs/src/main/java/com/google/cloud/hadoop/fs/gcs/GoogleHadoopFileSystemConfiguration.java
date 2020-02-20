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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.PATH_CODEC_USE_URI_ENCODING;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.GcsFileChecksumType;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.OutputStreamType;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.GenerationReadConsistency;
import com.google.cloud.hadoop.gcsio.PerformanceCachingGoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.cooplock.CooperativeLockingOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.HadoopConfigurationProperty;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;

/** This class provides a configuration for the {@link GoogleHadoopFileSystem} implementations. */
public class GoogleHadoopFileSystemConfiguration {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static final String GCS_CONFIG_PREFIX = "fs.gs";

  // -----------------------------------------------------------------
  // Configuration settings.
  // -----------------------------------------------------------------

  /** Configuration key for the Cloud Storage API endpoint root URL. */
  public static final HadoopConfigurationProperty<String> GCS_ROOT_URL =
      new HadoopConfigurationProperty<>(
          "fs.gs.storage.root.url", GoogleCloudStorageOptions.STORAGE_ROOT_URL_DEFAULT);

  /**
   * Key for the permissions that we report a file or directory to have. Can either be octal or
   * symbolic mode accepted by {@link FsPermission#FsPermission(String)}
   *
   * <p>Default value for the permissions that we report a file or directory to have. Note: We do
   * not really support file/dir permissions but we need to report some permission value when Hadoop
   * calls getFileStatus(). A MapReduce job fails if we report permissions more relaxed than the
   * value below and this is the default File System.
   */
  public static final HadoopConfigurationProperty<String> PERMISSIONS_TO_REPORT =
      new HadoopConfigurationProperty<>("fs.gs.reported.permissions", "700");

  /**
   * Configuration key for default block size of a file.
   *
   * <p>Note that this is the size that is reported to Hadoop FS clients. It does not modify the
   * actual block size of an underlying GCS object, because GCS JSON API does not allow modifying or
   * querying the value. Modifying this value allows one to control how many mappers are used to
   * process a given file.
   */
  public static final HadoopConfigurationProperty<Long> BLOCK_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.block.size", 64 * 1024 * 1024L);

  /** Configuration key for Delegation Token binding class. Default value: none */
  public static final HadoopConfigurationProperty<String> DELEGATION_TOKEN_BINDING_CLASS =
      new HadoopConfigurationProperty<>("fs.gs.delegation.token.binding");

  /** Configuration key for GCS project ID. Default value: none */
  public static final HadoopConfigurationProperty<String> GCS_PROJECT_ID =
      new HadoopConfigurationProperty<>("fs.gs.project.id");

  /** Configuration key for initial working directory of a GHFS instance. Default value: '/' */
  public static final HadoopConfigurationProperty<String> GCS_WORKING_DIRECTORY =
      new HadoopConfigurationProperty<>("fs.gs.working.dir", "/");

  /**
   * If true, recursive delete on a path that refers to a GCS bucket itself ('/' for any
   * bucket-rooted GoogleHadoopFileSystem) or delete on that path when it's empty will result in
   * fully deleting the GCS bucket. If false, any operation that normally would have deleted the
   * bucket will be ignored instead. Setting to 'false' preserves the typical behavior of "rm -rf /"
   * which translates to deleting everything inside of root, but without clobbering the filesystem
   * authority corresponding to that root path in the process.
   */
  public static final HadoopConfigurationProperty<Boolean> GCE_BUCKET_DELETE_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.bucket.delete.enable", false);

  /** Configuration key for GCS project ID. Default value: "DISABLED" */
  public static final HadoopConfigurationProperty<RequesterPaysMode> GCS_REQUESTER_PAYS_MODE =
      new HadoopConfigurationProperty<>(
          "fs.gs.requester.pays.mode", RequesterPaysOptions.REQUESTER_PAYS_MODE_DEFAULT);

  /** Configuration key for GCS Requester Pays Project ID. Default value: none */
  public static final HadoopConfigurationProperty<String> GCS_REQUESTER_PAYS_PROJECT_ID =
      new HadoopConfigurationProperty<>("fs.gs.requester.pays.project.id");

  /** Configuration key for GCS Requester Pays Buckets. Default value: none */
  public static final HadoopConfigurationProperty<Collection<String>> GCS_REQUESTER_PAYS_BUCKETS =
      new HadoopConfigurationProperty<>("fs.gs.requester.pays.buckets", ImmutableList.of());

  /**
   * Configuration key for which type of FileChecksum to return; if a particular file doesn't
   * support the requested type, then getFileChecksum() will return null for that file.
   */
  public static final HadoopConfigurationProperty<GcsFileChecksumType> GCS_FILE_CHECKSUM_TYPE =
      new HadoopConfigurationProperty<>("fs.gs.checksum.type", GcsFileChecksumType.NONE);

  /**
   * Configuration key for using a local item cache to supplement GCS API "getFile" results. This
   * provides faster access to recently queried data. Because the data is cached, modifications made
   * outside of this instance may not be immediately reflected. The performance cache can be used in
   * conjunction with other caching options.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_PERFORMANCE_CACHE_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.performance.cache.enable", false);

  /**
   * Configuration key for maximum number of milliseconds a GoogleCloudStorageItemInfo will remain
   * "valid" in the performance cache before it's invalidated.
   */
  public static final HadoopConfigurationProperty<Long> GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS =
      new HadoopConfigurationProperty<>(
          "fs.gs.performance.cache.max.entry.age.ms",
          PerformanceCachingGoogleCloudStorageOptions.MAX_ENTRY_AGE_MILLIS_DEFAULT);

  /** Configuration key for whether or not to enable list caching for the performance cache. */
  public static final HadoopConfigurationProperty<Boolean>
      GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE =
          new HadoopConfigurationProperty<>(
              "fs.gs.performance.cache.list.caching.enable",
              PerformanceCachingGoogleCloudStorageOptions.LIST_CACHING_ENABLED);

  /**
   * If true, executes GCS requests in {@code listStatus} and {@code getFileStatus} methods in
   * parallel to reduce latency.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_STATUS_PARALLEL_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.status.parallel.enable", false);

  /** Configuration key for enabling lazy initialization of GCS FS instance. */
  public static final HadoopConfigurationProperty<Boolean> GCS_LAZY_INITIALIZATION_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.lazy.init.enable", false);

  /**
   * Configuration key for enabling automatic repair of implicit directories whenever detected
   * inside delete and rename calls.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.implicit.dir.repair.enable", true);

  /** Configuration key for changing the path codec from legacy to 'uri path encoding'. */
  public static final HadoopConfigurationProperty<String> PATH_CODEC =
      new HadoopConfigurationProperty<>("fs.gs.path.encoding", PATH_CODEC_USE_URI_ENCODING);

  /**
   * Configuration key for enabling automatic inference of implicit directories. If set, we create
   * and return in-memory directory objects on the fly when no backing object exists, but we know
   * there are files with the same prefix.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.implicit.dir.infer.enable", true);

  /**
   * Configuration key for enabling the use of a large flat listing to pre-populate possible glob
   * matches in a single API call before running the core globbing logic in-memory rather than
   * sequentially and recursively performing API calls.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_FLAT_GLOB_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.glob.flatlist.enable", true);

  /**
   * Configuration key for enabling the use of flat and regular glob search algorithms in two
   * parallel threads. After the first one returns result, the other one will be interrupted.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_CONCURRENT_GLOB_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.glob.concurrent.enable", true);

  /** Configuration key for marker file pattern. Default value: none */
  public static final HadoopConfigurationProperty<String> GCS_MARKER_FILE_PATTERN =
      new HadoopConfigurationProperty<>("fs.gs.marker.file.pattern");

  /** Configuration key for a max number of GCS RPCs in batch request. */
  public static final HadoopConfigurationProperty<Long> GCS_MAX_REQUESTS_PER_BATCH =
      new HadoopConfigurationProperty<>("fs.gs.max.requests.per.batch", 15L);

  /** Configuration key for a number of threads to execute batch requests. */
  public static final HadoopConfigurationProperty<Integer> GCS_BATCH_THREADS =
      new HadoopConfigurationProperty<>("fs.gs.batch.threads", 15);

  /** Configuration key for a max number of GCS RPCs in batch request for copy operations. */
  public static final HadoopConfigurationProperty<Long> GCS_COPY_MAX_REQUESTS_PER_BATCH =
      new HadoopConfigurationProperty<>("fs.gs.copy.max.requests.per.batch", 15L);

  /** Configuration key for a number of threads to execute batch requests for copy operations. */
  public static final HadoopConfigurationProperty<Integer> GCS_COPY_BATCH_THREADS =
      new HadoopConfigurationProperty<>("fs.gs.copy.batch.threads", 15);

  /**
   * Configuration key for enabling the use of Rewrite requests for copy operations. Rewrite request
   * has the same effect as Copy request, but it can handle moving large objects that may
   * potentially timeout a Copy request.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_COPY_WITH_REWRITE_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.copy.with.rewrite.enable", true);

  /**
   * Configuration key for specifying max number of bytes rewritten in a single rewrite request when
   * fs.gs.copy.with.rewrite.enable is set to 'true'.
   */
  public static final HadoopConfigurationProperty<Long> GCS_REWRITE_MAX_BYTES_PER_CALL =
      new HadoopConfigurationProperty<>("fs.gs.rewrite.max.bytes.per.call", 512 * 1024 * 1024L);

  /** Configuration key for number of items to return per call to the list* GCS RPCs. */
  public static final HadoopConfigurationProperty<Long> GCS_MAX_LIST_ITEMS_PER_CALL =
      new HadoopConfigurationProperty<>("fs.gs.list.max.items.per.call", 1024L);

  /**
   * Configuration key for the max number of retries for failed HTTP request to GCS. Note that the
   * connector will retry *up to* the number of times as specified, using a default
   * ExponentialBackOff strategy.
   *
   * <p>Also, note that this number will only control the number of retries in the low level HTTP
   * request implementation.
   */
  public static final HadoopConfigurationProperty<Integer> GCS_HTTP_MAX_RETRY =
      new HadoopConfigurationProperty<>("fs.gs.http.max.retry", 10);

  /** Configuration key for the connect timeout (in millisecond) for HTTP request to GCS. */
  public static final HadoopConfigurationProperty<Integer> GCS_HTTP_CONNECT_TIMEOUT =
      new HadoopConfigurationProperty<>("fs.gs.http.connect-timeout", 20 * 1000);

  /** Configuration key for the connect timeout (in millisecond) for HTTP request to GCS. */
  public static final HadoopConfigurationProperty<Integer> GCS_HTTP_READ_TIMEOUT =
      new HadoopConfigurationProperty<>("fs.gs.http.read-timeout", 20 * 1000);

  /**
   * Configuration key for setting a proxy for the connector to use to connect to GCS. The proxy
   * must be an HTTP proxy of the form "host:port".
   */
  public static final HadoopConfigurationProperty<String> GCS_PROXY_ADDRESS =
      HadoopCredentialConfiguration.PROXY_ADDRESS;

  /**
   * Configuration key for setting a proxy username for the connector to use to authenticate with
   * proxy used to connect to GCS.
   */
  public static final HadoopConfigurationProperty<String> GCS_PROXY_USERNAME =
      HadoopCredentialConfiguration.PROXY_USERNAME;

  /**
   * Configuration key for setting a proxy password for the connector to use to authenticate with
   * proxy used to connect to GCS.
   */
  public static final HadoopConfigurationProperty<String> GCS_PROXY_PASSWORD =
      HadoopCredentialConfiguration.PROXY_PASSWORD;

  /**
   * Configuration key for the name of HttpTransport class to use for connecting to GCS. Must be the
   * name of an HttpTransportFactory.HttpTransportType (APACHE or JAVA_NET).
   */
  public static final HadoopConfigurationProperty<HttpTransportType> GCS_HTTP_TRANSPORT =
      HadoopCredentialConfiguration.HTTP_TRANSPORT;

  /** Configuration key for adding a suffix to the GHFS application name sent to GCS. */
  public static final HadoopConfigurationProperty<String> GCS_APPLICATION_NAME_SUFFIX =
      new HadoopConfigurationProperty<>("fs.gs.application.name.suffix", "");

  /**
   * Configuration key for modifying the maximum amount of time to wait for empty object creation.
   */
  public static final HadoopConfigurationProperty<Integer> GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE =
      new HadoopConfigurationProperty<>("fs.gs.max.wait.for.empty.object.creation.ms", 3_000);

  /**
   * Configuration key for which type of output stream to use; different options may have different
   * degrees of support for advanced features like {@code hsync()} and different performance
   * characteristics. Options:
   *
   * <p>BASIC: Stream is closest analogue to direct wrapper around low-level HTTP stream into GCS.
   *
   * <p>SYNCABLE_COMPOSITE: Stream behaves similarly to BASIC when used with basic
   * create/write/close patterns, but supports hsync() by creating discrete temporary GCS objects
   * which are composed onto the destination object.
   */
  public static final HadoopConfigurationProperty<OutputStreamType> GCS_OUTPUT_STREAM_TYPE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.type", OutputStreamType.BASIC);

  /** Configuration key for setting write buffer size. */
  public static final HadoopConfigurationProperty<Integer> GCS_OUTPUT_STREAM_BUFFER_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.buffer.size", 8 * 1024 * 1024);

  /** Configuration key for setting pipe buffer size. */
  public static final HadoopConfigurationProperty<Integer> GCS_OUTPUT_STREAM_PIPE_BUFFER_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.pipe.buffer.size", 1024 * 1024);

  /** Configuration key for setting GCS upload chunk size. */
  // chunk size etc. Get the following value from GCSWC class in a better way. For now, we hard code
  // it to a known good value.
  public static final HadoopConfigurationProperty<Integer> GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.outputstream.upload.chunk.size", 64 * 1024 * 1024, "fs.gs.io.buffersize.write");

  /** Configuration key for enabling GCS direct upload. */
  public static final HadoopConfigurationProperty<Boolean> GCS_OUTPUT_STREAM_DIRECT_UPLOAD_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.outputstream.direct.upload.enable", false);

  /** Configuration key for the generation consistency read model. */
  public static final HadoopConfigurationProperty<GenerationReadConsistency>
      GCS_GENERATION_READ_CONSISTENCY =
          new HadoopConfigurationProperty<>(
              "fs.gs.generation.read.consistency",
              GoogleCloudStorageReadOptions.DEFAULT_GENERATION_READ_CONSISTENCY);

  /** Configuration key for setting read buffer size. */
  public static final HadoopConfigurationProperty<Integer> GCS_INPUT_STREAM_BUFFER_SIZE =
      new HadoopConfigurationProperty<>("fs.gs.inputstream.buffer.size", 0, "fs.gs.io.buffersize");

  /**
   * If true, on opening a file we will proactively perform a metadata GET to check whether the
   * object exists, even though the underlying channel will not open a data stream until read() is
   * actually called so that streams can seek to nonzero file positions without incurring an extra
   * stream creation. This is necessary to technically match the expected behavior of Hadoop
   * filesystems, but incurs extra latency overhead on open(). If the calling code can handle late
   * failures on not-found errors, or has independently already ensured that a file exists before
   * calling open(), then set this to false for more efficient reads.
   */
  public static final HadoopConfigurationProperty<Boolean>
      GCS_INPUT_STREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE =
          new HadoopConfigurationProperty<>(
              "fs.gs.inputstream.fast.fail.on.not.found.enable", true);

  /**
   * If true, reading a file with GZIP content encoding (HTTP header "Content-Encoding: gzip") will
   * result in failure (IOException is thrown).
   */
  public static final HadoopConfigurationProperty<Boolean>
      GCS_INPUT_STREAM_SUPPORT_GZIP_ENCODING_ENABLE =
          new HadoopConfigurationProperty<>(
              "fs.gs.inputstream.support.gzip.encoding.enable", false);

  /**
   * If forward seeks are within this many bytes of the current position, seeks are performed by
   * reading and discarding bytes in-place rather than opening a new underlying stream.
   */
  public static final HadoopConfigurationProperty<Long> GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT =
      new HadoopConfigurationProperty<>("fs.gs.inputstream.inplace.seek.limit", 8 * 1024 * 1024L);

  /** Tunes reading objects behavior to optimize HTTP GET requests for various use cases. */
  public static final HadoopConfigurationProperty<Fadvise> GCS_INPUT_STREAM_FADVISE =
      new HadoopConfigurationProperty<>("fs.gs.inputstream.fadvise", Fadvise.AUTO);

  /**
   * Minimum size in bytes of the HTTP Range header set in GCS request when opening new stream to
   * read an object.
   */
  public static final HadoopConfigurationProperty<Integer> GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE =
      new HadoopConfigurationProperty<>(
          "fs.gs.inputstream.min.range.request.size",
          GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE);

  /** Override configuration file path. This file must be a valid Hadoop configuration file. */
  public static final HadoopConfigurationProperty<String> GCS_CONFIG_OVERRIDE_FILE =
      new HadoopConfigurationProperty<>("fs.gs.config.override.file", null);

  /**
   * Configuration key for using cooperative locking to achieve a directory mutation operations
   * isolation.
   */
  public static final HadoopConfigurationProperty<Boolean> GCS_COOPERATIVE_LOCKING_ENABLE =
      new HadoopConfigurationProperty<>("fs.gs.cooperative.locking.enable", false);

  /** Configuration key for lock expiration when using cooperative locking. */
  public static final HadoopConfigurationProperty<Long>
      GCS_COOPERATIVE_LOCKING_EXPIRATION_TIMEOUT_MS =
          new HadoopConfigurationProperty<>(
              "fs.gs.cooperative.locking.expiration.timeout.ms",
              CooperativeLockingOptions.LOCK_EXPIRATION_TIMEOUT_MS_DEFAULT);

  /** Configuration key for maximum allowed concurrent operations when using cooperative locking. */
  public static final HadoopConfigurationProperty<Integer>
      GCS_COOPERATIVE_LOCKING_MAX_CONCURRENT_OPERATIONS =
          new HadoopConfigurationProperty<>(
              "fs.gs.cooperative.locking.max.concurrent.operations",
              CooperativeLockingOptions.MAX_CONCURRENT_OPERATIONS_DEFAULT);

  // TODO(b/120887495): This @VisibleForTesting annotation was being ignored by prod code.
  // Please check that removing it is correct, and remove this comment along with it.
  // @VisibleForTesting
  static GoogleCloudStorageFileSystemOptions.Builder getGcsFsOptionsBuilder(Configuration config) {
    return GoogleCloudStorageFileSystemOptions.builder()
        .setBucketDeleteEnabled(GCE_BUCKET_DELETE_ENABLE.get(config, config::getBoolean))
        .setMarkerFilePattern(GCS_MARKER_FILE_PATTERN.get(config, config::get))
        .setPerformanceCacheEnabled(GCS_PERFORMANCE_CACHE_ENABLE.get(config, config::getBoolean))
        .setCooperativeLockingEnabled(
            GCS_COOPERATIVE_LOCKING_ENABLE.get(config, config::getBoolean))
        .setPerformanceCacheOptions(getPerformanceCachingOptions(config))
        .setStatusParallelEnabled(GCS_STATUS_PARALLEL_ENABLE.get(config, config::getBoolean))
        .setCloudStorageOptions(getGcsOptionsBuilder(config).build());
  }

  @VisibleForTesting
  static GoogleCloudStorageOptions.Builder getGcsOptionsBuilder(Configuration config) {
    String projectId = GCS_PROJECT_ID.get(config, config::get);
    return GoogleCloudStorageOptions.builder()
        .setStorageRootUrl(GCS_ROOT_URL.get(config, config::get))
        .setAutoRepairImplicitDirectoriesEnabled(
            GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.get(config, config::getBoolean))
        .setInferImplicitDirectoriesEnabled(
            GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE.get(config, config::getBoolean))
        .setCopyWithRewriteEnabled(GCS_COPY_WITH_REWRITE_ENABLE.get(config, config::getBoolean))
        .setMaxBytesRewrittenPerCall(GCS_REWRITE_MAX_BYTES_PER_CALL.get(config, config::getLong))
        .setCopyMaxRequestsPerBatch(GCS_COPY_MAX_REQUESTS_PER_BATCH.get(config, config::getLong))
        .setCopyBatchThreads(GCS_COPY_BATCH_THREADS.get(config, config::getInt))
        .setTransportType(GCS_HTTP_TRANSPORT.get(config, config::getEnum))
        .setProxyAddress(GCS_PROXY_ADDRESS.get(config, config::get))
        .setProxyUsername(GCS_PROXY_USERNAME.getPassword(config))
        .setProxyPassword(GCS_PROXY_PASSWORD.getPassword(config))
        .setProjectId(projectId)
        .setMaxListItemsPerCall(GCS_MAX_LIST_ITEMS_PER_CALL.get(config, config::getLong))
        .setMaxRequestsPerBatch(GCS_MAX_REQUESTS_PER_BATCH.get(config, config::getLong))
        .setBatchThreads(GCS_BATCH_THREADS.get(config, config::getInt))
        .setMaxHttpRequestRetries(GCS_HTTP_MAX_RETRY.get(config, config::getInt))
        .setHttpRequestConnectTimeout(GCS_HTTP_CONNECT_TIMEOUT.get(config, config::getInt))
        .setHttpRequestReadTimeout(GCS_HTTP_READ_TIMEOUT.get(config, config::getInt))
        .setAppName(getApplicationName(config))
        .setMaxWaitMillisForEmptyObjectCreation(
            GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE.get(config, config::getInt))
        .setReadChannelOptions(getReadChannelOptions(config))
        .setWriteChannelOptions(getWriteChannelOptions(config))
        .setRequesterPaysOptions(getRequesterPaysOptions(config, projectId))
        .setCooperativeLockingOptions(getCooperativeLockingOptions(config));
  }

  private static PerformanceCachingGoogleCloudStorageOptions getPerformanceCachingOptions(
      Configuration config) {
    return PerformanceCachingGoogleCloudStorageOptions.builder()
        .setMaxEntryAgeMillis(
            GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS.get(config, config::getLong))
        .setListCachingEnabled(
            GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE.get(config, config::getBoolean))
        .build();
  }

  private static String getApplicationName(Configuration config) {
    String appNameSuffix = nullToEmpty(GCS_APPLICATION_NAME_SUFFIX.get(config, config::get));
    String applicationName = GoogleHadoopFileSystem.GHFS_ID + appNameSuffix;
    logger.atFinest().log("getApplicationName(config: %s): %s", config, applicationName);
    return applicationName;
  }

  private static GoogleCloudStorageReadOptions getReadChannelOptions(Configuration config) {
    return GoogleCloudStorageReadOptions.builder()
        .setFastFailOnNotFound(
            GCS_INPUT_STREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE.get(config, config::getBoolean))
        .setSupportGzipEncoding(
            GCS_INPUT_STREAM_SUPPORT_GZIP_ENCODING_ENABLE.get(config, config::getBoolean))
        .setInplaceSeekLimit(GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.get(config, config::getLong))
        .setBufferSize(GCS_INPUT_STREAM_BUFFER_SIZE.get(config, config::getInt))
        .setFadvise(GCS_INPUT_STREAM_FADVISE.get(config, config::getEnum))
        .setMinRangeRequestSize(GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.get(config, config::getInt))
        .setGenerationReadConsistency(GCS_GENERATION_READ_CONSISTENCY.get(config, config::getEnum))
        .build();
  }

  private static AsyncWriteChannelOptions getWriteChannelOptions(Configuration config) {
    return AsyncWriteChannelOptions.builder()
        .setBufferSize(GCS_OUTPUT_STREAM_BUFFER_SIZE.get(config, config::getInt))
        .setPipeBufferSize(GCS_OUTPUT_STREAM_PIPE_BUFFER_SIZE.get(config, config::getInt))
        .setUploadChunkSize(GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.get(config, config::getInt))
        .setDirectUploadEnabled(
            GCS_OUTPUT_STREAM_DIRECT_UPLOAD_ENABLE.get(config, config::getBoolean))
        .build();
  }

  private static RequesterPaysOptions getRequesterPaysOptions(
      Configuration config, String projectId) {
    String requesterPaysProjectId = GCS_REQUESTER_PAYS_PROJECT_ID.get(config, config::get);
    return RequesterPaysOptions.builder()
        .setMode(GCS_REQUESTER_PAYS_MODE.get(config, config::getEnum))
        .setProjectId(requesterPaysProjectId == null ? projectId : requesterPaysProjectId)
        .setBuckets(GCS_REQUESTER_PAYS_BUCKETS.getStringCollection(config))
        .build();
  }

  private static CooperativeLockingOptions getCooperativeLockingOptions(Configuration config) {
    return CooperativeLockingOptions.builder()
        .setLockExpirationTimeoutMilli(
            GCS_COOPERATIVE_LOCKING_EXPIRATION_TIMEOUT_MS.get(config, config::getLong))
        .setMaxConcurrentOperations(
            GCS_COOPERATIVE_LOCKING_MAX_CONCURRENT_OPERATIONS.get(config, config::getInt))
        .build();
  }
}
