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

import static com.google.cloud.hadoop.util.RequesterPaysOptions.REQUESTER_PAYS_MODE_DEFAULT;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.flogger.LazyArgs.lazy;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.GenerationReadConsistency;
import com.google.cloud.hadoop.gcsio.PathCodec;
import com.google.cloud.hadoop.gcsio.PerformanceCachingGoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.AccessTokenProviderClassFromConfigFactory;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.CredentialFromAccessTokenProviderClassFactory;
import com.google.cloud.hadoop.util.EntriesCredentialConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.cloud.hadoop.util.HadoopVersionInfo;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.PropertyUtil;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.flogger.GoogleLogger;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

/**
 * This class provides a Hadoop compatible File System on top of Google Cloud Storage (GCS).
 *
 * <p>It is implemented as a thin abstraction layer on top of GCS. The layer hides any specific
 * characteristics of the underlying store and exposes FileSystem interface understood by the Hadoop
 * engine.
 *
 * <p>Users interact with the files in the storage using fully qualified URIs. The file system
 * exposed by this class is identified using the 'gs' scheme. For example, {@code
 * gs://dir1/dir2/file1.txt}.
 *
 * <p>This implementation translates paths between hadoop Path and GCS URI with the convention that
 * the Hadoop root directly corresponds to the GCS "root", e.g. gs:/. This is convenient for many
 * reasons, such as data portability and close equivalence to gsutil paths, but imposes certain
 * inherited constraints, such as files not being allowed in root (only 'directories' can be placed
 * in root), and directory names inside root have a more limited set of allowed characters.
 *
 * <p>One of the main goals of this implementation is to maintain compatibility with behavior of
 * HDFS implementation when accessed through FileSystem interface. HDFS implementation is not very
 * consistent about the cases when it throws versus the cases when methods return false. We run GHFS
 * tests and HDFS tests against the same test data and use that as a guide to decide whether to
 * throw or to return false.
 */
public abstract class GoogleHadoopFileSystemBase extends GoogleHadoopFileSystemBaseSpecific
    implements FileSystemDescriptor {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** Default value of replication factor. */
  public static final short REPLICATION_FACTOR_DEFAULT = 3;

  /** Splitter for list values stored in a single configuration value */
  private static final Splitter CONFIGURATION_SPLITTER = Splitter.on(',');

  // -----------------------------------------------------------------
  // Configuration settings.
  // -----------------------------------------------------------------

  /**
   * Key for the permissions that we report a file or directory to have. Can either be octal or
   * symbolic mode accepted by {@link FsPermission#FsPermission(String)}
   */
  public static final String PERMISSIONS_TO_REPORT_KEY = "fs.gs.reported.permissions";

  /**
   * Default value for the permissions that we report a file or directory to have. Note: We do not
   * really support file/dir permissions but we need to report some permission value when Hadoop
   * calls getFileStatus(). A MapReduce job fails if we report permissions more relaxed than the
   * value below and this is the default File System.
   */
  public static final String PERMISSIONS_TO_REPORT_DEFAULT = "700";

  /** Configuration key for setting IO buffer size. */
  // TODO(user): rename the following to indicate that it is read buffer size.
  public static final String BUFFERSIZE_KEY = "fs.gs.io.buffersize";

  /**
   * Hadoop passes 4096 bytes as buffer size which causes poor perf. Default value of {@link
   * #BUFFERSIZE_KEY}.
   */
  public static final int BUFFERSIZE_DEFAULT = 0;

  /** Configuration key for setting write buffer size. */
  public static final String WRITE_BUFFERSIZE_KEY = "fs.gs.io.buffersize.write";

  /** Default value of {@link #WRITE_BUFFERSIZE_KEY}. */
  // chunk size etc. Get the following value from GCSWC class in a better way. For now, we hard code
  // it to a known good value.
  public static final int WRITE_BUFFERSIZE_DEFAULT = 64 * 1024 * 1024;

  /** Configuration key for default block size of a file. */
  public static final String BLOCK_SIZE_KEY = "fs.gs.block.size";

  /** Default value of {@link #BLOCK_SIZE_KEY}. */
  public static final int BLOCK_SIZE_DEFAULT = 64 * 1024 * 1024;

  /** Prefix to use for common authentication keys. */
  public static final String AUTHENTICATION_PREFIX = "fs.gs";

  /**
   * Configuration key for enabling GCE service account authentication. This key is deprecated. See
   * {@link HadoopCredentialConfiguration} for current key names.
   */
  public static final String ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY =
      "fs.gs.enable.service.account.auth";

  /**
   * Configuration key specifying the email address of the service-account with which to
   * authenticate. Only required if {@link #ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY} is true AND we're
   * using fs.gs.service.account.auth.keyfile to authenticate with a private keyfile. NB: Once GCE
   * supports setting multiple service account email addresses for metadata auth, this key will also
   * be used in the metadata auth flow. This key is deprecated. See {@link
   * HadoopCredentialConfiguration} for current key names.
   */
  public static final String SERVICE_ACCOUNT_AUTH_EMAIL_KEY = "fs.gs.service.account.auth.email";

  /**
   * Configuration key specifying local file containing a service-account private .p12 keyfile. Only
   * used if {@link #ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY} is true; if provided, the keyfile will be
   * used for service-account authentication. Otherwise, it is assumed that we are on a GCE VM with
   * metadata-authentication for service-accounts enabled, and the metadata server will be used
   * instead. Default value: none This key is deprecated. See {@link HadoopCredentialConfiguration}
   * for current key names.
   */
  public static final String SERVICE_ACCOUNT_AUTH_KEYFILE_KEY =
      "fs.gs.service.account.auth.keyfile";

  /** Configuration key for GCS project ID. Default value: none */
  public static final String GCS_PROJECT_ID_KEY = "fs.gs.project.id";

  /** Configuration key for GCS project ID. Default value: "DISABLED" */
  public static final String GCS_REQUESTER_PAYS_MODE_KEY = "fs.gs.requester.pays.mode";

  /** Configuration key for GCS Requester Pays Project ID. Default value: none */
  public static final String GCS_REQUESTER_PAYS_PROJECT_ID_KEY = "fs.gs.requester.pays.project.id";

  /** Configuration key for GCS Requester Pays Buckets. Default value: none */
  public static final String GCS_REQUESTER_PAYS_BUCKETS_KEY = "fs.gs.requester.pays.buckets";

  /**
   * Configuration key for GCS client ID. Required if {@link #ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY}
   * == false. Default value: none This key is deprecated. See {@link HadoopCredentialConfiguration}
   * for current key names.
   */
  public static final String GCS_CLIENT_ID_KEY = "fs.gs.client.id";

  /**
   * Configuration key for GCS client secret. Required if {@link
   * #ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY} == false. Default value: none This key is deprecated. See
   * HadoopCredentialConfiguration for current key names.
   */
  public static final String GCS_CLIENT_SECRET_KEY = "fs.gs.client.secret";

  /**
   * Configuration key for system bucket name. It is a fall back for the rootBucket of
   * GoogleHadoopFileSystem in gs:///path URIs . Default value: none This key is deprecated. Always
   * init the FileSystem with a bucket.
   */
  public static final String GCS_SYSTEM_BUCKET_KEY = "fs.gs.system.bucket";

  /**
   * Configuration key for flag to indicate whether system bucket should be created if it does not
   * exist. This key is deprecated. See {@link #GCS_SYSTEM_BUCKET_KEY}.
   */
  public static final String GCS_CREATE_SYSTEM_BUCKET_KEY = "fs.gs.system.bucket.create";

  /** Default value of {@link #GCS_CREATE_SYSTEM_BUCKET_KEY}. */
  public static final boolean GCS_CREATE_SYSTEM_BUCKET_DEFAULT = true;

  /** Configuration key for initial working directory of a GHFS instance. Default value: '/' */
  public static final String GCS_WORKING_DIRECTORY_KEY = "fs.gs.working.dir";

  /**
   * Configuration key for setting 250GB upper limit on file size to gain higher write throughput.
   */
  // TODO(user): remove it once blobstore supports high throughput without limiting size.
  public static final String GCS_FILE_SIZE_LIMIT_250GB = "fs.gs.file.size.limit.250gb";

  /** Default value of {@link #GCS_FILE_SIZE_LIMIT_250GB}. */
  public static final boolean GCS_FILE_SIZE_LIMIT_250GB_DEFAULT = false;

  /** Configuration key for marker file pattern. Default value: none */
  public static final String GCS_MARKER_FILE_PATTERN_KEY = "fs.gs.marker.file.pattern";

  /**
   * Configuration key for using a local item cache to supplement GCS API "getFile" results. This
   * provides faster access to recently queried data. Because the data is cached, modifications made
   * outside of this instance may not be immediately reflected. The performance cache can be used in
   * conjunction with other caching options.
   */
  public static final String GCS_ENABLE_PERFORMANCE_CACHE_KEY = "fs.gs.performance.cache.enable";

  /** Default value for {@link #GCS_ENABLE_PERFORMANCE_CACHE_KEY}. */
  public static final boolean GCS_ENABLE_PERFORMANCE_CACHE_DEFAULT = false;

  /**
   * Configuration key for maximum number of milliseconds a GoogleCloudStorageItemInfo will remain
   * "valid" in the performance cache before it's invalidated.
   */
  public static final String GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS_KEY =
      "fs.gs.performance.cache.max.entry.age.ms";

  /** Default value for {@link #GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS_KEY}. */
  public static final long GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS_DEFAULT =
      PerformanceCachingGoogleCloudStorageOptions.MAX_ENTRY_AGE_MILLIS_DEFAULT;

  /** Configuration key for whether or not to enable list caching for the performance cache. */
  public static final String GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE_KEY =
      "fs.gs.performance.cache.list.caching.enable";

  /** Default value for {@link #GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE_KEY}. */
  public static final boolean GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE_DEFAULT =
      PerformanceCachingGoogleCloudStorageOptions.LIST_CACHING_ENABLED;

  /** Configuration key for number of prefetched directory objects metadata in performance cache. */
  public static final String GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT_KEY =
      "fs.gs.performance.cache.dir.metadata.prefetch.limit";

  /** Default value for {@link #GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT_KEY}. */
  public static final long GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT_DEFAULT =
      PerformanceCachingGoogleCloudStorageOptions.DIR_METADATA_PREFETCH_LIMIT_DEFAULT;

  /**
   * Configuration key for whether or not we should update timestamps for parent directories when we
   * create new files in them.
   */
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_KEY =
      "fs.gs.parent.timestamp.update.enable";

  /** Default value for {@link #GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_KEY}. */
  public static final boolean GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_DEFAULT = true;

  /**
   * Configuration key containing a comma-separated list of sub-strings that when matched will cause
   * a particular directory to not have its modification timestamp updated. Includes take precedence
   * over excludes.
   */
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY =
      "fs.gs.parent.timestamp.update.substrings.excludes";

  /** Default value for {@link #GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY}. */
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_DEFAULT = "/";

  /** Configuration key for the MR intermediate done dir. */
  public static final String MR_JOB_HISTORY_INTERMEDIATE_DONE_DIR_KEY =
      "mapreduce.jobhistory.intermediate-done-dir";

  /** Configuration key of the MR done directory. */
  public static final String MR_JOB_HISTORY_DONE_DIR_KEY = "mapreduce.jobhistory.done-dir";

  /**
   * Configuration key containing a comma-separated list of sub-strings that when matched will cause
   * a particular directory to have its modification timestamp updated. Includes take precedence
   * over excludes.
   */
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY =
      "fs.gs.parent.timestamp.update.substrings.includes";

  /** Default value for {@link #GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY}. */
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_DEFAULT =
      String.format(
          "${%s},${%s}", MR_JOB_HISTORY_INTERMEDIATE_DONE_DIR_KEY, MR_JOB_HISTORY_DONE_DIR_KEY);

  /**
   * Configuration key for enabling automatic repair of implicit directories whenever detected
   * inside listStatus and globStatus calls, or other methods which may indirectly call listStatus
   * and/or globaStatus.
   */
  public static final String GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY =
      "fs.gs.implicit.dir.repair.enable";

  /** Default value for {@link #GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY}. */
  public static final boolean GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT = true;

  /** Configuration key for changing the path codec from legacy to 'uri path encoding'. */
  public static final String PATH_CODEC_KEY = "fs.gs.path.encoding";

  /** Use new URI_ENCODED_PATH_CODEC. */
  public static final String PATH_CODEC_USE_URI_ENCODING = "uri-path";

  /** Use LEGACY_PATH_CODEC. */
  public static final String PATH_CODEC_USE_LEGACY_ENCODING = "legacy";

  /** Use the default path codec. */
  public static final String PATH_CODEC_DEFAULT = PATH_CODEC_USE_LEGACY_ENCODING;

  /**
   * Instance value of {@link #GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY} based on the initial
   * Configuration.
   */
  private boolean enableAutoRepairImplicitDirectories =
      GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT;

  /**
   * Configuration key for enabling automatic inference of implicit directories. If set, we create
   * and return in-memory directory objects on the fly when no backing object exists, but we know
   * there are files with the same prefix. The ENABLE_REPAIR flag takes precedence over this flag:
   * if both are set, the repair is attempted, and only if it fails does the setting of this flag
   * kick in.
   */
  public static final String GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_KEY =
      "fs.gs.implicit.dir.infer.enable";

  /** Default value for {@link #GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_KEY}. */
  public static final boolean GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_DEFAULT = true;

  /**
   * Instance value of {@link #GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_KEY} based on the initial
   * Configuration.
   */
  private boolean enableInferImplicitDirectories = GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_DEFAULT;

  /**
   * Configuration key for enabling the use of a large flat listing to pre-populate possible glob
   * matches in a single API call before running the core globbing logic in-memory rather than
   * sequentially and recursively performing API calls.
   */
  public static final String GCS_ENABLE_FLAT_GLOB_KEY = "fs.gs.glob.flatlist.enable";

  /** Default value for {@link #GCS_ENABLE_FLAT_GLOB_KEY}. */
  public static final boolean GCS_ENABLE_FLAT_GLOB_DEFAULT = true;

  /**
   * Configuration key for enabling the use of marker files during file creation. When running
   * non-MR applications that make use of the FileSystem, it is a good idea to enable marker files
   * to better mimic HDFS overwrite and locking behavior.
   */
  public static final String GCS_ENABLE_MARKER_FILE_CREATION_KEY =
      "fs.gs.create.marker.files.enable";

  /** Default value for {@link #GCS_ENABLE_MARKER_FILE_CREATION_KEY}. */
  public static final boolean GCS_ENABLE_MARKER_FILE_CREATION_DEFAULT = false;

  /**
   * Configuration key for enabling the use of Rewrite requests for copy operations. Rewrite request
   * has the same effect as Copy request, but it can handle moving large objects that may
   * potentially timeout a Copy request.
   */
  public static final String GCS_ENABLE_COPY_WITH_REWRITE_KEY = "fs.gs.copy.with.rewrite.enable";

  /** Default value for {@link #GCS_ENABLE_COPY_WITH_REWRITE_KEY}. */
  public static final boolean GCS_ENABLE_COPY_WITH_REWRITE_DEFAULT = true;

  /** Configuration key for a max number of GCS RPCs in batch request for copy operations. */
  public static final String GCS_COPY_MAX_REQUESTS_PER_BATCH = "fs.gs.copy.max.requests.per.batch";

  /** Default value for {@link #GCS_COPY_MAX_REQUESTS_PER_BATCH}. */
  public static final long GCS_COPY_MAX_REQUESTS_PER_BATCH_DEFAULT = 15;

  /** Configuration key for a number of threads to execute batch requests for copy operations. */
  public static final String GCS_COPY_BATCH_THREADS = "fs.gs.copy.batch.threads";

  /** Default value for {@link #GCS_COPY_BATCH_THREADS}. */
  public static final int GCS_COPY_BATCH_THREADS_DEFAULT = 15;

  /** Configuration key for number of items to return per call to the list* GCS RPCs. */
  public static final String GCS_MAX_LIST_ITEMS_PER_CALL = "fs.gs.list.max.items.per.call";

  /** Default value for {@link #GCS_MAX_LIST_ITEMS_PER_CALL}. */
  public static final long GCS_MAX_LIST_ITEMS_PER_CALL_DEFAULT = 1024;

  /** Configuration key for a max number of GCS RPCs in batch request. */
  public static final String GCS_MAX_REQUESTS_PER_BATCH = "fs.gs.max.requests.per.batch";

  /** Default value for {@link #GCS_MAX_REQUESTS_PER_BATCH}. */
  public static final long GCS_MAX_REQUESTS_PER_BATCH_DEFAULT = 15;

  /** Configuration key for a number of threads to execute batch requests. */
  public static final String GCS_BATCH_THREADS = "fs.gs.batch.threads";

  /** Default value for {@link #GCS_BATCH_THREADS}. */
  public static final int GCS_BATCH_THREADS_DEFAULT = 15;

  /**
   * Configuration key for the max number of retries for failed HTTP request to GCS. Note that the
   * connector will retry *up to* the number of times as specified, using a default
   * ExponentialBackOff strategy.
   *
   * <p>Also, note that this number will only control the number of retries in the low level HTTP
   * request implementation.
   */
  public static final String GCS_HTTP_MAX_RETRY_KEY = "fs.gs.http.max.retry";

  /** Default value for {@link #GCS_HTTP_MAX_RETRY_KEY}. */
  public static final int GCS_HTTP_MAX_RETRY_DEFAULT = 10;

  /** Configuration key for the connect timeout (in millisecond) for HTTP request to GCS. */
  public static final String GCS_HTTP_CONNECT_TIMEOUT_KEY = "fs.gs.http.connect-timeout";

  /** Default value for {@link #GCS_HTTP_CONNECT_TIMEOUT_KEY}. */
  public static final int GCS_HTTP_CONNECT_TIMEOUT_DEFAULT = 20 * 1000;

  /** Configuration key for the connect timeout (in millisecond) for HTTP request to GCS. */
  public static final String GCS_HTTP_READ_TIMEOUT_KEY = "fs.gs.http.read-timeout";

  /** Default value for {@link #GCS_HTTP_READ_TIMEOUT_KEY}. */
  public static final int GCS_HTTP_READ_TIMEOUT_DEFAULT = 20 * 1000;

  /**
   * Configuration key for setting a proxy for the connector to use to connect to GCS. The proxy
   * must be an HTTP proxy of the form "host:port".
   */
  public static final String GCS_PROXY_ADDRESS_KEY =
      EntriesCredentialConfiguration.PROXY_ADDRESS_KEY;

  /** Default to no proxy. */
  public static final String GCS_PROXY_ADDRESS_DEFAULT =
      EntriesCredentialConfiguration.PROXY_ADDRESS_DEFAULT;

  /**
   * Configuration key for the name of HttpTransport class to use for connecting to GCS. Must be the
   * name of an HttpTransportFactory.HttpTransportType (APACHE or JAVA_NET).
   */
  public static final String GCS_HTTP_TRANSPORT_KEY =
      EntriesCredentialConfiguration.HTTP_TRANSPORT_KEY;

  /** Default to the default specified in HttpTransportFactory. */
  public static final String GCS_HTTP_TRANSPORT_DEFAULT =
      EntriesCredentialConfiguration.HTTP_TRANSPORT_DEFAULT;

  /** Configuration key for adding a suffix to the GHFS application name sent to GCS. */
  public static final String GCS_APPLICATION_NAME_SUFFIX_KEY = "fs.gs.application.name.suffix";

  /** Default suffix to add to the application name. */
  public static final String GCS_APPLICATION_NAME_SUFFIX_DEFAULT = "";

  /**
   * Configuration key for modifying the maximum amount of time to wait for empty object creation.
   */
  public static final String GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE_KEY =
      "fs.gs.max.wait.for.empty.object.creation.ms";

  /** Default to 3 seconds. */
  public static final int GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE_DEFAULT = 3_000;

  /**
   * Configuration key for which type of output stream to use; different options may have different
   * degrees of support for advanced features like hsync() and different performance
   * characteristics. Options:
   *
   * <p>BASIC: Stream is closest analogue to direct wrapper around low-level HTTP stream into GCS.
   *
   * <p>SYNCABLE_COMPOSITE: Stream behaves similarly to BASIC when used with basic
   * create/write/close patterns, but supports hsync() by creating discrete temporary GCS objects
   * which are composed onto the destination object. Has a hard upper limit of number of components
   * which can be composed onto the destination object.
   */
  public static final String GCS_OUTPUTSTREAM_TYPE_KEY = "fs.gs.outputstream.type";

  /** Default value for {@link #GCS_OUTPUTSTREAM_TYPE_KEY}. */
  public static final String GCS_OUTPUTSTREAM_TYPE_DEFAULT = "BASIC";

  /** Available types for use with {@link #GCS_OUTPUTSTREAM_TYPE_KEY}. */
  public enum OutputStreamType {
    BASIC,
    SYNCABLE_COMPOSITE
  }

  /** Configuration key for the generation consistency read model. */
  public static final String GCS_GENERATION_READ_CONSISTENCY_KEY =
      "fs.gs.generation.read.consistency";

  /** Default value for {@link #GCS_GENERATION_READ_CONSISTENCY_KEY}. */
  public static final GenerationReadConsistency GCS_GENERATION_READ_CONSISTENCY_DEFAULT =
      GoogleCloudStorageReadOptions.DEFAULT_GENERATION_READ_CONSISTENCY;

  /**
   * If true, on opening a file we will proactively perform a metadata GET to check whether the
   * object exists, even though the underlying channel will not open a data stream until read() is
   * actually called so that streams can seek to nonzero file positions without incurring an extra
   * stream creation. This is necessary to technically match the expected behavior of Hadoop
   * filesystems, but incurs extra latency overhead on open(). If the calling code can handle late
   * failures on not-found errors, or has independently already ensured that a file exists before
   * calling open(), then set this to false for more efficient reads.
   */
  public static final String GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_KEY =
      "fs.gs.inputstream.fast.fail.on.not.found.enable";

  /** Default value for {@link #GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_KEY}. */
  public static final boolean GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_DEFAULT = true;

  /**
   * If forward seeks are within this many bytes of the current position, seeks are performed by
   * reading and discarding bytes in-place rather than opening a new underlying stream.
   */
  public static final String GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_KEY =
      "fs.gs.inputstream.inplace.seek.limit";

  /** Default value for {@link #GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_KEY}. */
  public static final long GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_DEFAULT = 8 * 1024 * 1024L;

  /** Tunes reading objects behavior to optimize HTTP GET requests for various use cases. */
  public static final String GCS_INPUTSTREAM_FADVISE_KEY = "fs.gs.inputstream.fadvise";

  /** Default value for {@link #GCS_INPUTSTREAM_FADVISE_KEY}. */
  public static final Fadvise GCS_INPUTSTREAM_FADVISE_DEFAULT =
      GoogleCloudStorageReadOptions.DEFAULT_FADVISE;

  /**
   * Minimum size in bytes of the HTTP Range header set in GCS request when opening new stream to
   * read an object.
   */
  public static final String GCS_INPUTSTREAM_MIN_RANGE_REQUEST_SIZE_KEY =
      "fs.gs.inputstream.min.range.request.size";

  /** Default value for {@link #GCS_INPUTSTREAM_MIN_RANGE_REQUEST_SIZE_KEY}. */
  public static final int GCS_INPUTSTREAM_MIN_RANGE_REQUEST_SIZE_DEFAULT =
      GoogleCloudStorageReadOptions.DEFAULT_MIN_RANGE_REQUEST_SIZE;

  /**
   * If true, recursive delete on a path that refers to a GCS bucket itself ('/' for any
   * bucket-rooted GoogleHadoopFileSystem) or delete on that path when it's empty will result in
   * fully deleting the GCS bucket. If false, any operation that normally would have deleted the
   * bucket will be ignored instead. Setting to 'false' preserves the typical behavior of "rm -rf /"
   * which translates to deleting everything inside of root, but without clobbering the filesystem
   * authority corresponding to that root path in the process.
   */
  public static final String GCE_BUCKET_DELETE_ENABLE_KEY = "fs.gs.bucket.delete.enable";

  /** Default value for {@link #GCE_BUCKET_DELETE_ENABLE_KEY}. */
  public static final boolean GCE_BUCKET_DELETE_ENABLE_DEFAULT = false;

  /** Default PathFilter that accepts all paths. */
  public static final PathFilter DEFAULT_FILTER = path -> true;

  /** A resource file containing GCS related build properties. */
  public static final String PROPERTIES_FILE = "gcs.properties";

  /** The key in the PROPERTIES_FILE that contains the version built. */
  public static final String VERSION_PROPERTY = "gcs.connector.version";

  /** The version returned when one cannot be found in properties. */
  public static final String UNKNOWN_VERSION = "0.0.0";

  /** Current version. */
  public static final String VERSION;

  /** Identifies this version of the GoogleHadoopFileSystemBase library. */
  public static final String GHFS_ID;

  static {
    VERSION =
        PropertyUtil.getPropertyOrDefault(
            GoogleHadoopFileSystemBase.class, PROPERTIES_FILE, VERSION_PROPERTY, UNKNOWN_VERSION);
    logger.atInfo().log("GHFS version: %s", VERSION);
    GHFS_ID = String.format("GHFS/%s", VERSION);
  }

  /** Instance value of {@link #GCS_ENABLE_FLAT_GLOB_KEY} based on the initial Configuration. */
  private boolean enableFlatGlob = GCS_ENABLE_FLAT_GLOB_DEFAULT;

  /** The URI the File System is passed in initialize. */
  protected URI initUri;

  /**
   * The retrieved configuration value for {@link #GCS_SYSTEM_BUCKET_KEY}. Used as a fallback for a
   * root bucket, when required.
   */
  @Deprecated protected String systemBucket;

  /** Underlying GCS file system object. */
  protected GoogleCloudStorageFileSystem gcsfs;

  /**
   * Current working directory; overridden in initialize() if {@link #GCS_WORKING_DIRECTORY_KEY} is
   * set.
   */
  private Path workingDirectory;

  /**
   * Default block size. Note that this is the size that is reported to Hadoop FS clients. It does
   * not modify the actual block size of an underlying GCS object, because GCS JSON API does not
   * allow modifying or querying the value. Modifying this value allows one to control how many
   * mappers are used to process a given file.
   */
  protected long defaultBlockSize = BLOCK_SIZE_DEFAULT;

  /** The fixed reported permission of all files. */
  private FsPermission reportedPermissions;

  /** Map of counter values */
  protected final ImmutableMap<Counter, AtomicLong> counters = createCounterMap();

  protected ImmutableMap<Counter, AtomicLong> createCounterMap() {
    EnumMap<Counter, AtomicLong> countersMap = new EnumMap<>(Counter.class);
    for (Counter counter : ALL_COUNTERS) {
      countersMap.put(counter, new AtomicLong());
    }
    return Maps.immutableEnumMap(countersMap);
  }

  /**
   * Behavior of listStatus when a path is not found.
   */
  protected enum ListStatusFileNotFoundBehavior {
    Hadoop1 {
      @Override
      public FileStatus[] handle(String path) throws IOException {
        return null;
      }
    },
    Hadoop2 {
      @Override
      public FileStatus[] handle(String path) throws IOException {
        throw new FileNotFoundException(String.format("Path '%s' does not exist.", path));
      }
    };

    /**
     * Perform version specific handling for a missing path.
     * @param path The missing path
     */
    public abstract FileStatus[] handle(String path) throws IOException;

    /**
     * Get the ListStatusFileNotFoundBehavior for the currently running Hadoop version.
     */
    public static ListStatusFileNotFoundBehavior get() {
      return get(HadoopVersionInfo.getInstance());
    }

    /**
     * Get the ListStatusFileNotFoundBehavior for the given hadoop version/
     * @param hadoopVersionInfo The hadoop version.
     */
    public static ListStatusFileNotFoundBehavior get(HadoopVersionInfo hadoopVersionInfo) {
      if (hadoopVersionInfo.isGreaterThan(2, 0)
          || hadoopVersionInfo.isEqualTo(2, 0)
          || hadoopVersionInfo.isEqualTo(0, 23)) {
        return Hadoop2;
      }
      return Hadoop1;
    }
  }

  // Behavior when a path is not found in listStatus()
  protected ListStatusFileNotFoundBehavior listStatusFileNotFoundBehavior =
      ListStatusFileNotFoundBehavior.get();

  @VisibleForTesting
  protected void setListStatusFileNotFoundBehavior(ListStatusFileNotFoundBehavior behavior) {
    this.listStatusFileNotFoundBehavior = behavior;
  }


  /**
   * Defines names of counters we track for each operation.
   *
   * There are two types of counters:
   * -- METHOD_NAME      : Number of successful invocations of method METHOD.
   * -- METHOD_NAME_TIME : Total inclusive time spent in method METHOD.
   */
  public enum Counter {
    APPEND,
    APPEND_TIME,
    CREATE,
    CREATE_TIME,
    DELETE,
    DELETE_TIME,
    GET_FILE_STATUS,
    GET_FILE_STATUS_TIME,
    INIT,
    INIT_TIME,
    INPUT_STREAM,
    INPUT_STREAM_TIME,
    LIST_STATUS,
    LIST_STATUS_TIME,
    MKDIRS,
    MKDIRS_TIME,
    OPEN,
    OPEN_TIME,
    OUTPUT_STREAM,
    OUTPUT_STREAM_TIME,
    READ1,
    READ1_TIME,
    READ,
    READ_TIME,
    READ_FROM_CHANNEL,
    READ_FROM_CHANNEL_TIME,
    READ_CLOSE,
    READ_CLOSE_TIME,
    READ_POS,
    READ_POS_TIME,
    RENAME,
    RENAME_TIME,
    SEEK,
    SEEK_TIME,
    SET_WD,
    SET_WD_TIME,
    WRITE1,
    WRITE1_TIME,
    WRITE,
    WRITE_TIME,
    WRITE_CLOSE,
    WRITE_CLOSE_TIME,
  }

  /**
   * Set of all counters.
   *
   * <p>It is used for performance optimization instead of `Counter.values`, because
   * `Counter.values` returns new array on each invocation.
   */
  private static final ImmutableSet<Counter> ALL_COUNTERS =
      Sets.immutableEnumSet(EnumSet.allOf(Counter.class));

  /**
   * A predicate that processes individual directory paths and evaluates the conditions set in
   * fs.gs.parent.timestamp.update.enable, fs.gs.parent.timestamp.update.substrings.include and
   * fs.gs.parent.timestamp.update.substrings.exclude to determine if a path should be ignored
   * when running directory timestamp updates. If no match is found in either include or
   * exclude and updates are enabled, the directory timestamp will be updated.
   */
  public static class ParentTimestampUpdateIncludePredicate
      implements GoogleCloudStorageFileSystemOptions.TimestampUpdatePredicate {

    /**
     * Create a new ParentTimestampUpdateIncludePredicate from the passed Hadoop configuration
     * object.
     */
    public static ParentTimestampUpdateIncludePredicate create(Configuration config) {
      boolean enableDirectoryTimestampUpdating = config.getBoolean(
          GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_KEY,
          GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_DEFAULT);
      logger.atFine().log(
          "%s = %s", GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_KEY, enableDirectoryTimestampUpdating);

      String includedParentPaths = config.get(
          GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY,
          GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_DEFAULT);
      logger.atFine().log("%s = %s", GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY, includedParentPaths);
      List<String> splitIncludedParentPaths =
          CONFIGURATION_SPLITTER.splitToList(includedParentPaths);

      String excludedParentPaths = config.get(
          GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY,
          GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_DEFAULT);
      logger.atFine().log("%s = %s", GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY, excludedParentPaths);
      List<String> splitExcludedParentPaths =
          CONFIGURATION_SPLITTER.splitToList(excludedParentPaths);

      return new ParentTimestampUpdateIncludePredicate(
          enableDirectoryTimestampUpdating,
          splitIncludedParentPaths,
          splitExcludedParentPaths);
    }

    // Include and exclude lists are intended to be small N and checked relatively
    // infrequently. If that becomes not that case, consider Aho-Corasick or similar matching
    // algorithms.
    private final List<String> includeSubstrings;
    private final List<String> excludeSubstrings;
    private final boolean enableTimestampUpdates;

    public ParentTimestampUpdateIncludePredicate(
        boolean enableTimestampUpdates,
        List<String> includeSubstrings,
        List<String> excludeSubstrings) {
      this.includeSubstrings = includeSubstrings;
      this.excludeSubstrings = excludeSubstrings;
      this.enableTimestampUpdates = enableTimestampUpdates;
    }

    /**
     * Determine if updating directory timestamps should be ignored.
     * @return True if the directory timestamp should not be updated. False to indicate it should
     * be updated.
     */
    @Override
    public boolean shouldUpdateTimestamp(URI uri) {
      if (!enableTimestampUpdates) {
        logger.atFine().log("Timestamp updating disabled. Not updating uri %s", uri);
        return false;
      }

      for (String include : includeSubstrings) {
        if (uri.toString().contains(include)) {
          logger.atFine().log(
              "Path %s matched included path %s. Updating timestamps.", uri, include);
          return true;
        }
      }

      for (String exclude : excludeSubstrings) {
        if (uri.toString().contains(exclude)) {
          logger.atFine().log(
              "Path %s matched excluded path %s. Not updating timestamps.", uri, exclude);
          return false;
        }
      }

      return true;
    }
  }

  /**
   * Constructs an instance of GoogleHadoopFileSystemBase; the internal
   * GoogleCloudStorageFileSystem will be set up with config settings when initialize() is called.
   */
  public GoogleHadoopFileSystemBase() {
  }

  /**
   * Constructs an instance of GoogleHadoopFileSystemBase using the provided
   * GoogleCloudStorageFileSystem; initialize() will not re-initialize it.
   */
  public GoogleHadoopFileSystemBase(GoogleCloudStorageFileSystem gcsfs) {
    Preconditions.checkArgument(gcsfs != null, "gcsfs must not be null");
    this.gcsfs = gcsfs;
  }

  /**
   * Returns an unqualified path without any leading slash, relative to the filesystem root,
   * which serves as the home directory of the current user; see {@code getHomeDirectory} for
   * a description of what the home directory means.
   */
  protected abstract String getHomeDirectorySubpath();

  /**
   * Gets Hadoop path corresponding to the given GCS path.
   *
   * @param gcsPath Fully-qualified GCS path, of the form gs://<bucket>/<object>.
   */
  public abstract Path getHadoopPath(URI gcsPath);

  /**
   * Gets GCS path corresponding to the given Hadoop path, which can be relative or absolute,
   * and can have either gs://<path> or gs:/<path> forms.
   *
   * @param hadoopPath Hadoop path.
   */
  public abstract URI getGcsPath(Path hadoopPath);

  /**
   * Gets the default value of working directory.
   */
  public abstract Path getDefaultWorkingDirectory();

  // =================================================================
  // Methods implementing FileSystemDescriptor interface; these define the way
  // paths are translated between Hadoop and GCS.
  // =================================================================

  @Override
  public abstract Path getFileSystemRoot();

  @Override
  public abstract String getScheme();

  @Deprecated
  @Override
  public String getHadoopScheme() {
    return getScheme();
  }

  /**
   *
   * <p> Overridden to make root it's own parent. This is POSIX compliant, but more importantly
   * guards against poor directory accounting in the PathData class of Hadoop 2's FsShell.
   */
  @Override
  public Path makeQualified(Path path) {
    logger.atFine().log("GHFS.makeQualified: path: %s", path);
    Path qualifiedPath = super.makeQualified(path);

    URI uri = qualifiedPath.toUri();

    Preconditions.checkState(
        "".equals(uri.getPath()) || qualifiedPath.isAbsolute(),
        "Path '%s' must be fully qualified.",
        qualifiedPath);

    // Strip initial '..'s to make root is its own parent.
    StringBuilder sb = new StringBuilder(uri.getPath());
    while (sb.indexOf("/../") == 0) {
      // Leave a preceding slash, so path is still absolute.
      sb.delete(0, 3);
    }

    String strippedPath = sb.toString();

    // Allow a Path of gs://someBucket to map to gs://someBucket/
    if (strippedPath.equals("/..") || strippedPath.equals("")) {
      strippedPath = "/";
    }

    Path result = new Path(uri.getScheme(), uri.getAuthority(), strippedPath);
    logger.atFine().log("GHFS.makeQualified:=> %s", result);
    return result;
  }

  @Override
  protected void checkPath(Path path) {
    URI uri = path.toUri();
    String scheme = uri.getScheme();
    // Only check that the scheme matches. The authority and path will be
    // validated later.
    if (scheme == null || scheme.equalsIgnoreCase(getScheme())) {
      return;
    }
    String msg = String.format(
        "Wrong FS scheme: %s, in path: %s, expected scheme: %s",
        scheme, path, getScheme());
    throw new IllegalArgumentException(msg);
  }

  /**
   * See {@link #initialize(URI, Configuration, boolean)} for details; calls with third arg
   * defaulting to 'true' for initializing the superclass.
   *
   * @param path URI of a file/directory within this file system.
   * @param config Hadoop configuration.
   */
  @Override
  public void initialize(URI path, Configuration config)
      throws IOException {
    // initSuperclass == true.
    initialize(path, config, true);
  }

  /**
   * Initializes this file system instance.
   *
   * Note:
   * The path passed to this method could be path of any file/directory.
   * It does not matter because the only thing we check is whether
   * it uses 'gs' scheme. The rest is ignored.
   *
   * @param path URI of a file/directory within this file system.
   * @param config Hadoop configuration.
   * @param initSuperclass if false, doesn't call super.initialize(path, config); avoids
   *     registering a global Statistics object for this instance.
   */
  public void initialize(URI path, Configuration config, boolean initSuperclass)
      throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(path != null, "path must not be null");
    Preconditions.checkArgument(config != null, "config must not be null");
    Preconditions.checkArgument(path.getScheme() != null, "scheme of path must not be null");
    if (!path.getScheme().equals(getScheme())) {
      throw new IllegalArgumentException("URI scheme not supported: " + path);
    }
    initUri = path;
    logger.atFine().log("GHFS.initialize: %s", path);

    if (initSuperclass) {
      super.initialize(path, config);
    } else {
      logger.atFine().log(
          "Initializing 'statistics' as an instance not attached to the static FileSystem map");
      // Provide an ephemeral Statistics object to avoid NPE, but still avoid registering a global
      // statistics object.
      statistics = new Statistics(getScheme());
    }
    configure(config);

    long duration = System.nanoTime() - startTime;
    increment(Counter.INIT);
    increment(Counter.INIT_TIME, duration);
  }

  /**
   * Returns a URI of the root of this FileSystem.
   */
  @Override
  public URI getUri() {
    return getFileSystemRoot().toUri();
  }

  /**
   * The default port is listed as -1 as an indication that ports are not used.
   */
  @Override
  protected int getDefaultPort() {
    logger.atFine().log("GHFS.getDefaultPort:");
    int result = -1;
    logger.atFine().log("GHFS.getDefaultPort:=> %s", result);
    return result;
  }

  // TODO(user): Improve conversion of exceptions to 'false'.
  // Hadoop is inconsistent about when methods are expected to throw
  // and when they should return false. The FileSystem documentation
  // is unclear on this and many other aspects. For now, we convert
  // all IOExceptions to false which is not the right thing to do.
  // We need to find a way to only convert known cases to 'false'
  // and let the other exceptions bubble up.

  /**
   * Opens the given file for reading.
   *
   * <p>Note: This function overrides the given bufferSize value with a higher number unless further
   * overridden using configuration parameter fs.gs.io.buffersize.
   *
   * @param hadoopPath File to open.
   * @param bufferSize Size of buffer to use for IO.
   * @return A readable stream.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if an error occurs.
   */
  @Override
  public FSDataInputStream open(Path hadoopPath, int bufferSize) throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    logger.atFine().log("GHFS.open: %s, bufferSize: %d (ignored)", hadoopPath, bufferSize);
    URI gcsPath = getGcsPath(hadoopPath);
    GoogleCloudStorageReadOptions readChannelOptions =
        getGcsFs().getOptions().getCloudStorageOptions().getReadChannelOptions();
    GoogleHadoopFSInputStream in =
        new GoogleHadoopFSInputStream(this, gcsPath, readChannelOptions, statistics);

    long duration = System.nanoTime() - startTime;
    increment(Counter.OPEN);
    increment(Counter.OPEN_TIME, duration);
    return new FSDataInputStream(in);
  }

  /**
   * Opens the given file for writing.
   *
   * @param hadoopPath The file to open.
   * @param permission Permissions to set on the new file. Ignored.
   * @param overwrite If a file with this name already exists, then if true,
   *        the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize The size of the buffer to use.
   * @param replication Required block replication for the file. Ignored.
   * @param blockSize The block-size to be used for the new file. Ignored.
   * @param progress Progress is reported through this. Ignored.
   * @return A writable stream.
   * @throws IOException if an error occurs.
   * @see #setPermission(Path, FsPermission)
   */
  @Override
  public FSDataOutputStream create(
      Path hadoopPath,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");
    Preconditions.checkArgument(
        replication > 0, "replication must be a positive integer: %s", replication);
    Preconditions.checkArgument(
        blockSize > 0, "blockSize must be a positive integer: %s", blockSize);

    checkOpen();

    logger.atFine().log(
        "GHFS.create: %s, overwrite: %s, bufferSize: %d (ignored)",
        hadoopPath, overwrite, bufferSize);

    URI gcsPath = getGcsPath(hadoopPath);

    OutputStreamType type = OutputStreamType.valueOf(
        getConf().get(GCS_OUTPUTSTREAM_TYPE_KEY, GCS_OUTPUTSTREAM_TYPE_DEFAULT));
    OutputStream out;
    switch (type) {
      case BASIC:
        out =
            new GoogleHadoopOutputStream(
                this, gcsPath, statistics, new CreateFileOptions(overwrite));
        break;
      case SYNCABLE_COMPOSITE:
        out =
            new GoogleHadoopSyncableOutputStream(
                this, gcsPath, statistics, new CreateFileOptions(overwrite));
        break;
      default:
        throw new IOException(String.format(
            "Unsupported output stream type given for key '%s': '%s'",
            GCS_OUTPUTSTREAM_TYPE_KEY, type));
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.CREATE);
    increment(Counter.CREATE_TIME, duration);
    return new FSDataOutputStream(out, null);
  }

  /**
   * Appends to an existing file (optional operation). Not supported.
   *
   * @param hadoopPath The existing file to be appended.
   * @param bufferSize The size of the buffer to be used.
   * @param progress For reporting progress if it is not null.
   * @return A writable stream.
   * @throws IOException if an error occurs.
   */
  @Override
  public FSDataOutputStream append(Path hadoopPath, int bufferSize, Progressable progress)
      throws IOException {

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    logger.atFine().log("GHFS.append: %s, bufferSize: %d (ignored)", hadoopPath, bufferSize);

    long duration = System.nanoTime() - startTime;
    increment(Counter.APPEND);
    increment(Counter.APPEND_TIME, duration);
    throw new IOException("The append operation is not supported.");
  }

  /**
   * Renames src to dst. Src must not be equal to the filesystem root.
   *
   * @param src Source path.
   * @param dst Destination path.
   * @return true if rename succeeds.
   * @throws FileNotFoundException if src does not exist.
   * @throws IOException if an error occurs.
   */
  @Override
  public boolean rename(Path src, Path dst)
      throws IOException {
    // Even though the underlying GCSFS will also throw an IAE if src is root, since our filesystem
    // root happens to equal the global root, we want to explicitly check it here since derived
    // classes may not have filesystem roots equal to the global root.
    if (src.makeQualified(this).equals(getFileSystemRoot())) {
      logger.atFine().log("GHFS.rename: src is root: '%s'", src);
      return false;
    }

    long startTime = System.nanoTime();
    Preconditions.checkArgument(src != null, "src must not be null");
    Preconditions.checkArgument(dst != null, "dst must not be null");

    checkOpen();

    try {
      logger.atFine().log("GHFS.rename: %s -> %s", src, dst);

      URI srcPath = getGcsPath(src);
      URI dstPath = getGcsPath(dst);
      gcsfs.rename(srcPath, dstPath);
    } catch (IOException e) {
      logger.atFine().withCause(e).log("GHFS.rename");
      return false;
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.RENAME);
    increment(Counter.RENAME_TIME, duration);
    return true;
  }

  /**
   * Delete a file.
   * @deprecated Use {@code delete(Path, boolean)} instead
   */
  @Deprecated
  @Override
  public boolean delete(Path f)
      throws IOException {

    return delete(f, true);
  }

  /**
   * Deletes the given file or directory.
   *
   * @param hadoopPath The path to delete.
   * @param recursive If path is a directory and set to
   * true, the directory is deleted, else throws an exception.
   * In case of a file, the recursive parameter is ignored.
   * @return  true if delete is successful else false.
   * @throws IOException if an error occurs.
   */
  @Override
  public boolean delete(Path hadoopPath, boolean recursive)
      throws IOException {

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    logger.atFine().log("GHFS.delete: %s, recursive: %s", hadoopPath, recursive);
    URI gcsPath = getGcsPath(hadoopPath);
    try {
      gcsfs.delete(gcsPath, recursive);
    } catch (DirectoryNotEmptyException e) {
      throw e;
    } catch (IOException e) {
      logger.atFine().withCause(e).log("GHFS.delete");
      return false;
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.DELETE);
    increment(Counter.DELETE_TIME, duration);
    return true;
  }

  /**
   * Lists file status. If the given path points to a directory then the status
   * of children is returned, otherwise the status of the given file is returned.
   *
   * @param hadoopPath Given path.
   * @return File status list or null if path does not exist.
   * @throws IOException if an error occurs.
   */
  @Override
  public FileStatus[] listStatus(Path hadoopPath)
      throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    logger.atFine().log("GHFS.listStatus: %s", hadoopPath);

    URI gcsPath = getGcsPath(hadoopPath);
    List<FileStatus> status;

    try {
      List<FileInfo> fileInfos = gcsfs.listFileInfo(
          gcsPath, enableAutoRepairImplicitDirectories);
      status = new ArrayList<>(fileInfos.size());
      String userName = getUgiUserName();
      for (FileInfo fileInfo : fileInfos) {
        status.add(getFileStatus(fileInfo, userName));
      }
    } catch (FileNotFoundException fnfe) {
      logger.atFine().withCause(fnfe).log("Got fnfe: ");
      return listStatusFileNotFoundBehavior.handle(gcsPath.toString());
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.LIST_STATUS);
    increment(Counter.LIST_STATUS_TIME, duration);
    return status.toArray(new FileStatus[0]);
  }

  /**
   * Sets the current working directory to the given path.
   *
   * @param hadoopPath New working directory.
   */
  @Override
  public void setWorkingDirectory(Path hadoopPath) {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    logger.atFine().log("GHFS.setWorkingDirectory: %s", hadoopPath);
    URI gcsPath = getGcsPath(hadoopPath);
    gcsPath = FileInfo.convertToDirectoryPath(gcsfs.getPathCodec(), gcsPath);
    Path newPath = getHadoopPath(gcsPath);

    // Ideally we should check (as we did earlier) if the given path really points to an existing
    // directory. However, it takes considerable amount of time for that check which hurts perf.
    // Given that HDFS code does not do such checks either, we choose to not do them in favor of
    // better performance.

    workingDirectory = newPath;
    logger.atFine().log("GHFS.setWorkingDirectory: => %s", workingDirectory);

    long duration = System.nanoTime() - startTime;
    increment(Counter.SET_WD);
    increment(Counter.SET_WD_TIME, duration);
  }

  /**
   * Gets the current working directory.
   *
   * @return The current working directory.
   */
  @Override
  public Path getWorkingDirectory() {
    logger.atFine().log("GHFS.getWorkingDirectory: %s", workingDirectory);
    return workingDirectory;
  }

  /**
   * Makes the given path and all non-existent parents directories.
   * Has the semantics of Unix 'mkdir -p'.
   *
   * @param hadoopPath Given path.
   * @param permission Permissions to set on the given directory.
   * @return true on success, false otherwise.
   * @throws IOException if an error occurs.
   */
  @Override
  public boolean mkdirs(Path hadoopPath, FsPermission permission)
      throws IOException {

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    logger.atFine().log("GHFS.mkdirs: %s, perm: %s", hadoopPath, permission);
    URI gcsPath = getGcsPath(hadoopPath);
    try {
      gcsfs.mkdirs(gcsPath);
    } catch (java.nio.file.FileAlreadyExistsException faee) {
      // Need to convert to the Hadoop flavor of FileAlreadyExistsException.
      throw (FileAlreadyExistsException)
          (new FileAlreadyExistsException(faee.getMessage()).initCause(faee));
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.MKDIRS);
    increment(Counter.MKDIRS_TIME, duration);

    return true;
  }

  /**
   * Gets the default replication factor.
   */
  @Override
  public short getDefaultReplication() {
    return REPLICATION_FACTOR_DEFAULT;
  }

  /**
   * Gets status of the given path item.
   *
   * @param hadoopPath The path we want information about.
   * @return A FileStatus object for the given path.
   * @throws FileNotFoundException when the path does not exist;
   * @throws IOException on other errors.
   */
  @Override
  public FileStatus getFileStatus(Path hadoopPath)
      throws IOException {

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    logger.atFine().log("GHFS.getFileStatus: %s", hadoopPath);
    URI gcsPath = getGcsPath(hadoopPath);
    FileInfo fileInfo = gcsfs.getFileInfo(gcsPath);
    if (!fileInfo.exists()) {
      logger.atFine().log("GHFS.getFileStatus: not found: %s", gcsPath);
      String msg = fileInfo.isDirectory() ? "Directory not found : " : "File not found : ";
      msg += hadoopPath.toString();
      throw new FileNotFoundException(msg);
    }
    String userName = getUgiUserName();
    FileStatus status = getFileStatus(fileInfo, userName);

    long duration = System.nanoTime() - startTime;
    increment(Counter.GET_FILE_STATUS);
    increment(Counter.GET_FILE_STATUS_TIME, duration);
    return status;
  }

  /** Gets FileStatus corresponding to the given FileInfo value. */
  private FileStatus getFileStatus(FileInfo fileInfo, String userName) throws IOException {
    // GCS does not provide modification time. It only provides creation time.
    // It works for objects because they are immutable once created.
    FileStatus status =
        new FileStatus(
            fileInfo.getSize(),
            fileInfo.isDirectory(),
            REPLICATION_FACTOR_DEFAULT,
            defaultBlockSize,
            /* modificationTime= */ fileInfo.getModificationTime(),
            /* accessTime= */ fileInfo.getModificationTime(),
            reportedPermissions,
            /* owner= */ userName,
            /* group= */ userName,
            getHadoopPath(fileInfo.getPath()));
    logger.atFine().log(
        "GHFS.getFileStatus: %s => %s", fileInfo.getPath(), lazy(() -> fileStatusToString(status)));
    return status;
  }

  /**
   * Determines based on config settings and suitability of {@code fixedPath} whether to use
   * flat globbing logic where we use a single large listing during globStatus to then perform
   * the core globbing logic in-memory.
   */
  @VisibleForTesting
  boolean shouldUseFlatGlob(Path fixedPath) {
    // Config setting overrides all else.
    if (!enableFlatGlob) {
      return false;
    }

    // Only works for filesystems where the base Hadoop Path scheme matches the underlying URI
    // scheme for GCS.
    if (!getUri().getScheme().equals(GoogleCloudStorageFileSystem.SCHEME)) {
      logger.atFine().log(
          "Flat glob is on, but doesn't work for scheme '%s'; using default behavior.",
          getUri().getScheme());
      return false;
    }

    // The full pattern should have a wildcard, otherwise there's no point doing the flat glob.
    GlobPattern fullPattern = new GlobPattern(fixedPath.toString());
    if (!fullPattern.hasWildcard()) {
      logger.atFine().log(
          "Flat glob is on, but Path '%s' has no wildcard; using default behavior.", fixedPath);
      return false;
    }

    // To use a flat glob, there must be an authority defined.
    if (Strings.isNullOrEmpty(fixedPath.toUri().getAuthority())) {
      logger.atInfo().log(
          "Flat glob is on, but Path '%s' has a empty authority, using default behavior.",
          fixedPath);
      return false;
    }

    // And the authority must not contain a wildcard.
    GlobPattern authorityPattern = new GlobPattern(fixedPath.toUri().getAuthority());
    if (authorityPattern.hasWildcard()) {
      logger.atInfo().log(
          "Flat glob is on, but Path '%s' has a wildcard authority, using default behavior.",
          fixedPath);
      return false;
    }

    return true;
  }

  @VisibleForTesting
  String trimToPrefixWithoutGlob(String path) {
    char[] wildcardChars = "*?{[".toCharArray();
    int trimIndex = path.length();

    // Find the first occurrence of any one of the wildcard characters, or just path.length()
    // if none are found.
    for (char wildcard : wildcardChars) {
      int wildcardIndex = path.indexOf(wildcard);
      if (wildcardIndex >= 0 && wildcardIndex < trimIndex) {
        trimIndex = wildcardIndex;
      }
    }
    return path.substring(0, trimIndex);
  }

  /**
   * Returns an array of FileStatus objects whose path names match pathPattern.
   *
   * Return null if pathPattern has no glob and the path does not exist.
   * Return an empty array if pathPattern has a glob and no path matches it.
   *
   * @param pathPattern A regular expression specifying the path pattern.
   * @return An array of FileStatus objects.
   * @throws IOException if an error occurs.
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return globStatus(pathPattern, DEFAULT_FILTER);
  }

  /**
   * Returns an array of FileStatus objects whose path names match pathPattern and is accepted by
   * the user-supplied path filter. Results are sorted by their path names.
   *
   * <p>Return null if pathPattern has no glob and the path does not exist. Return an empty array if
   * pathPattern has a glob and no path matches it.
   *
   * @param pathPattern A regular expression specifying the path pattern.
   * @param filter A user-supplied path filter.
   * @return An array of FileStatus objects.
   * @throws IOException if an error occurs.
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    checkOpen();

    logger.atFine().log("GHFS.globStatus: %s", pathPattern);
    // URI does not handle glob expressions nicely, for the purpose of
    // fully-qualifying a path we can URI-encode them.
    // Using toString() to avoid Path(URI) constructor.
    Path encodedPath = new Path(pathPattern.toUri().toString());
    // We convert pathPattern to GCS path and then to Hadoop path to ensure that it ends up in
    // the correct format. See note in getHadoopPath for more information.
    Path fixedPath = getHadoopPath(getGcsPath(encodedPath));
    // Decode URI-encoded path back into a glob path.
    fixedPath = new Path(URI.create(fixedPath.toString()));
    logger.atFine().log("GHFS.globStatus fixedPath: %s => %s", pathPattern, fixedPath);

    if (shouldUseFlatGlob(fixedPath)) {
      String pathString = fixedPath.toString();
      String prefixString = trimToPrefixWithoutGlob(pathString);
      Path prefixPath = new Path(prefixString);
      URI prefixUri = getGcsPath(prefixPath);

      if (prefixString.endsWith("/") && !prefixPath.toString().endsWith("/")) {
        // Path strips a trailing slash unless it's the 'root' path. We want to keep the trailing
        // slash so that we don't wastefully list sibling files which may match the directory-name
        // as a strict prefix but would've been omitted due to not containing the '/' at the end.
        prefixUri = FileInfo.convertToDirectoryPath(gcsfs.getPathCodec(), prefixUri);
      }

      // Get everything matching the non-glob prefix.
      logger.atFine().log("Listing everything with prefix '%s'", prefixUri);
      List<FileInfo> fileInfos = gcsfs.listAllFileInfoForPrefix(prefixUri);
      if (fileInfos.isEmpty()) {
        // Let the superclass define the proper logic for finding no matches.
        return super.globStatus(fixedPath, filter);
      }

      Collection<FileStatus> fileStatuses = toFileStatusesWithImplicitDirectories(fileInfos);

      // Perform the core globbing logic in the helper filesystem.
      FileSystem helperFileSystem =
          InMemoryGlobberFileSystem.createInstance(getConf(), getWorkingDirectory(), fileStatuses);
      FileStatus[] returnList = helperFileSystem.globStatus(fixedPath, filter);

      // If the return list contains directories, we should repair them if they're 'implicit'.
      if (enableAutoRepairImplicitDirectories) {
        List<URI> toRepair = new ArrayList<>();
        for (FileStatus status : returnList) {
          // Modification time of 0 indicates implicit directory.
          if (status.isDir() && status.getModificationTime() == 0) {
            toRepair.add(getGcsPath(status.getPath()));
          }
        }
        if (!toRepair.isEmpty()) {
          logger.atWarning().log(
              "Discovered %s implicit directories to repair within return values.",
              toRepair.size());
          gcsfs.repairDirs(toRepair);
        }
      }
      return returnList;
    } else {
      FileStatus[] ret = super.globStatus(fixedPath, filter);
      if (ret == null) {
        if (enableAutoRepairImplicitDirectories) {
          logger.atFine().log(
              "GHFS.globStatus returned null for '%s', attempting possible repair.", pathPattern);
          if (gcsfs.repairPossibleImplicitDirectory(getGcsPath(fixedPath))) {
            logger.atWarning().log("Success repairing '%s', re-globbing.", pathPattern);
            ret = super.globStatus(fixedPath, filter);
          }
        }
      }
      return ret;
    }
  }

  /** Helper method that converts {@link FileInfo} collection to {@link FileStatus} collection. */
  private Collection<FileStatus> toFileStatusesWithImplicitDirectories(
      Collection<FileInfo> fileInfos) throws IOException {
    List<FileStatus> fileStatuses = new ArrayList<>(fileInfos.size());
    Set<URI> filePaths = Sets.newHashSetWithExpectedSize(fileInfos.size());
    String userName = getUgiUserName();
    for (FileInfo fileInfo : fileInfos) {
      filePaths.add(fileInfo.getPath());
      fileStatuses.add(getFileStatus(fileInfo, userName));
    }

    // The flow for populating this doesn't bother to populate metadata entries for parent
    // directories but we know the parent directories are expected to exist, so we'll just
    // populate the missing entries explicitly here. Necessary for getFileStatus(parentOfInfo)
    // to work when using an instance of this class.
    for (FileInfo fileInfo : fileInfos) {
      URI parentPath = gcsfs.getParentPath(fileInfo.getPath());
      while (parentPath != null && !parentPath.equals(GoogleCloudStorageFileSystem.GCS_ROOT)) {
        if (!filePaths.contains(parentPath)) {
          logger.atFine().log("Adding fake entry for missing parent path '%s'", parentPath);
          StorageResourceId id = gcsfs.getPathCodec().validatePathAndGetId(parentPath, true);

          GoogleCloudStorageItemInfo fakeItemInfo =
              GoogleCloudStorageItemInfo.createInferredDirectory(id);
          FileInfo fakeFileInfo = FileInfo.fromItemInfo(gcsfs.getPathCodec(), fakeItemInfo);

          filePaths.add(parentPath);
          fileStatuses.add(getFileStatus(fakeFileInfo, userName));
        }
        parentPath = gcsfs.getParentPath(parentPath);
      }
    }

    return fileStatuses;
  }

  /** Helper method to get the UGI short user name */
  private static String getUgiUserName() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return ugi.getShortUserName();
  }

  /**
   * Returns home directory of the current user.
   *
   * Note: This directory is only used for Hadoop purposes.
   *       It is not the same as a user's OS home directory.
   */
  @Override
  public Path getHomeDirectory() {
    Path result = new Path(getFileSystemRoot(), getHomeDirectorySubpath());
    logger.atFine().log("GHFS.getHomeDirectory:=> %s", result);
    return result;
  }

  /**
   * Converts the given FileStatus to its string representation.
   *
   * @param stat FileStatus to convert.
   * @return String representation of the given FileStatus.
   */
  private static String fileStatusToString(FileStatus stat) {
    assert stat != null;

    return String.format(
        "path: %s, isDir: %s, len: %d, owner: %s",
        stat.getPath().toString(),
        stat.isDir(),
        stat.getLen(),
        stat.getOwner());
  }

  /**
   * Gets system bucket name.
   *
   * @deprecated Use getUri().authority instead.
   */
  @VisibleForTesting
  @Deprecated
  String getSystemBucketName() {
    return systemBucket;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns null, because GHFS does not use security tokens.
   */
  @Override
  public String getCanonicalServiceName() {
    logger.atFine().log("GHFS.getCanonicalServiceName:");
    logger.atFine().log("GHFS.getCanonicalServiceName:=> null");
    return null;
  }

  /**
   * Gets GCS FS instance.
   */
  public GoogleCloudStorageFileSystem getGcsFs() {
    return gcsfs;
  }

  /**
   * Increments by 1 the counter indicated by key.
   */
  void increment(Counter key) {
    increment(key, 1);
  }

  /**
   * Adds value to the counter indicated by key.
   */
  void increment(Counter key, long value) {
    counters.get(key).addAndGet(value);
  }

  /**
   * Gets value of all counters as a formatted string.
   */
  @VisibleForTesting
  String countersToString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    double numNanoSecPerSec = TimeUnit.SECONDS.toNanos(1);
    String timeSuffix = "_TIME";
    for (Counter c : Counter.values()) {
      String name = c.toString();
      if (!name.endsWith(timeSuffix)) {
        // Log invocation counter.
        long count = counters.get(c).get();
        sb.append(String.format("%20s = %d calls\n", name, count));

        // Log duration counter.
        String timeCounterName = name + timeSuffix;
        double totalTime =
            counters.get(Enum.valueOf(Counter.class, timeCounterName)).get()
                / numNanoSecPerSec;
        sb.append(String.format("%20s = %.2f sec\n", timeCounterName, totalTime));

        // Compute and log average duration per call (== total duration / num invocations).
        String avgName = name + " avg.";
        double avg = totalTime / count;
        sb.append(String.format("%20s = %.2f sec / call\n\n", avgName, avg));
      }
    }
    return sb.toString();
  }

  /**
   * Logs values of all counters.
   */
  private void logCounters() {
    logger.atFine().log("%s", lazy(this::countersToString));
  }

  /**
   * Copy the value of the deprecated key to the new key if a value is present for the deprecated
   * key, but not the new key.
   */
  private static void copyIfNotPresent(Configuration config, String deprecatedKey, String newKey) {
    String deprecatedValue = config.get(deprecatedKey);
    if (config.get(newKey) == null && deprecatedValue != null) {
      logger.atWarning().log(
          "Key %s is deprecated. Copying the value of key %s to new key %s",
          deprecatedKey, deprecatedKey, newKey);
      config.set(newKey, deprecatedValue);
    }
  }

  /**
   * Copy deprecated configuration options to new keys, if present.
   */
  private static void copyDeprecatedConfigurationOptions(Configuration config) {
    copyIfNotPresent(
        config,
        ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY,
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX);
    copyIfNotPresent(
        config,
        SERVICE_ACCOUNT_AUTH_KEYFILE_KEY,
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX);
    copyIfNotPresent(
        config,
        SERVICE_ACCOUNT_AUTH_EMAIL_KEY,
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX);
    copyIfNotPresent(
        config,
        GCS_CLIENT_ID_KEY,
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.CLIENT_ID_SUFFIX);
    copyIfNotPresent(
        config,
        GCS_CLIENT_SECRET_KEY,
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.CLIENT_SECRET_SUFFIX);

    String oauthClientFileKey =
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.OAUTH_CLIENT_FILE_SUFFIX;
    if (config.get(oauthClientFileKey) == null) {
      // No property to copy, but we can set this fairly safely (it's only invoked if client ID,
      // client secret are set and we're not using service accounts).
      config.set(
          oauthClientFileKey, System.getProperty("user.home") + "/.credentials/storage.json");
    }
  }

  /**
   * Retrieve user's Credential. If user implemented {@link AccessTokenProvider} and provided the
   * class name (See {@link AccessTokenProviderClassFromConfigFactory} then build a credential with
   * access token provided by this provider; Otherwise obtain credential through {@link
   * HadoopCredentialConfiguration#getCredential(List)}.
   */
  private Credential getCredential(
      AccessTokenProviderClassFromConfigFactory providerClassFactory, Configuration config)
      throws IOException, GeneralSecurityException {
    Credential credential =
        CredentialFromAccessTokenProviderClassFactory.credential(
            providerClassFactory, config, CredentialFactory.GCS_SCOPES);
    if (credential != null) {
      return credential;
    }

    return HadoopCredentialConfiguration.newBuilder()
        .withConfiguration(config)
        .withOverridePrefix(AUTHENTICATION_PREFIX)
        .build()
        .getCredential(CredentialFactory.GCS_SCOPES);
  }

  /**
   * Configures GHFS using the supplied configuration.
   *
   * @param config Hadoop configuration object.
   */
  private synchronized void configure(Configuration config)
      throws IOException {
    logger.atFine().log("GHFS.configure");
    logger.atFine().log("GHFS_ID = %s", GHFS_ID);

    if (gcsfs == null) {

      copyDeprecatedConfigurationOptions(config);

      Credential credential;
      try {
        credential =
            getCredential(
                new AccessTokenProviderClassFromConfigFactory().withOverridePrefix("fs.gs"),
                config);
      } catch (GeneralSecurityException gse) {
        throw new IOException(gse);
      }

      GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
          createOptionsBuilderFromConfig(config);

      PathCodec pathCodec;
      String specifiedPathCodec = config.get(PATH_CODEC_KEY, PATH_CODEC_DEFAULT).toLowerCase();
      logger.atFine().log("%s = %s", PATH_CODEC_KEY, specifiedPathCodec);
      if (specifiedPathCodec.equals(PATH_CODEC_USE_LEGACY_ENCODING)) {
        pathCodec = GoogleCloudStorageFileSystem.LEGACY_PATH_CODEC;
      } else if (specifiedPathCodec.equals(PATH_CODEC_USE_URI_ENCODING)) {
        pathCodec = GoogleCloudStorageFileSystem.URI_ENCODED_PATH_CODEC;
      } else {
        pathCodec = GoogleCloudStorageFileSystem.LEGACY_PATH_CODEC;
        logger.atWarning().log(
            "Unknown path codec specified %s. Using default / legacy.", specifiedPathCodec);
      }
      optionsBuilder.setPathCodec(pathCodec);
      gcsfs = new GoogleCloudStorageFileSystem(credential, optionsBuilder.build());
    }

    defaultBlockSize = config.getLong(BLOCK_SIZE_KEY, BLOCK_SIZE_DEFAULT);
    logger.atFine().log("%s = %s", BLOCK_SIZE_KEY, defaultBlockSize);

    String systemBucketName = config.get(GCS_SYSTEM_BUCKET_KEY, null);
    logger.atFine().log("%s = %s", GCS_SYSTEM_BUCKET_KEY, systemBucketName);

    boolean createSystemBucket =
        config.getBoolean(GCS_CREATE_SYSTEM_BUCKET_KEY, GCS_CREATE_SYSTEM_BUCKET_DEFAULT);
    logger.atFine().log("%s = %s", GCS_CREATE_SYSTEM_BUCKET_KEY, createSystemBucket);

    reportedPermissions = new FsPermission(
        config.get(PERMISSIONS_TO_REPORT_KEY, PERMISSIONS_TO_REPORT_DEFAULT));
    logger.atFine().log("%s = %s", PERMISSIONS_TO_REPORT_KEY, reportedPermissions);

    configureBuckets(systemBucketName, createSystemBucket);

    // Set initial working directory to root so that any configured value gets resolved
    // against file system root.
    workingDirectory = getFileSystemRoot();

    Path newWorkingDirectory;
    String configWorkingDirectory = config.get(GCS_WORKING_DIRECTORY_KEY);
    if (Strings.isNullOrEmpty(configWorkingDirectory)) {
      newWorkingDirectory = getDefaultWorkingDirectory();
      logger.atWarning().log(
          "No working directory configured, using default: '%s'", newWorkingDirectory);
    } else {
      newWorkingDirectory = new Path(configWorkingDirectory);
    }

    // Use the public method to ensure proper behavior of normalizing and resolving the new
    // working directory relative to the initial filesystem-root directory.
    setWorkingDirectory(newWorkingDirectory);
    logger.atFine().log("%s = %s", GCS_WORKING_DIRECTORY_KEY, getWorkingDirectory());

    // Set this configuration as the default config for this instance.
    setConf(config);

    logger.atFine().log("GHFS.configure: done");
  }

  /**
   * Validates and possibly creates the system bucket. Should be overridden to configure other
   * buckets.
   *
   * @param systemBucketName Name of system bucket
   * @param createSystemBucket Whether or not to create systemBucketName if it does not exist.
   * @throws IOException if systemBucketName is invalid or cannot be found and createSystemBucket
   *     is false.
   */
  @VisibleForTesting
  // TODO(user): Refactor to make protected
  public void configureBuckets(String systemBucketName, boolean createSystemBucket)
      throws IOException {

    logger.atFine().log("GHFS.configureBuckets: %s, %s", systemBucketName, createSystemBucket);

    systemBucket = systemBucketName;

    if (systemBucket != null) {
      logger.atFine().log("GHFS.configureBuckets: Warning fs.gs.system.bucket is deprecated.");
      // Ensure that system bucket exists. It really must be a bucket, not a GCS path.
      URI systemBucketPath = gcsfs.getPathCodec().getPath(systemBucket, null, true);

      checkOpen();

      if (!gcsfs.exists(systemBucketPath)) {
        if (createSystemBucket) {
          gcsfs.mkdirs(systemBucketPath);
        } else {
          String msg =
              String.format("%s: system bucket not found: %s", GCS_SYSTEM_BUCKET_KEY, systemBucket);
          throw new FileNotFoundException(msg);
        }
      }
    }

    logger.atFine().log("GHFS.configureBuckets:=>");
  }

  /**
   * Assert that the FileSystem has been initialized and not close()d.
   */
  private void checkOpen() throws IOException {
    if (gcsfs == null) {
      throw new IOException("GoogleHadoopFileSystem has been closed or not initialized.");
    }
  }

  // =================================================================
  // Overridden functions for debug tracing. The following functions
  // do not change functionality. They just log parameters and call base
  // class' function.
  // =================================================================

  @Override
  public boolean deleteOnExit(Path f)
      throws IOException {

    checkOpen();

    logger.atFine().log("GHFS.deleteOnExit: %s", f);
    boolean result = super.deleteOnExit(f);
    logger.atFine().log("GHFS.deleteOnExit:=> %s", result);
    return result;
  }

  @Override
  protected void processDeleteOnExit() {
    logger.atFine().log("GHFS.processDeleteOnExit:");
    super.processDeleteOnExit();
  }

  @Override
  public ContentSummary getContentSummary(Path f)
      throws IOException {
    logger.atFine().log("GHFS.getContentSummary: %s", f);
    ContentSummary result = super.getContentSummary(f);
    logger.atFine().log("GHFS.getContentSummary:=> %s", result);
    return result;
  }

  @Override
  public Token<?> getDelegationToken(String renewer)
      throws IOException {
    logger.atFine().log("GHFS.getDelegationToken: renewer: %s", renewer);
    Token<?> result = super.getDelegationToken(renewer);
    logger.atFine().log("GHFS.getDelegationToken:=> %s", result);
    return result;
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
      Path[] srcs, Path dst)
      throws IOException {
    logger.atFine().log(
        "GHFS.copyFromLocalFile: delSrc: %s, overwrite: %s, #srcs: %s, dst: %s",
        delSrc, overwrite, srcs.length, dst);
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    logger.atFine().log("GHFS.copyFromLocalFile:=> ");
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
      Path src, Path dst)
      throws IOException {
    logger.atFine().log(
        "GHFS.copyFromLocalFile: delSrc: %s, overwrite: %s, src: %s, dst: %s",
        delSrc, overwrite, src, dst);
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
    logger.atFine().log("GHFS.copyFromLocalFile:=> ");
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
      throws IOException {
    logger.atFine().log("GHFS.copyToLocalFile: delSrc: %s, src: %s, dst: %s", delSrc, src, dst);
    super.copyToLocalFile(delSrc, src, dst);
    logger.atFine().log("GHFS.copyToLocalFile:=> ");
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    logger.atFine().log("GHFS.startLocalOutput: out: %s, tmp: %s", fsOutputFile, tmpLocalFile);
    Path result = super.startLocalOutput(fsOutputFile, tmpLocalFile);
    logger.atFine().log("GHFS.startLocalOutput:=> %s", result);
    return result;
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    logger.atFine().log("GHFS.startLocalOutput: out: %s, tmp: %s", fsOutputFile, tmpLocalFile);
    super.completeLocalOutput(fsOutputFile, tmpLocalFile);
    logger.atFine().log("GHFS.completeLocalOutput:=> ");
  }

  @Override
  public void close()
      throws IOException {
    logger.atFine().log("GHFS.close:");
    super.close();

    // NB: We must *first* have the superclass close() before we close the underlying gcsfs since
    // the superclass may decide to perform various heavyweight cleanup operations (such as
    // deleteOnExit).
    if (gcsfs != null) {
      gcsfs.close();
      gcsfs = null;
    }
    logCounters();
    logger.atFine().log("GHFS.close:=> ");
  }

  @Override
  public long getUsed()
      throws IOException{
    logger.atFine().log("GHFS.getUsed:");
    long result = super.getUsed();
    logger.atFine().log("GHFS.getUsed:=> %s", result);
    return result;
  }

  @Override
  public long getDefaultBlockSize() {
    logger.atFine().log("GHFS.getDefaultBlockSize:");
    long result = defaultBlockSize;
    logger.atFine().log("GHFS.getDefaultBlockSize:=> %s", result);
    return result;
  }

  @Override
  public FileChecksum getFileChecksum(Path f)
      throws IOException {
    logger.atFine().log("GHFS.getFileChecksum:");
    FileChecksum result = super.getFileChecksum(f);
    logger.atFine().log("GHFS.getFileChecksum:=> %s", result);
    return result;
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    logger.atFine().log("GHFS.setVerifyChecksum:");
    super.setVerifyChecksum(verifyChecksum);
    logger.atFine().log("GHFS.setVerifyChecksum:=> ");
  }

  @Override
  public void setPermission(Path p, FsPermission permission)
      throws IOException {
    logger.atFine().log("GHFS.setPermission: path: %s, perm: %s", p, permission);
    super.setPermission(p, permission);
    logger.atFine().log("GHFS.setPermission:=> ");
  }

  @Override
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    logger.atFine().log("GHFS.setOwner: path: %s, user: %s, group: %s", p, username, groupname);
    super.setOwner(p, username, groupname);
    logger.atFine().log("GHFS.setOwner:=> ");
  }

  @Override
  public void setTimes(Path p, long mtime, long atime)
      throws IOException {
    logger.atFine().log("GHFS.setTimes: path: %s, mtime: %s, atime: %s", p, mtime, atime);
    super.setTimes(p, mtime, atime);
    logger.atFine().log("GHFS.setTimes:=> ");
  }

  @VisibleForTesting
  GoogleCloudStorageFileSystemOptions.Builder createOptionsBuilderFromConfig(Configuration config)
      throws IOException {
    GoogleCloudStorageFileSystemOptions.Builder gcsFsOptionsBuilder =
        GoogleCloudStorageFileSystemOptions.newBuilder();

    boolean enableBucketDelete = config.getBoolean(
        GCE_BUCKET_DELETE_ENABLE_KEY, GCE_BUCKET_DELETE_ENABLE_DEFAULT);
    logger.atFine().log("%s = %s", GCE_BUCKET_DELETE_ENABLE_KEY, enableBucketDelete);
    gcsFsOptionsBuilder.setEnableBucketDelete(enableBucketDelete);

    GoogleCloudStorageFileSystemOptions.TimestampUpdatePredicate updatePredicate =
        ParentTimestampUpdateIncludePredicate.create(config);
    gcsFsOptionsBuilder.setShouldIncludeInTimestampUpdatesPredicate(updatePredicate);

    GoogleCloudStorageOptions.Builder gcsOptionsBuilder =
        gcsFsOptionsBuilder.getCloudStorageOptionsBuilder();

    enableAutoRepairImplicitDirectories = config.getBoolean(
        GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY, GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT);
    logger.atFine().log(
        "%s = %s", GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY, enableAutoRepairImplicitDirectories);
    gcsOptionsBuilder.setAutoRepairImplicitDirectoriesEnabled(enableAutoRepairImplicitDirectories);

    enableInferImplicitDirectories = config.getBoolean(
        GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_KEY, GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_DEFAULT);
    logger.atFine().log(
        "%s = %s", GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_KEY, enableInferImplicitDirectories);
    gcsOptionsBuilder.setInferImplicitDirectoriesEnabled(enableInferImplicitDirectories);

    enableFlatGlob = config.getBoolean(GCS_ENABLE_FLAT_GLOB_KEY, GCS_ENABLE_FLAT_GLOB_DEFAULT);
    logger.atFine().log("%s = %s", GCS_ENABLE_FLAT_GLOB_KEY, enableFlatGlob);

    boolean enableMarkerFileCreation = config.getBoolean(
        GCS_ENABLE_MARKER_FILE_CREATION_KEY, GCS_ENABLE_MARKER_FILE_CREATION_DEFAULT);
    logger.atFine().log("%s = %s", GCS_ENABLE_MARKER_FILE_CREATION_KEY, enableMarkerFileCreation);
    gcsOptionsBuilder.setMarkerFileCreationEnabled(enableMarkerFileCreation);

    boolean enableCopyWithRewrite =
        config.getBoolean(GCS_ENABLE_COPY_WITH_REWRITE_KEY, GCS_ENABLE_COPY_WITH_REWRITE_DEFAULT);
    logger.atFine().log("%s = %s", GCS_ENABLE_COPY_WITH_REWRITE_KEY, enableCopyWithRewrite);
    gcsOptionsBuilder.setCopyWithRewriteEnabled(enableCopyWithRewrite);

    long copyMaxRequestsPerBatch =
        config.getLong(GCS_COPY_MAX_REQUESTS_PER_BATCH, GCS_COPY_MAX_REQUESTS_PER_BATCH_DEFAULT);
    logger.atFine().log("%s = %s", GCS_COPY_MAX_REQUESTS_PER_BATCH, copyMaxRequestsPerBatch);
    gcsOptionsBuilder.setCopyMaxRequestsPerBatch(copyMaxRequestsPerBatch);

    int copyBatchThreads = config.getInt(GCS_COPY_BATCH_THREADS, GCS_COPY_BATCH_THREADS_DEFAULT);
    logger.atFine().log("%s = %s", GCS_COPY_BATCH_THREADS, copyBatchThreads);
    gcsOptionsBuilder.setCopyBatchThreads(copyBatchThreads);

    String transportTypeString = config.get(GCS_HTTP_TRANSPORT_KEY, GCS_HTTP_TRANSPORT_DEFAULT);
    logger.atFine().log("%s = %s", GCS_HTTP_TRANSPORT_KEY, transportTypeString);
    gcsOptionsBuilder.setTransportType(
        HttpTransportFactory.getTransportTypeOf(transportTypeString));

    String proxyAddress = config.get(GCS_PROXY_ADDRESS_KEY, GCS_PROXY_ADDRESS_DEFAULT);
    logger.atFine().log("%s = %s", GCS_PROXY_ADDRESS_KEY, proxyAddress);
    gcsOptionsBuilder.setProxyAddress(proxyAddress);

    String projectId = config.get(GCS_PROJECT_ID_KEY);
    logger.atFine().log("%s = %s", GCS_PROJECT_ID_KEY, projectId);
    gcsOptionsBuilder.setProjectId(projectId);

    long maxListItemsPerCall =
        config.getLong(GCS_MAX_LIST_ITEMS_PER_CALL, GCS_MAX_LIST_ITEMS_PER_CALL_DEFAULT);
    logger.atFine().log("%s = %s", GCS_MAX_LIST_ITEMS_PER_CALL, maxListItemsPerCall);
    gcsOptionsBuilder.setMaxListItemsPerCall(maxListItemsPerCall);

    long maxRequestsPerBatch =
        config.getLong(GCS_MAX_REQUESTS_PER_BATCH, GCS_MAX_REQUESTS_PER_BATCH_DEFAULT);
    logger.atFine().log("%s = %s", GCS_MAX_REQUESTS_PER_BATCH, maxRequestsPerBatch);
    gcsOptionsBuilder.setMaxRequestsPerBatch(maxRequestsPerBatch);

    int batchThreads = config.getInt(GCS_BATCH_THREADS, GCS_BATCH_THREADS_DEFAULT);
    logger.atFine().log("%s = %s", GCS_BATCH_THREADS, batchThreads);
    gcsOptionsBuilder.setBatchThreads(batchThreads);

    int maxHttpRequestRetries = config.getInt(GCS_HTTP_MAX_RETRY_KEY, GCS_HTTP_MAX_RETRY_DEFAULT);
    logger.atFine().log("%s = %s", GCS_HTTP_MAX_RETRY_KEY, maxHttpRequestRetries);
    gcsOptionsBuilder.setMaxHttpRequestRetries(maxHttpRequestRetries);

    int httpRequestConnectTimeout =
        config.getInt(GCS_HTTP_CONNECT_TIMEOUT_KEY, GCS_HTTP_CONNECT_TIMEOUT_DEFAULT);
    logger.atFine().log("%s = %s", GCS_HTTP_CONNECT_TIMEOUT_KEY, httpRequestConnectTimeout);
    gcsOptionsBuilder.setHttpRequestConnectTimeout(httpRequestConnectTimeout);

    int httpRequestReadTimeout =
        config.getInt(GCS_HTTP_READ_TIMEOUT_KEY, GCS_HTTP_READ_TIMEOUT_DEFAULT);
    logger.atFine().log("%s = %s", GCS_HTTP_READ_TIMEOUT_KEY, httpRequestReadTimeout);
    gcsOptionsBuilder.setHttpRequestReadTimeout(httpRequestReadTimeout);

    String markerFilePattern = config.get(GCS_MARKER_FILE_PATTERN_KEY);
    logger.atFine().log("%s = %s", GCS_MARKER_FILE_PATTERN_KEY, markerFilePattern);
    gcsFsOptionsBuilder.setMarkerFilePattern(markerFilePattern);

    String applicationNameSuffix =
        config.get(GCS_APPLICATION_NAME_SUFFIX_KEY, GCS_APPLICATION_NAME_SUFFIX_DEFAULT);
    logger.atFine().log("%s = %s", GCS_APPLICATION_NAME_SUFFIX_KEY, applicationNameSuffix);
    String applicationName = GHFS_ID + nullToEmpty(applicationNameSuffix);
    logger.atFine().log("Setting GCS application name to %s", applicationName);
    gcsOptionsBuilder.setAppName(applicationName);

    int maxWaitMillisForEmptyObjectCreation = config.getInt(
        GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE_KEY,
        GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE_DEFAULT);
    logger.atFine().log(
        "%s = %s",
        GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE_KEY, maxWaitMillisForEmptyObjectCreation);
    gcsOptionsBuilder.setMaxWaitMillisForEmptyObjectCreation(maxWaitMillisForEmptyObjectCreation);

    boolean enablePerformanceCache =
        config.getBoolean(GCS_ENABLE_PERFORMANCE_CACHE_KEY, GCS_ENABLE_PERFORMANCE_CACHE_DEFAULT);
    logger.atFine().log("%s = %s", GCS_ENABLE_PERFORMANCE_CACHE_KEY, enablePerformanceCache);
    gcsFsOptionsBuilder
        .setIsPerformanceCacheEnabled(enablePerformanceCache)
        .setImmutablePerformanceCachingOptions(getPerformanceCachingOptions(config));

    gcsOptionsBuilder
        .setReadChannelOptions(getReadChannelOptions(config))
        .setWriteChannelOptions(getWriteChannelOptions(config))
        .setRequesterPaysOptions(getRequesterPaysOptions(config, projectId));

    return gcsFsOptionsBuilder;
  }

  private static PerformanceCachingGoogleCloudStorageOptions getPerformanceCachingOptions(
      Configuration config) {
    long performanceCacheMaxEntryAgeMillis =
        config.getLong(
            GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS_KEY,
            GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS_DEFAULT);
    logger.atFine().log(
        "%s = %s",
        GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS_KEY, performanceCacheMaxEntryAgeMillis);

    boolean listCachingEnabled =
        config.getBoolean(
            GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE_KEY,
            GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE_DEFAULT);
    logger.atFine().log(
        "%s = %s", GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE_KEY, listCachingEnabled);

    long dirMetadataPrefetchLimit =
        config.getLong(
            GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT_KEY,
            GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT_DEFAULT);
    logger.atFine().log(
        "%s = %s", GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT_KEY, dirMetadataPrefetchLimit);

    return PerformanceCachingGoogleCloudStorageOptions.builder()
        .setMaxEntryAgeMillis(performanceCacheMaxEntryAgeMillis)
        .setListCachingEnabled(listCachingEnabled)
        .setDirMetadataPrefetchLimit(dirMetadataPrefetchLimit)
        .build();
  }

  private static GoogleCloudStorageReadOptions getReadChannelOptions(Configuration configuration) {
    boolean fastFailOnNotFound =
        configuration.getBoolean(
            GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_KEY,
            GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_DEFAULT);
    logger.atFine().log(
        "%s = %s", GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_KEY, fastFailOnNotFound);

    long inplaceSeekLimit =
        configuration.getLong(
            GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_KEY, GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_DEFAULT);
    logger.atFine().log("%s = %d", GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_KEY, inplaceSeekLimit);

    int bufferSize = configuration.getInt(BUFFERSIZE_KEY, BUFFERSIZE_DEFAULT);
    logger.atFine().log("%s = %d", BUFFERSIZE_KEY, bufferSize);

    Fadvise fadvise =
        configuration.getEnum(GCS_INPUTSTREAM_FADVISE_KEY, GCS_INPUTSTREAM_FADVISE_DEFAULT);
    logger.atFine().log("%s = %s", GCS_INPUTSTREAM_FADVISE_KEY, fadvise);

    int minRangeRequestSize =
        configuration.getInt(
            GCS_INPUTSTREAM_MIN_RANGE_REQUEST_SIZE_KEY,
            GCS_INPUTSTREAM_MIN_RANGE_REQUEST_SIZE_DEFAULT);
    logger.atFine().log("%s = %d", GCS_INPUTSTREAM_MIN_RANGE_REQUEST_SIZE_KEY, minRangeRequestSize);

    GenerationReadConsistency generationConsistency =
        configuration.getEnum(
            GCS_GENERATION_READ_CONSISTENCY_KEY, GCS_GENERATION_READ_CONSISTENCY_DEFAULT);
    logger.atFine().log("%s = %s", GCS_GENERATION_READ_CONSISTENCY_KEY, generationConsistency);

    return GoogleCloudStorageReadOptions.builder()
        .setFastFailOnNotFound(fastFailOnNotFound)
        .setInplaceSeekLimit(inplaceSeekLimit)
        .setBufferSize(bufferSize)
        .setFadvise(fadvise)
        .setMinRangeRequestSize(minRangeRequestSize)
        .setGenerationReadConsistency(generationConsistency)
        .build();
  }

  private static AsyncWriteChannelOptions getWriteChannelOptions(Configuration config) {
    // Configuration for setting 250GB upper limit on file size to gain higher write throughput.
    boolean limitFileSizeTo250Gb =
        config.getBoolean(GCS_FILE_SIZE_LIMIT_250GB, GCS_FILE_SIZE_LIMIT_250GB_DEFAULT);
    logger.atFine().log("%s = %s", GCS_FILE_SIZE_LIMIT_250GB, limitFileSizeTo250Gb);

    // Configuration for setting GoogleCloudStorageWriteChannel upload buffer size.
    int uploadBufferSize = config.getInt(WRITE_BUFFERSIZE_KEY, WRITE_BUFFERSIZE_DEFAULT);
    logger.atFine().log("%s = %s", WRITE_BUFFERSIZE_KEY, uploadBufferSize);

    return AsyncWriteChannelOptions.newBuilder()
        .setFileSizeLimitedTo250Gb(limitFileSizeTo250Gb)
        .setUploadBufferSize(uploadBufferSize)
        .build();
  }

  private static RequesterPaysOptions getRequesterPaysOptions(
      Configuration config, String projectId) {
    RequesterPaysMode requesterPaysMode =
        config.getEnum(GCS_REQUESTER_PAYS_MODE_KEY, REQUESTER_PAYS_MODE_DEFAULT);
    logger.atFine().log("%s = %s", GCS_REQUESTER_PAYS_MODE_KEY, requesterPaysMode);

    String requesterPaysProjectId = config.get(GCS_REQUESTER_PAYS_PROJECT_ID_KEY);
    logger.atFine().log("%s = %s", GCS_REQUESTER_PAYS_PROJECT_ID_KEY, requesterPaysProjectId);

    Collection<String> requesterPaysBuckets =
        config.getStringCollection(GCS_REQUESTER_PAYS_BUCKETS_KEY);
    logger.atFine().log("%s = %s", GCS_REQUESTER_PAYS_BUCKETS_KEY, requesterPaysBuckets);

    return RequesterPaysOptions.builder()
        .setMode(requesterPaysMode)
        .setProjectId(requesterPaysProjectId == null ? projectId : requesterPaysProjectId)
        .setBuckets(requesterPaysBuckets)
        .build();
  }
}
