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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONCURRENT_GLOB_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CREATE_SYSTEM_BUCKET;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_FILE_CHECKSUM_TYPE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_FLAT_GLOB_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_TYPE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_SYSTEM_BUCKET;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_WORKING_DIRECTORY;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.PATH_CODEC;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.PERMISSIONS_TO_REPORT;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.flogger.LazyArgs.lazy;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.GenerationReadConsistency;
import com.google.cloud.hadoop.gcsio.PathCodec;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.util.AccessTokenProviderClassFromConfigFactory;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.CredentialFromAccessTokenProviderClassFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.cloud.hadoop.util.HadoopVersionInfo;
import com.google.cloud.hadoop.util.PropertyUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.flogger.GoogleLogger;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.codec.binary.Hex;
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

  /**
   * Available types for use with {@link
   * GoogleHadoopFileSystemConfiguration#GCS_OUTPUT_STREAM_TYPE}.
   */
  public enum OutputStreamType {
    BASIC,
    SYNCABLE_COMPOSITE
  }

  /**
   * Available GCS checksum types for use with {@link
   * GoogleHadoopFileSystemConfiguration#GCS_FILE_CHECKSUM_TYPE}.
   */
  public static enum GcsFileChecksumType {
    NONE(null, 0),
    CRC32C("COMPOSITE-CRC32C", 4),
    MD5("MD5", 16);

    private final String algorithmName;
    private final int byteLength;

    GcsFileChecksumType(String algorithmName, int byteLength) {
      this.algorithmName = algorithmName;
      this.byteLength = byteLength;
    }

    public String getAlgorithmName() {
      return algorithmName;
    }

    public int getByteLength() {
      return byteLength;
    }
  }

  /** Use new URI_ENCODED_PATH_CODEC. */
  public static final String PATH_CODEC_USE_URI_ENCODING = "uri-path";
  /** Use LEGACY_PATH_CODEC. */
  public static final String PATH_CODEC_USE_LEGACY_ENCODING = "legacy";

  /** Default value of replication factor. */
  public static final short REPLICATION_FACTOR_DEFAULT = 3;

  /** Default PathFilter that accepts all paths. */
  public static final PathFilter DEFAULT_FILTER = path -> true;

  /** Prefix to use for common authentication keys. */
  public static final String AUTHENTICATION_PREFIX = "fs.gs";

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
    logger.atFine().log("GHFS version: %s", VERSION);
    GHFS_ID = String.format("GHFS/%s", VERSION);
  }

  /**
   * Instance value of {@link
   * GoogleHadoopFileSystemConfiguration#GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE} based on the
   * initial Configuration.
   */
  private boolean enableAutoRepairImplicitDirectories =
      GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getDefault();

  @VisibleForTesting
  boolean enableFlatGlob = GCS_FLAT_GLOB_ENABLE.getDefault();

  @VisibleForTesting
  boolean enableConcurrentGlob = GCS_CONCURRENT_GLOB_ENABLE.getDefault();

  private GcsFileChecksumType checksumType = GCS_FILE_CHECKSUM_TYPE.getDefault();

  /** The URI the File System is passed in initialize. */
  protected URI initUri;

  /**
   * The retrieved configuration value for {@link
   * GoogleHadoopFileSystemConfiguration#GCS_SYSTEM_BUCKET}. Used as a fallback for a root bucket,
   * when required.
   */
  @Deprecated protected String systemBucket;

  /** Underlying GCS file system object. */
  protected GoogleCloudStorageFileSystem gcsfs;

  /**
   * Current working directory; overridden in initialize() if {@link
   * GoogleHadoopFileSystemConfiguration#GCS_WORKING_DIRECTORY} is set.
   */
  private Path workingDirectory;

  /**
   * Default block size. Note that this is the size that is reported to Hadoop FS clients. It does
   * not modify the actual block size of an underlying GCS object, because GCS JSON API does not
   * allow modifying or querying the value. Modifying this value allows one to control how many
   * mappers are used to process a given file.
   */
  protected long defaultBlockSize = GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getDefault();

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
    GET_FILE_CHECKSUM,
    GET_FILE_CHECKSUM_TIME,
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
   * GCS {@link FileChecksum} which takes constructor parameters to define the return values of the
   * various abstract methods of {@link FileChecksum}.
   */
  private static class GcsFileChecksum extends FileChecksum {
    private final GcsFileChecksumType checksumType;
    private final byte[] bytes;

    public GcsFileChecksum(GcsFileChecksumType checksumType, byte[] bytes) {
      this.checksumType = checksumType;
      this.bytes = bytes;
      checkState(
          bytes == null || bytes.length == checksumType.getByteLength(),
          "Checksum value length (%s) should be equal to the algorithm byte length (%s)",
          checksumType.getByteLength(), bytes.length);
    }

    @Override
    public String getAlgorithmName() {
      return checksumType.getAlgorithmName();
    }

    @Override
    public int getLength() {
      return checksumType.getByteLength();
    }

    @Override
    public byte[] getBytes() {
      return bytes;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      in.readFully(bytes);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.write(bytes);
    }

    @Override
    public String toString() {
      return getAlgorithmName() + ": " + (bytes == null ? null : new String(Hex.encodeHex(bytes)));
    }
  }

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
      return new ParentTimestampUpdateIncludePredicate(
          GCS_PARENT_TIMESTAMP_UPDATE_ENABLE.get(config, config::getBoolean),
          GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getStringCollection(config),
          GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES.getStringCollection(config));
    }

    // Include and exclude lists are intended to be small N and checked relatively
    // infrequently. If that becomes not that case, consider Aho-Corasick or similar matching
    // algorithms.
    private final Collection<String> includeSubstrings;
    private final Collection<String> excludeSubstrings;
    private final boolean enableTimestampUpdates;

    public ParentTimestampUpdateIncludePredicate(
        boolean enableTimestampUpdates,
        Collection<String> includeSubstrings,
        Collection<String> excludeSubstrings) {
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

    checkState(
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
   * overridden using configuration parameter {@code fs.gs.inputstream.buffer.size}.
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
   * <p>Note: This function overrides the given bufferSize value with a higher number unless further
   * overridden using configuration parameter {@code fs.gs.outputstream.buffer.size}.
   *
   * @param hadoopPath The file to open.
   * @param permission Permissions to set on the new file. Ignored.
   * @param overwrite If a file with this name already exists, then if true, the file will be
   *     overwritten, and if false an error will be thrown.
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
      Progressable progress)
      throws IOException {

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

    OutputStreamType type = GCS_OUTPUT_STREAM_TYPE.get(getConf(), getConf()::getEnum);
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
        throw new IOException(
            String.format(
                "Unsupported output stream type given for key '%s': '%s'",
                GCS_OUTPUT_STREAM_TYPE.getKey(), type));
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
      List<FileInfo> fileInfos = gcsfs.listFileInfo(gcsPath, enableAutoRepairImplicitDirectories);
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
      throw new FileNotFoundException(
          (fileInfo.isDirectory() ? "Directory not found : " : "File not found : ") + hadoopPath);
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
   * Determines based on suitability of {@code fixedPath} whether to use flat globbing logic where
   * we use a single large listing during globStatus to then perform the core globbing logic
   * in-memory.
   */
  @VisibleForTesting
  boolean couldUseFlatGlob(Path fixedPath) {
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
    Path encodedFixedPath = getHadoopPath(getGcsPath(encodedPath));
    // Decode URI-encoded path back into a glob path.
    Path fixedPath = new Path(URI.create(encodedFixedPath.toString()));
    logger.atFine().log("GHFS.globStatus fixedPath: %s => %s", pathPattern, fixedPath);

    if (enableConcurrentGlob && couldUseFlatGlob(fixedPath)) {
      return concurrentGlobInternal(fixedPath, filter, pathPattern);
    }

    if (enableFlatGlob && couldUseFlatGlob(fixedPath)) {
      return flatGlobInternal(fixedPath, filter);
    }

    return globInternal(fixedPath, filter, pathPattern);
  }

  private FileStatus[] concurrentGlobInternal(Path fixedPath, PathFilter filter, Path pathPattern)
      throws IOException {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    Callable<FileStatus[]> flatGlobTask = () -> flatGlobInternal(fixedPath, filter);
    Callable<FileStatus[]> nonFlatGlobTask = () -> globInternal(fixedPath, filter, pathPattern);

    try {
      return executorService.invokeAny(Arrays.asList(flatGlobTask, nonFlatGlobTask));
    } catch (InterruptedException | ExecutionException e) {
      throw (e.getCause() instanceof IOException) ? (IOException) e.getCause() : new IOException(e);
    } finally {
      executorService.shutdownNow();
    }
  }

  private FileStatus[] flatGlobInternal(Path fixedPath, PathFilter filter) throws IOException {
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
    List<FileStatus> matchedStatuses = null;
    String pageToken = null;
    do {
      ListPage<FileInfo> infoPage = gcsfs.listAllFileInfoForPrefixPage(prefixUri, pageToken);

      // TODO: Are implicit directories really always needed for globbing?
      //  Probably they should be inferred only when fs.gs.implicit.dir.infer.enable is true.
      Collection<FileStatus> statusPage =
          toFileStatusesWithImplicitDirectories(infoPage.getItems());

      // TODO: refactor to use GlobPattern and PathFilter directly without helper FS
      FileSystem helperFileSystem =
          InMemoryGlobberFileSystem.createInstance(getConf(), getWorkingDirectory(), statusPage);
      FileStatus[] matchedStatusPage = helperFileSystem.globStatus(fixedPath, filter);
      if (matchedStatusPage != null) {
        Collections.addAll(
            (matchedStatuses == null ? matchedStatuses = new ArrayList<>() : matchedStatuses),
            matchedStatusPage);
      }

      pageToken = infoPage.getNextPageToken();
    } while (pageToken != null);

    if (matchedStatuses == null || matchedStatuses.isEmpty()) {
      return matchedStatuses == null ? null : new FileStatus[0];
    }

    matchedStatuses.sort(
        ((Comparator<FileStatus>) Comparator.<FileStatus>naturalOrder())
            // Place duplicate implicit directories after real directory
            .thenComparingInt((FileStatus f) -> isImplicitDirectory(f) ? 1 : 0));

    // Remove duplicate file statuses that could be in the matchedStatuses
    // because of pagination and implicit directories
    List<FileStatus> filteredStatuses = new ArrayList<>(matchedStatuses.size());
    FileStatus lastAdded = null;
    for (FileStatus fileStatus : matchedStatuses) {
      if (lastAdded == null || lastAdded.compareTo(fileStatus) != 0) {
        filteredStatuses.add(fileStatus);
        lastAdded = fileStatus;
      }
    }

    FileStatus[] returnList = filteredStatuses.toArray(new FileStatus[0]);

    // If the return list contains directories, we should repair them if they're 'implicit'.
    if (enableAutoRepairImplicitDirectories) {
      List<URI> toRepair = new ArrayList<>();
      for (FileStatus status : returnList) {
        if (isImplicitDirectory(status)) {
          toRepair.add(getGcsPath(status.getPath()));
        }
      }
      if (!toRepair.isEmpty()) {
        logger.atWarning().log(
            "Discovered %s implicit directories to repair within return values.", toRepair.size());
        gcsfs.repairDirs(toRepair);
      }
    }

    return returnList;
  }

  private FileStatus[] globInternal(Path fixedPath, PathFilter filter, Path pathPattern)
      throws IOException {
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

  private static boolean isImplicitDirectory(FileStatus curr) {
    // Modification time of 0 indicates implicit directory.
    return curr.isDir() && curr.getModificationTime() == 0;
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
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_ENABLE.getKey(),
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX);
    copyIfNotPresent(
        config,
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_KEY_FILE.getKey(),
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX);
    copyIfNotPresent(
        config,
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_EMAIL.getKey(),
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX);
    copyIfNotPresent(
        config,
        GoogleHadoopFileSystemConfiguration.AUTH_CLIENT_ID.getKey(),
        AUTHENTICATION_PREFIX + HadoopCredentialConfiguration.CLIENT_ID_SUFFIX);
    copyIfNotPresent(
        config,
        GoogleHadoopFileSystemConfiguration.AUTH_CLIENT_SECRET.getKey(),
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

    overrideConfigFromFile(config);

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

      enableFlatGlob = GCS_FLAT_GLOB_ENABLE.get(config, config::getBoolean);
      enableConcurrentGlob = GCS_CONCURRENT_GLOB_ENABLE.get(config, config::getBoolean);
      checksumType = GCS_FILE_CHECKSUM_TYPE.get(config, config::getEnum);

      GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
          GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);

      PathCodec pathCodec;
      String specifiedPathCodec = PATH_CODEC.get(config, config::get).toLowerCase();
      switch (specifiedPathCodec) {
        case PATH_CODEC_USE_LEGACY_ENCODING:
          pathCodec = GoogleCloudStorageFileSystem.LEGACY_PATH_CODEC;
          break;
        case PATH_CODEC_USE_URI_ENCODING:
          pathCodec = GoogleCloudStorageFileSystem.URI_ENCODED_PATH_CODEC;
          break;
        default:
          pathCodec = GoogleCloudStorageFileSystem.LEGACY_PATH_CODEC;
          logger.atWarning().log(
              "Unknown path codec specified %s. Using default / legacy.", specifiedPathCodec);
      }
      optionsBuilder.setPathCodec(pathCodec);

      GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();

      enableAutoRepairImplicitDirectories =
          options.getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled();

      gcsfs = new GoogleCloudStorageFileSystem(credential, options);
    }

    defaultBlockSize = GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.get(config, config::getLong);
    reportedPermissions = new FsPermission(PERMISSIONS_TO_REPORT.get(config, config::get));

    String systemBucketName = GCS_SYSTEM_BUCKET.get(config, config::get);
    boolean createSystemBucket = GCS_CREATE_SYSTEM_BUCKET.get(config, config::getBoolean);
    configureBuckets(systemBucketName, createSystemBucket);

    // Set initial working directory to root so that any configured value gets resolved
    // against file system root.
    workingDirectory = getFileSystemRoot();

    Path newWorkingDirectory;
    String configWorkingDirectory = GCS_WORKING_DIRECTORY.get(config, config::get);
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
    logger.atFine().log("%s = %s", GCS_WORKING_DIRECTORY.getKey(), getWorkingDirectory());

    // Set this configuration as the default config for this instance.
    setConf(config);

    logger.atFine().log("GHFS.configure: done");
  }

  /**
   * If overrides file configured, update properties from override file into {@link Configuration}
   * object
   */
  private void overrideConfigFromFile(Configuration config) throws IOException {
    String configFile =
        GoogleHadoopFileSystemConfiguration.GCS_CONFIG_OVERRIDE_FILE.get(config, config::get);
    if (configFile != null) {
      config.addResource(new FileInputStream(configFile));
    }
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
          throw new FileNotFoundException(
              String.format(
                  "%s: system bucket not found: %s", GCS_SYSTEM_BUCKET.getKey(), systemBucket));
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
  public FileChecksum getFileChecksum(Path hadoopPath) throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");

    checkOpen();

    URI gcsPath = getGcsPath(hadoopPath);
    final FileInfo fileInfo = gcsfs.getFileInfo(gcsPath);
    if (!fileInfo.exists()) {
      logger.atFine().log("GHFS.getFileStatus: not found: %s", gcsPath);
      throw new FileNotFoundException(
          (fileInfo.isDirectory() ? "Directory not found : " : "File not found : ") + hadoopPath);
    }
    FileChecksum checksum = getFileChecksum(checksumType, fileInfo);
    logger.atFine().log("GHFS.getFileChecksum:=> %s", checksum);

    long duration = System.nanoTime() - startTime;
    increment(Counter.GET_FILE_CHECKSUM);
    increment(Counter.GET_FILE_CHECKSUM_TIME, duration);
    return checksum;
  }

  private static FileChecksum getFileChecksum(GcsFileChecksumType type, FileInfo fileInfo)
      throws IOException {
    switch (type) {
      case NONE:
        return null;
      case CRC32C:
        return new GcsFileChecksum(
            type, fileInfo.getItemInfo().getVerificationAttributes().getCrc32c());
      case MD5:
        return new GcsFileChecksum(
            type, fileInfo.getItemInfo().getVerificationAttributes().getMd5hash());
    }
    throw new IOException("Unrecognized GcsFileChecksumType: " + type);
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

  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#PERMISSIONS_TO_REPORT} */
  @Deprecated
  public static final String PERMISSIONS_TO_REPORT_KEY =
      GoogleHadoopFileSystemConfiguration.PERMISSIONS_TO_REPORT.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#PERMISSIONS_TO_REPORT} */
  @Deprecated
  public static final String PERMISSIONS_TO_REPORT_DEFAULT =
      GoogleHadoopFileSystemConfiguration.PERMISSIONS_TO_REPORT.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_BUFFER_SIZE} */
  @Deprecated
  public static final String BUFFERSIZE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_BUFFER_SIZE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_BUFFER_SIZE} */
  @Deprecated
  public static final int BUFFERSIZE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_BUFFER_SIZE.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_OUTPUT_STREAM_BUFFER_SIZE} */
  @Deprecated
  public static final String WRITE_BUFFERSIZE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_OUTPUT_STREAM_BUFFER_SIZE} */
  @Deprecated
  public static final int WRITE_BUFFERSIZE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#BLOCK_SIZE} */
  @Deprecated
  public static final String BLOCK_SIZE_KEY =
      GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#BLOCK_SIZE} */
  @Deprecated
  public static final int BLOCK_SIZE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getDefault().intValue();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#AUTH_SERVICE_ACCOUNT_ENABLE} */
  @Deprecated
  public static final String ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY =
      GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_ENABLE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#AUTH_SERVICE_ACCOUNT_EMAIL} */
  @Deprecated
  public static final String SERVICE_ACCOUNT_AUTH_EMAIL_KEY =
      GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_EMAIL.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#AUTH_SERVICE_ACCOUNT_KEY_FILE} */
  @Deprecated
  public static final String SERVICE_ACCOUNT_AUTH_KEYFILE_KEY =
      GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_KEY_FILE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_PROJECT_ID} */
  @Deprecated
  public static final String GCS_PROJECT_ID_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_REQUESTER_PAYS_MODE} */
  @Deprecated
  public static final String GCS_REQUESTER_PAYS_MODE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_REQUESTER_PAYS_PROJECT_ID} */
  @Deprecated
  public static final String GCS_REQUESTER_PAYS_PROJECT_ID_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_REQUESTER_PAYS_BUCKETS} */
  @Deprecated
  public static final String GCS_REQUESTER_PAYS_BUCKETS_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_BUCKETS.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#AUTH_CLIENT_ID} */
  @Deprecated
  public static final String GCS_CLIENT_ID_KEY =
      GoogleHadoopFileSystemConfiguration.AUTH_CLIENT_ID.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#AUTH_CLIENT_SECRET} */
  @Deprecated
  public static final String GCS_CLIENT_SECRET_KEY =
      GoogleHadoopFileSystemConfiguration.AUTH_CLIENT_SECRET.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_SYSTEM_BUCKET} */
  @Deprecated
  public static final String GCS_SYSTEM_BUCKET_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_SYSTEM_BUCKET.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_CREATE_SYSTEM_BUCKET} */
  @Deprecated
  public static final String GCS_CREATE_SYSTEM_BUCKET_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_CREATE_SYSTEM_BUCKET.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_CREATE_SYSTEM_BUCKET} */
  @Deprecated
  public static final boolean GCS_CREATE_SYSTEM_BUCKET_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_CREATE_SYSTEM_BUCKET.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_WORKING_DIRECTORY} */
  @Deprecated
  public static final String GCS_WORKING_DIRECTORY_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_WORKING_DIRECTORY.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_FILE_SIZE_LIMIT_250GB} */
  @Deprecated
  public static final String GCS_FILE_SIZE_LIMIT_250GB =
      GoogleHadoopFileSystemConfiguration.GCS_FILE_SIZE_LIMIT_250GB.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_FILE_SIZE_LIMIT_250GB} */
  @Deprecated
  public static final boolean GCS_FILE_SIZE_LIMIT_250GB_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_FILE_SIZE_LIMIT_250GB.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_MARKER_FILE_PATTERN} */
  @Deprecated
  public static final String GCS_MARKER_FILE_PATTERN_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_MARKER_FILE_PATTERN.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_PERFORMANCE_CACHE_ENABLE} */
  @Deprecated
  public static final String GCS_ENABLE_PERFORMANCE_CACHE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_PERFORMANCE_CACHE_ENABLE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_PERFORMANCE_CACHE_ENABLE} */
  @Deprecated
  public static final boolean GCS_ENABLE_PERFORMANCE_CACHE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_PERFORMANCE_CACHE_ENABLE.getDefault();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS}
   */
  @Deprecated
  public static final String GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS.getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS}
   */
  @Deprecated
  public static final long GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_PERFORMANCE_CACHE_MAX_ENTRY_AGE_MILLIS.getDefault();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE}
   */
  @Deprecated
  public static final String GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE.getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE}
   */
  @Deprecated
  public static final boolean GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_PERFORMANCE_CACHE_LIST_CACHING_ENABLE.getDefault();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT}
   */
  @Deprecated
  public static final String GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT
          .getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT}
   */
  @Deprecated
  public static final long GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_PERFORMANCE_CACHE_DIR_METADATA_PREFETCH_LIMIT
          .getDefault();
  /**
   * @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_PARENT_TIMESTAMP_UPDATE_ENABLE}
   */
  @Deprecated
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_ENABLE.getKey();
  /**
   * @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_PARENT_TIMESTAMP_UPDATE_ENABLE}
   */
  @Deprecated
  public static final boolean GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_ENABLE.getDefault();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES}
   */
  @Deprecated
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES.getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES}
   */
  @Deprecated
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_DEFAULT =
      Joiner.on(',')
          .join(
              GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES
                  .getDefault());
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#MR_JOB_HISTORY_INTERMEDIATE_DONE_DIR_KEY}
   */
  @Deprecated
  public static final String MR_JOB_HISTORY_INTERMEDIATE_DONE_DIR_KEY =
      GoogleHadoopFileSystemConfiguration.MR_JOB_HISTORY_INTERMEDIATE_DONE_DIR_KEY;
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#MR_JOB_HISTORY_DONE_DIR_KEY} */
  @Deprecated
  public static final String MR_JOB_HISTORY_DONE_DIR_KEY =
      GoogleHadoopFileSystemConfiguration.MR_JOB_HISTORY_DONE_DIR_KEY;
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES}
   */
  @Deprecated
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES}
   */
  @Deprecated
  public static final String GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_DEFAULT =
      Joiner.on(',')
          .join(
              GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES
                  .getDefault());
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE}
   */
  @Deprecated
  public static final String GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE}
   */
  @Deprecated
  public static final boolean GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#PATH_CODEC} */
  @Deprecated
  public static final String PATH_CODEC_KEY =
      GoogleHadoopFileSystemConfiguration.PATH_CODEC.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#PATH_CODEC} */
  @Deprecated
  public static final String PATH_CODEC_DEFAULT =
      GoogleHadoopFileSystemConfiguration.PATH_CODEC.getDefault();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE}
   */
  @Deprecated
  public static final String GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE.getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE}
   */
  @Deprecated
  public static final boolean GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_FLAT_GLOB_ENABLE} */
  @Deprecated
  public static final String GCS_ENABLE_FLAT_GLOB_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_FLAT_GLOB_ENABLE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_FLAT_GLOB_ENABLE} */
  @Deprecated
  public static final boolean GCS_ENABLE_FLAT_GLOB_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_FLAT_GLOB_ENABLE.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_MARKER_FILE_CREATION_ENABLE} */
  @Deprecated
  public static final String GCS_ENABLE_MARKER_FILE_CREATION_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_MARKER_FILE_CREATION_ENABLE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_MARKER_FILE_CREATION_ENABLE} */
  @Deprecated
  public static final boolean GCS_ENABLE_MARKER_FILE_CREATION_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_MARKER_FILE_CREATION_ENABLE.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_COPY_WITH_REWRITE_ENABLE} */
  @Deprecated
  public static final String GCS_ENABLE_COPY_WITH_REWRITE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_COPY_WITH_REWRITE_ENABLE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_COPY_WITH_REWRITE_ENABLE} */
  @Deprecated
  public static final boolean GCS_ENABLE_COPY_WITH_REWRITE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_COPY_WITH_REWRITE_ENABLE.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_COPY_MAX_REQUESTS_PER_BATCH} */
  @Deprecated
  public static final String GCS_COPY_MAX_REQUESTS_PER_BATCH =
      GoogleHadoopFileSystemConfiguration.GCS_COPY_MAX_REQUESTS_PER_BATCH.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_COPY_MAX_REQUESTS_PER_BATCH} */
  @Deprecated
  public static final long GCS_COPY_MAX_REQUESTS_PER_BATCH_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_COPY_MAX_REQUESTS_PER_BATCH.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_COPY_BATCH_THREADS} */
  @Deprecated
  public static final String GCS_COPY_BATCH_THREADS =
      GoogleHadoopFileSystemConfiguration.GCS_COPY_BATCH_THREADS.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_COPY_BATCH_THREADS} */
  @Deprecated
  public static final int GCS_COPY_BATCH_THREADS_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_COPY_BATCH_THREADS.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_MAX_LIST_ITEMS_PER_CALL} */
  @Deprecated
  public static final String GCS_MAX_LIST_ITEMS_PER_CALL =
      GoogleHadoopFileSystemConfiguration.GCS_MAX_LIST_ITEMS_PER_CALL.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_MAX_LIST_ITEMS_PER_CALL} */
  @Deprecated
  public static final long GCS_MAX_LIST_ITEMS_PER_CALL_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_MAX_LIST_ITEMS_PER_CALL.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_MAX_REQUESTS_PER_BATCH} */
  @Deprecated
  public static final String GCS_MAX_REQUESTS_PER_BATCH =
      GoogleHadoopFileSystemConfiguration.GCS_MAX_REQUESTS_PER_BATCH.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_MAX_REQUESTS_PER_BATCH} */
  @Deprecated
  public static final long GCS_MAX_REQUESTS_PER_BATCH_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_MAX_REQUESTS_PER_BATCH.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_BATCH_THREADS} */
  @Deprecated
  public static final String GCS_BATCH_THREADS =
      GoogleHadoopFileSystemConfiguration.GCS_BATCH_THREADS.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_BATCH_THREADS} */
  @Deprecated
  public static final int GCS_BATCH_THREADS_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_BATCH_THREADS.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_HTTP_MAX_RETRY} */
  @Deprecated
  public static final String GCS_HTTP_MAX_RETRY_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_HTTP_MAX_RETRY.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_HTTP_MAX_RETRY} */
  @Deprecated
  public static final int GCS_HTTP_MAX_RETRY_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_HTTP_MAX_RETRY.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_HTTP_CONNECT_TIMEOUT} */
  @Deprecated
  public static final String GCS_HTTP_CONNECT_TIMEOUT_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_HTTP_CONNECT_TIMEOUT.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_HTTP_CONNECT_TIMEOUT} */
  @Deprecated
  public static final int GCS_HTTP_CONNECT_TIMEOUT_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_HTTP_CONNECT_TIMEOUT.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_HTTP_READ_TIMEOUT} */
  @Deprecated
  public static final String GCS_HTTP_READ_TIMEOUT_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_HTTP_READ_TIMEOUT.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_HTTP_READ_TIMEOUT} */
  @Deprecated
  public static final int GCS_HTTP_READ_TIMEOUT_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_HTTP_READ_TIMEOUT.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_PROXY_ADDRESS} */
  @Deprecated
  public static final String GCS_PROXY_ADDRESS_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_PROXY_ADDRESS.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_PROXY_ADDRESS} */
  @Deprecated
  public static final String GCS_PROXY_ADDRESS_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_PROXY_ADDRESS.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_HTTP_TRANSPORT} */
  @Deprecated
  public static final String GCS_HTTP_TRANSPORT_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_HTTP_TRANSPORT.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_HTTP_TRANSPORT} */
  @Deprecated public static final String GCS_HTTP_TRANSPORT_DEFAULT = null;
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_APPLICATION_NAME_SUFFIX} */
  @Deprecated
  public static final String GCS_APPLICATION_NAME_SUFFIX_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_APPLICATION_NAME_SUFFIX.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_APPLICATION_NAME_SUFFIX} */
  @Deprecated
  public static final String GCS_APPLICATION_NAME_SUFFIX_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_APPLICATION_NAME_SUFFIX.getDefault();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE}
   */
  @Deprecated
  public static final String GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE.getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE}
   */
  @Deprecated
  public static final int GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_MAX_WAIT_MILLIS_EMPTY_OBJECT_CREATE.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_OUTPUT_STREAM_TYPE} */
  @Deprecated
  public static final String GCS_OUTPUTSTREAM_TYPE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_TYPE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_OUTPUT_STREAM_TYPE} */
  @Deprecated
  public static final String GCS_OUTPUTSTREAM_TYPE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_TYPE.getDefault().toString();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_GENERATION_READ_CONSISTENCY} */
  @Deprecated
  public static final String GCS_GENERATION_READ_CONSISTENCY_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_GENERATION_READ_CONSISTENCY.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_GENERATION_READ_CONSISTENCY} */
  @Deprecated
  public static final GenerationReadConsistency GCS_GENERATION_READ_CONSISTENCY_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_GENERATION_READ_CONSISTENCY.getDefault();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE}
   */
  @Deprecated
  public static final String GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE.getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE}
   */
  @Deprecated
  public static final boolean GCS_INPUTSTREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FAST_FAIL_ON_NOT_FOUND_ENABLE
          .getDefault();
  /**
   * @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT}
   */
  @Deprecated
  public static final String GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.getKey();
  /**
   * @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT}
   */
  @Deprecated
  public static final long GCS_INPUTSTREAM_INPLACE_SEEK_LIMIT_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_FADVISE} */
  @Deprecated
  public static final String GCS_INPUTSTREAM_FADVISE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_FADVISE} */
  @Deprecated
  public static final Fadvise GCS_INPUTSTREAM_FADVISE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getDefault();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE}
   */
  @Deprecated
  public static final String GCS_INPUTSTREAM_MIN_RANGE_REQUEST_SIZE_KEY =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.getKey();
  /**
   * @deprecated use {@link
   *     GoogleHadoopFileSystemConfiguration#GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE}
   */
  @Deprecated
  public static final int GCS_INPUTSTREAM_MIN_RANGE_REQUEST_SIZE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.getDefault();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCE_BUCKET_DELETE_ENABLE} */
  @Deprecated
  public static final String GCE_BUCKET_DELETE_ENABLE_KEY =
      GoogleHadoopFileSystemConfiguration.GCE_BUCKET_DELETE_ENABLE.getKey();
  /** @deprecated use {@link GoogleHadoopFileSystemConfiguration#GCE_BUCKET_DELETE_ENABLE} */
  @Deprecated
  public static final boolean GCE_BUCKET_DELETE_ENABLE_DEFAULT =
      GoogleHadoopFileSystemConfiguration.GCE_BUCKET_DELETE_ENABLE.getDefault();
}
