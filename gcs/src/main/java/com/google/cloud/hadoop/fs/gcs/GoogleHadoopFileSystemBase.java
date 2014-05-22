/**
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

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.cloud.hadoop.util.HadoopVersionInfo;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class provides a Hadoop compatible File System on top of Google Cloud Storage (GCS).
 *
 * It is implemented as a thin abstraction layer on top of GCS.
 * The layer hides any specific characteristics of the underlying store and exposes FileSystem
 * interface understood by the Hadoop engine.
 * <p>
 * Users interact with the files in the storage using fully qualified URIs.
 * The file system exposed by this class is identified using the 'gs' scheme.
 * For example, {@code gs://dir1/dir2/file1.txt}.
 * <p>
 * This implementation translates paths between hadoop Path and GCS URI with the convention that
 * the Hadoop root directly corresponds to the GCS "root", e.g. gs:/. This is convenient for many
 * reasons, such as data portability and close equivalence to gsutil paths, but imposes certain
 * inherited constraints, such as files not being allowed in root (only 'directories' can be placed
 * in root), and directory names inside root have a more limited set of allowed characters.
 * <p>
 * One of the main goals of this implementation is to maintain compatibility
 * with behavior of HDFS implementation when accessed through FileSystem interface.
 * HDFS implementation is not very consistent about the cases when it throws versus
 * the cases when methods return false. We run GHFS tests and HDFS tests against the
 * same test data and use that as a guide to decide whether to throw or to
 * return false.
 */
public abstract class GoogleHadoopFileSystemBase
    extends FileSystem implements FileSystemDescriptor {
  // Logger.
  public static final LogUtil log = new LogUtil(GoogleHadoopFileSystemBase.class);

  // Current version.
  public static final String VERSION = "1.2.6-SNAPSHOT";

  // Identifies this version of the GoogleHadoopFileSystemBase library.
  public static final String GHFS_ID = String.format("GHFS/%s", VERSION);

  // Default value of replication factor.
  public static final short REPLICATION_FACTOR_DEFAULT = 3;

  // Permissions that we report a file or directory to have.
  // Note:
  // We do not really support file/dir permissions but we need to
  // report some permission value when Hadoop calls getFileStatus().
  // A MapReduce job fails if we report permissions more relaxed than
  // the value below.
  private static final FsPermission PERMISSIONS_TO_REPORT = FsPermission.valueOf("-rwx------");

  // We report this value as a file's owner/group name.
  private static final String USER_NAME = System.getProperty("user.name");

  // -----------------------------------------------------------------
  // Configuration settings.

  // Configuration key for setting IO buffer size.
  // TODO(user): rename the following to indicate that it is read buffer size.
  public static final String BUFFERSIZE_KEY = "fs.gs.io.buffersize";

  // Hadoop passes 4096 bytes as buffer size which causes poor perf.
  // Default value of fs.gs.io.buffersize.
  public static final int BUFFERSIZE_DEFAULT = 8 * 1024 * 1024;

  // Configuration key for setting write buffer size.
  public static final String WRITE_BUFFERSIZE_KEY = "fs.gs.io.buffersize.write";

  // Default value of fs.gs.io.buffersize.write.
  // chunk size etc. Get the following value from GCSWC class in a better way. For now, we hard code
  // it to a known good value.
  public static final int WRITE_BUFFERSIZE_DEFAULT = 64 * 1024 * 1024;

  // Configuration key for default block size of a file.
  public static final String BLOCK_SIZE_KEY = "fs.gs.block.size";

  // Default value of fs.gs.block.size.
  public static final int BLOCK_SIZE_DEFAULT = 64 * 1024 * 1024;

  // Prefix to use for common authentication keys
  public static final String AUTHENTICATION_PREFIX = "fs.gs";

  // Configuration key for enabling GCE service account authentication.
  // This key is deprecated. See HadoopCredentialConfiguration for current key names.
  public static final String ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY  =
      "fs.gs.enable.service.account.auth";

  // Configuration key specifying the email address of the service-account with which to
  // authenticate. Only required if fs.gs.enable.service.account.auth is true AND we're using
  // fs.gs.service.account.auth.keyfile to authenticate with a private keyfile.
  // NB: Once GCE supports setting multiple service account email addresses for metadata auth,
  // this key will also be used in the metadata auth flow.
  // This key is deprecated. See HadoopCredentialConfiguration for current key names.
  public static final String SERVICE_ACCOUNT_AUTH_EMAIL_KEY  =
      "fs.gs.service.account.auth.email";

  // Configuration key specifying local file containing a service-account private .p12 keyfile.
  // Only used if fs.gs.enable.service.account.auth is true; if provided, the keyfile will be used
  // for service-account authentication. Otherwise, it is assumed that we are on a GCE VM with
  // metadata-authentication for service-accounts enabled, and the metadata server will be used
  // instead.
  // Default value: none
  // This key is deprecated. See HadoopCredentialConfiguration for current key names.
  public static final String SERVICE_ACCOUNT_AUTH_KEYFILE_KEY  =
      "fs.gs.service.account.auth.keyfile";

  // Configuration key for GCS project ID.
  // Default value: none
  public static final String GCS_PROJECT_ID_KEY  = "fs.gs.project.id";

  // Configuration key for GCS client ID.
  // Required if fs.gs.enable.service.account.auth == false.
  // Default value: none
  // This key is deprecated. See HadoopCredentialConfiguration for current key names.
  public static final String GCS_CLIENT_ID_KEY = "fs.gs.client.id";

  // Configuration key for GCS client secret.
  // Required if fs.gs.enable.service.account.auth == false.
  // Default value: none
  // This key is deprecated. See HadoopCredentialConfiguration for current key names.
  public static final String GCS_CLIENT_SECRET_KEY = "fs.gs.client.secret";

  // Configuration key for system bucket name. If the FileSystem is global-rooted, the bucket
  // is equivalent to the top-level directory where the Hadoop "home directory" resides.
  // If bucket-rooted, the bucket specified here *is* the root, and all paths are defined relative
  // to this bucket.
  // Default value: none
  public static final String GCS_SYSTEM_BUCKET_KEY = "fs.gs.system.bucket";

  // Configuration key for flag to indicate whether system bucket should be created
  // if it does not exist.
  public static final String GCS_CREATE_SYSTEM_BUCKET_KEY = "fs.gs.system.bucket.create";

  // Default value of fs.gs.system.bucket.create.
  public static final boolean GCS_CREATE_SYSTEM_BUCKET_DEFAULT = true;

  // Configuration key for initial working directory of a GHFS instance.
  // Default value: getHadoopPath(systemBucket, "")
  public static final String GCS_WORKING_DIRECTORY_KEY = "fs.gs.working.dir";

  // Configuration key for setting 250GB upper limit on file size to gain higher write throughput.
  public static final String GCS_FILE_SIZE_LIMIT_250GB = "fs.gs.file.size.limit.250gb";

  // Default value of fs.gs.file.size.limit.250gb.
  public static final boolean GCS_FILE_SIZE_LIMIT_250GB_DEFAULT = true;

  // Configuration key for using a local metadata cache to supplement GCS API "list" results;
  // this allows same-client create() to immediately be visible to a subsequent list() call.
  public static final String GCS_ENABLE_METADATA_CACHE_KEY = "fs.gs.metadata.cache.enable";

  // Default value for fs.gs.metadata.cache.enable.
  public static final boolean GCS_ENABLE_METADATA_CACHE_DEFAULT = true;

  // Configuration key for enabling automatic repair of implicit directories whenever detected
  // inside listStatus and globStatus calls, or other methods which may indirectly call listStatus
  // and/or globaStatus.
  public static final String GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY =
      "fs.gs.implicit.dir.repair.enable";

  // Default value for fs.gs.implicit.dir.repair.enable.
  public static final boolean GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT = true;

  // Instance value of fs.gs.implicit.dir.repair.enable based on the initial Configuration.
  private boolean enableAutoRepairImplicitDirectories =
      GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT;

  // Configuration key for enabling the use of a large flat listing to pre-populate possible
  // glob matches in a single API call before running the core globbing logic in-memory rather
  // than sequentially and recursively performing API calls.
  public static final String GCS_ENABLE_FLAT_GLOB_KEY = "fs.gs.glob.flatlist.enable";

  // Default value for fs.gs.glob.flatlist.enable.
  public static final boolean GCS_ENABLE_FLAT_GLOB_DEFAULT = true;

  // Default PathFilter that accepts all paths.
  public static final PathFilter DEFAULT_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return true;
    }
  };

  // Instance value of fs.gs.glob.flatlist.enable based on the initial Configuration.
  private boolean enableFlatGlob = GCS_ENABLE_FLAT_GLOB_DEFAULT;

  //The URI the File System is passed in initialize.
  protected URI initUri;

  // The retrieved configuration value for fs.gs.system.bucket, used for default values of:
  // -- working directory
  // -- user home directories (only for Hadoop purposes).
  //    This is not the same as a user's OS home directory.
  protected String systemBucket;

  // Underlying GCS file system object.
  protected GoogleCloudStorageFileSystem gcsfs;

  // Current working directory; overridden in initialize() if fs.gs.working.dir is set.
  private Path workingDirectory;

  // Buffer size to use instead of what Hadoop passed.
  private int bufferSizeOverride = BUFFERSIZE_DEFAULT;

  // Default block size.
  // Note that this is the size that is reported to Hadoop FS clients.
  // It does not modify the actual block size of an underlying GCS object,
  // because GCS JSON API does not allow modifying or querying the value.
  // Modifying this value allows one to control how many mappers are used
  // to process a given file.
  protected long defaultBlockSize = BLOCK_SIZE_DEFAULT;

  // Map of counter values
  protected final ImmutableMap<Counter, AtomicLong> counters = createCounterMap();

  protected ImmutableMap<Counter, AtomicLong> createCounterMap() {
    ImmutableMap.Builder<Counter, AtomicLong> builder = ImmutableMap.builder();
    for (Counter counter : Counter.values()) {
      builder.put(counter, new AtomicLong());
    }
    return builder.build();
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
      if (hadoopVersionInfo.isGreaterThan(2, 0) || hadoopVersionInfo.isEqualTo(0, 23)) {
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
  public abstract String getHadoopScheme();

  @Override
  protected void checkPath(Path path) {
    URI uri = path.toUri();
    String scheme = uri.getScheme();
    // Only check that the scheme matches. The authority and path will be
    // validated later.
    if (scheme == null || scheme.equalsIgnoreCase(getHadoopScheme())) {
      return;
    } else {
      String msg = String.format(
          "Wrong FS scheme: %s, in path: %s, expected scheme: %s",
          scheme, path, getHadoopScheme());
      throw new IllegalArgumentException(msg);
    }
  }

  /**
   * See {@link initialize(URI, Configuration, boolean)} for details; calls with third arg
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
   * @param initSuperClass if false, doesn't call super.initialize(path, config); avoids
   *     registering a global Statistics object for this instance.
   */
  public void initialize(URI path, Configuration config, boolean initSuperclass)
      throws IOException {
    long startTime = System.nanoTime();
    Preconditions.checkArgument(path != null, "path must not be null");
    Preconditions.checkArgument(config != null, "config must not be null");
    Preconditions.checkArgument(path.getScheme() != null, "scheme of path must not be null");
    if (!path.getScheme().equals(getHadoopScheme())) {
      throw new IllegalArgumentException("URI scheme not supported: " + path);
    }
    initUri = path;
    log.debug("GHFS.initialize: %s", path);

    if (initSuperclass) {
      super.initialize(path, config);
    } else {
      log.debug(
          "Initializing 'statistics' as an instance not attached to the static FileSystem map");
      // Provide an ephemeral Statistics object to avoid NPE, but still avoid registering a global
      // statistics object.
      statistics = new Statistics(getHadoopScheme());
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
    log.debug("GHFS.getDefaultPort:");
    int result = -1;
    log.debug("GHFS.getDefaultPort:=> %d", result);
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
   * Note:
   * This function overrides the given bufferSize value with a higher
   * number unless further overridden using configuration
   * parameter fs.gs.io.buffersize.
   *
   * @param hadoopPath File to open.
   * @param bufferSize Size of buffer to use for IO.
   * @return A readable stream.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if an error occurs.
   */
  @Override
  public FSDataInputStream open(Path hadoopPath, int bufferSize)
      throws IOException {

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null,
        "hadoopPath must not be null");
    Preconditions.checkArgument(bufferSize > 0,
        "bufferSize must be a positive integer: %s", bufferSize);

    checkOpen();

    log.debug("GHFS.open: %s, bufferSize: %d (override: %d)",
        hadoopPath, bufferSize, bufferSizeOverride);
    bufferSize = bufferSizeOverride;
    URI gcsPath = getGcsPath(hadoopPath);
    GoogleHadoopFSInputStream in =
        new GoogleHadoopFSInputStream(this, gcsPath, bufferSize, statistics);

    long duration = System.nanoTime() - startTime;
    increment(Counter.OPEN);
    increment(Counter.OPEN_TIME, duration);
    return new FSDataInputStream(in);
  }

  /**
   * Opens the given file for writing.
   *
   * Note:
   * This function overrides the given bufferSize value with a higher
   * number unless further overridden using configuration
   * parameter fs.gs.io.buffersize.
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
    Preconditions.checkArgument(bufferSize > 0,
        "bufferSize must be a positive integer: %s", bufferSize);
    Preconditions.checkArgument(replication > 0,
        "replication must be a positive integer: %s", replication);
    Preconditions.checkArgument(blockSize > 0,
        "blockSize must be a positive integer: %s", blockSize);

    checkOpen();

    log.debug("GHFS.create: %s, overwrite: %s, bufferSize: %d (override: %d)",
        hadoopPath, overwrite, bufferSize, bufferSizeOverride);
    bufferSize = bufferSizeOverride;

    URI gcsPath = getGcsPath(hadoopPath);

    GoogleHadoopOutputStream out = new GoogleHadoopOutputStream(
        this,
        gcsPath,
        bufferSize,
        statistics,
        new CreateFileOptions(overwrite));

    long duration = System.nanoTime() - startTime;
    increment(Counter.CREATE);
    increment(Counter.CREATE_TIME, duration);
    return new FSDataOutputStream(out);
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
  public FSDataOutputStream append(
      Path hadoopPath,
      int bufferSize,
      Progressable progress) throws IOException {

    long startTime = System.nanoTime();
    Preconditions.checkArgument(hadoopPath != null, "hadoopPath must not be null");
    Preconditions.checkArgument(bufferSize > 0,
        "bufferSize must be a positive integer: %s", bufferSize);

    log.debug("GHFS.append: %s, bufferSize: %d (override: %d)",
        hadoopPath, bufferSize, bufferSizeOverride);
    bufferSize = bufferSizeOverride;

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
      log.debug("GHFS.rename: src is root: '%s'", src);
      return false;
    }

    long startTime = System.nanoTime();
    Preconditions.checkArgument(src != null, "src must not be null");
    Preconditions.checkArgument(dst != null, "dst must not be null");

    checkOpen();

    try {
      log.debug("GHFS.rename: %s -> %s", src, dst);

      URI srcPath = getGcsPath(src);
      URI dstPath = getGcsPath(dst);
      gcsfs.rename(srcPath, dstPath);
    } catch (IOException e) {
      log.debug("GHFS.rename", e);
      return false;
    }

    long duration = System.nanoTime() - startTime;
    increment(Counter.RENAME);
    increment(Counter.RENAME_TIME, duration);
    return true;
  }

  /** Delete a file. */
  /** @deprecated Use {@code delete(Path, boolean)} instead */
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

    log.debug("GHFS.delete: %s, recursive: %s", hadoopPath, recursive);
    URI gcsPath = getGcsPath(hadoopPath);
    try {
      gcsfs.delete(gcsPath, recursive);
    } catch (DirectoryNotEmptyException e) {
      throw e;
    } catch (IOException e) {
      log.debug("GHFS.delete", e);
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

    log.debug("GHFS.listStatus: %s", hadoopPath);

    URI gcsPath = getGcsPath(hadoopPath);
    List<FileStatus> status = new ArrayList<>();

    try {
      List<FileInfo> fileInfos = gcsfs.listFileInfo(
          gcsPath, enableAutoRepairImplicitDirectories);
      for (FileInfo fileInfo : fileInfos) {
        status.add(getFileStatus(fileInfo));
      }
    } catch (FileNotFoundException fnfe) {
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

    log.debug("GHFS.setWorkingDirectory: %s", hadoopPath);
    URI gcsPath = getGcsPath(hadoopPath);
    gcsPath = FileInfo.convertToDirectoryPath(gcsPath);
    Path newPath = getHadoopPath(gcsPath);

    // Ideally we should check (as we did earlier) if the given path really points to an existing
    // directory. However, it takes considerable amount of time for that check which hurts perf.
    // Given that HDFS code does not do such checks either, we choose to not do them in favor of
    // better performance.

    workingDirectory = newPath;
    log.debug("GHFS.setWorkingDirectory: => %s", workingDirectory);

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
    log.debug("GHFS.getWorkingDirectory: %s", workingDirectory);
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

    log.debug("GHFS.mkdirs: %s, perm: %s", hadoopPath, permission);
    URI gcsPath = getGcsPath(hadoopPath);
    gcsfs.mkdirs(gcsPath);

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

    log.debug("GHFS.getFileStatus: %s", hadoopPath);
    URI gcsPath = getGcsPath(hadoopPath);
    FileInfo fileInfo = gcsfs.getFileInfo(gcsPath);
    if (!fileInfo.exists()) {
      log.debug("GHFS.getFileStatus: not found: %s", gcsPath);
      String msg = fileInfo.isDirectory() ? "Directory not found : " : "File not found : ";
      msg += hadoopPath.toString();
      throw new FileNotFoundException(msg);
    }
    FileStatus status = getFileStatus(fileInfo);

    long duration = System.nanoTime() - startTime;
    increment(Counter.GET_FILE_STATUS);
    increment(Counter.GET_FILE_STATUS_TIME, duration);
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
      log.debug(
          "Flat glob is on, but doesn't work for scheme '%s'; usig default behavior.",
          getUri().getScheme());
      return false;
    }

    // The full pattern should have a wildcard, otherwise there's no point doing the flat glob.
    GlobPattern fullPattern = new GlobPattern(fixedPath.toString());
    if (!fullPattern.hasWildcard()) {
      log.debug(
          "Flat glob is on, but Path '%s' has no wildcard; using default behavior.",
          fixedPath);
      return false;
    }

    // To use a flat glob, there must be an authority defined.
    if (Strings.isNullOrEmpty(fixedPath.toUri().getAuthority())) {
      log.info(
          "Flat glob is on, but Path '%s' has a empty authority, using default behavior.",
          fixedPath);
      return false;
    }

    // And the authority must not contain a wildcard.
    GlobPattern authorityPattern = new GlobPattern(fixedPath.toUri().getAuthority());
    if (authorityPattern.hasWildcard()) {
      log.info(
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

    // Find the first occurence of any one of the wildcard characters, or just path.length()
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
   * Returns an array of FileStatus objects whose path names match pathPattern
   * and is accepted by the user-supplied path filter. Results are sorted by
   * their path names.
   *
   * Return null if pathPattern has no glob and the path does not exist.
   * Return an empty array if pathPattern has a glob and no path matches it.
   *
   * @param pathPattern A regular expression specifying the path pattern.
   * @param filter A user-supplied path filter.
   * @return An array of FileStatus objects.
   * @throws IOException if an error occurs.
   */
  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {

    checkOpen();

    log.debug("GHFS.globStatus: %s", pathPattern);
    // We convert pathPattern to GCS path and then to Hadoop path to ensure that it ends up in
    // the correct format. See note in getHadoopPath for more information.
    Path fixedPath = getHadoopPath(getGcsPath(pathPattern));
    log.debug("GHFS.globStatus fixedPath: %s => %s", pathPattern, fixedPath);

    if (shouldUseFlatGlob(fixedPath)) {
      String pathString = fixedPath.toString();
      String prefixString = trimToPrefixWithoutGlob(pathString);
      Path prefixPath = new Path(prefixString);

      // Get everything matching the non-glob prefix.
      log.debug("Listing everything with prefix '%s'", prefixPath);
      List<FileInfo> fileInfos = gcsfs.listAllFileInfoForPrefix(getGcsPath(prefixPath));
      if (fileInfos.isEmpty()) {
        // Let the superclass define the proper logic for finding no matches.
        return super.globStatus(fixedPath, filter);
      }

      // Perform the core globbing logic in the helper filesystem.
      GoogleHadoopFileSystem helperFileSystem =
          ListHelperGoogleHadoopFileSystem.createInstance(fileInfos);
      FileStatus[] returnList = helperFileSystem.globStatus(pathPattern, filter);

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
          log.warn("Discovered %d implicit directories to repair within return values.",
                   toRepair.size());
          gcsfs.repairDirs(toRepair);
        }
      }
      return returnList;
    } else {
      FileStatus[] ret = super.globStatus(fixedPath, filter);
      if (ret == null) {
        if (enableAutoRepairImplicitDirectories) {
          log.debug("GHFS.globStatus returned null for '%s', attempting possible repair.",
                    pathPattern);
          if (gcsfs.repairPossibleImplicitDirectory(getGcsPath(fixedPath))) {
            log.warn("Success repairing '%s', re-globbing.", pathPattern);
            ret = super.globStatus(fixedPath, filter);
          }
        }
      }
      return ret;
    }
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
    log.debug("GHFS.getHomeDirectory:=> %s", result);
    return result;
  }

  /**
   * Gets FileStatus corresponding to the given FileInfo value.
   */
  private FileStatus getFileStatus(FileInfo fileInfo) {
    // GCS does not provide modification time. It only provides creation time.
    // It works for objects because they are immutable once created.
    FileStatus status =
        new FileStatus(
            fileInfo.getSize(),
            fileInfo.isDirectory(),
            REPLICATION_FACTOR_DEFAULT,
            defaultBlockSize,
            fileInfo.getCreationTime(),
            fileInfo.getCreationTime(),
            PERMISSIONS_TO_REPORT,
            USER_NAME,
            USER_NAME,
            getHadoopPath(fileInfo.getPath()));
    log.debug("GHFS.getFileStatus: %s => %s", fileInfo.getPath(), fileStatusToString(status));
    return status;
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
   * Gets buffer size that overrides the default value.
   */
  @VisibleForTesting
  int getBufferSizeOverride() {
    return bufferSizeOverride;
  }

  /**
   * Gets system bucket name.
   */
  @VisibleForTesting
  String getSystemBucketName() {
    return systemBucket;
  }


  /**
   * {@inheritDoc}
   *
   * Returns null, because GHFS does not use security tokens.
   */
  @Override
  public String getCanonicalServiceName() {
    log.debug("GHFS.getCanonicalServiceName:");
    log.debug("GHFS.getCanonicalServiceName:=> null");
    return null;
  }



  /**
   * Gets GCS FS instance.
   */
  GoogleCloudStorageFileSystem getGcsFs() {
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
    log.debug(countersToString());
  }

  /**
   * Copy the value of the deprecated key to the new key if a value is present for the deprecated
   * key, but not the new key.
   */
  private static void copyIfNotPresent(Configuration config, String deprecatedKey, String newKey) {
    String deprecatedValue = config.get(deprecatedKey);
    if (config.get(newKey) == null && deprecatedValue != null) {
      log.warn(
          "Key %s is deprecated. Copying the value of key %s to new key %s",
          deprecatedKey,
          deprecatedKey,
          newKey);
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
   * Configures GHFS using the supplied configuration.
   *
   * @param config Hadoop configuration object.
   */
  private synchronized void configure(Configuration config)
      throws IOException {
    log.debug("GHFS.configure");
    log.debug("GHFS_ID = %s", GHFS_ID);

    if (gcsfs == null) {

      copyDeprecatedConfigurationOptions(config);

      String projectId;
      Credential credential;
      try {
         credential = HadoopCredentialConfiguration
            .newBuilder()
            .withConfiguration(config)
            .withOverridePrefix(AUTHENTICATION_PREFIX)
            .build()
            .getCredential(CredentialFactory.GCS_SCOPES);
      } catch (GeneralSecurityException gse) {
        throw new IOException(gse);
      }

      GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
          GoogleCloudStorageFileSystemOptions.newBuilder();

      boolean enableMetadataCache = config.getBoolean(
          GCS_ENABLE_METADATA_CACHE_KEY,
          GCS_ENABLE_METADATA_CACHE_DEFAULT);
      log.debug("%s = %s", GCS_ENABLE_METADATA_CACHE_KEY, enableMetadataCache);
      optionsBuilder.setIsMetadataCacheEnabled(enableMetadataCache);

      enableAutoRepairImplicitDirectories = config.getBoolean(
          GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY,
          GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_DEFAULT);
      log.debug("%s = %s", GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY,
                enableAutoRepairImplicitDirectories);

      enableFlatGlob = config.getBoolean(
          GCS_ENABLE_FLAT_GLOB_KEY,
          GCS_ENABLE_FLAT_GLOB_DEFAULT);
      log.debug("%s = %s", GCS_ENABLE_FLAT_GLOB_KEY, enableFlatGlob);

      optionsBuilder
          .getCloudStorageOptionsBuilder()
          .setAutoRepairImplicitDirectoriesEnabled(enableAutoRepairImplicitDirectories);

      projectId = ConfigurationUtil.getMandatoryConfig(config, GCS_PROJECT_ID_KEY);

      optionsBuilder.getCloudStorageOptionsBuilder().setProjectId(projectId);

      // Configuration for setting 250GB upper limit on file size to gain higher write throughput.
      boolean limitFileSizeTo250Gb =
          config.getBoolean(GCS_FILE_SIZE_LIMIT_250GB, GCS_FILE_SIZE_LIMIT_250GB_DEFAULT);

      optionsBuilder
          .getCloudStorageOptionsBuilder()
          .getWriteChannelOptionsBuilder()
          .setFileSizeLimitedTo250Gb(limitFileSizeTo250Gb);

      // Configuration for setting GoogleCloudStorageWriteChannel upload buffer size.
      int uploadBufferSize = config.getInt(WRITE_BUFFERSIZE_KEY, WRITE_BUFFERSIZE_DEFAULT);
      log.debug("%s = %d", WRITE_BUFFERSIZE_KEY, uploadBufferSize);

      optionsBuilder.
          getCloudStorageOptionsBuilder().
          getWriteChannelOptionsBuilder().
          setUploadBufferSize(uploadBufferSize);

      optionsBuilder
          .getCloudStorageOptionsBuilder()
          .setAppName(GHFS_ID);

      gcsfs = new GoogleCloudStorageFileSystem(credential, optionsBuilder.build());
    }

    bufferSizeOverride = config.getInt(BUFFERSIZE_KEY, BUFFERSIZE_DEFAULT);
    log.debug("%s = %d", BUFFERSIZE_KEY, bufferSizeOverride);

    defaultBlockSize = config.getLong(BLOCK_SIZE_KEY, BLOCK_SIZE_DEFAULT);
    log.debug("%s = %d", BLOCK_SIZE_KEY, defaultBlockSize);

    String systemBucketName = ConfigurationUtil.getMandatoryConfig(config, GCS_SYSTEM_BUCKET_KEY);
    log.debug("%s = %s", GCS_SYSTEM_BUCKET_KEY, systemBucketName);

    boolean createSystemBucket =
        config.getBoolean(GCS_CREATE_SYSTEM_BUCKET_KEY, GCS_CREATE_SYSTEM_BUCKET_DEFAULT);
    log.debug("%s = %s", GCS_CREATE_SYSTEM_BUCKET_KEY, createSystemBucket);

    configureBuckets(systemBucketName, createSystemBucket);

    // Set initial working directory to root so that any configured value gets resolved
    // against file system root.
    workingDirectory = getFileSystemRoot();

    Path newWorkingDirectory;
    String configWorkingDirectory = config.get(GCS_WORKING_DIRECTORY_KEY);
    if (Strings.isNullOrEmpty(configWorkingDirectory)) {
      newWorkingDirectory = getDefaultWorkingDirectory();
      log.warn(
          "No working directory configured, using default: '%s'", newWorkingDirectory);
    } else {
      newWorkingDirectory = new Path(configWorkingDirectory);
    }

    // Use the public method to ensure proper behavior of normalizing and resolving the new
    // working directory relative to the initial filesystem-root directory.
    setWorkingDirectory(newWorkingDirectory);
    log.debug("%s = %s", GCS_WORKING_DIRECTORY_KEY, getWorkingDirectory());

    // Set this configuration as the default config for this instance.
    setConf(config);

    log.debug("GHFS.configure: done");
  }

  /**
   * Validates and possibly creates the system bucket. Should be overridden to configure other
   * buckets.
   *
   * @param systemBucketName Name of system bucket
   * @param createSystemBucket Whether or not to create systemBucketName if it does not exist.
   * @throws IOException if systemBucketName is invalid or cannot be found.
   *     and createSystemBucket is false.
   */
  @VisibleForTesting
  // TODO(user): Refactor to make protected
  public void configureBuckets(String systemBucketName, boolean createSystemBucket)
      throws IOException {

    log.debug("GHFS.configureBuckets: %s, %s", systemBucketName, createSystemBucket);

    systemBucket = systemBucketName;

    // Ensure that system bucket exists. It really must be a bucket, not a GCS path.
    URI systemBucketPath = GoogleCloudStorageFileSystem.getPath(systemBucket);

    checkOpen();

    if (!gcsfs.exists(systemBucketPath)) {
      if (createSystemBucket) {
        gcsfs.mkdirs(systemBucketPath);
      } else {
        String msg = String.format("%s: system bucket not found: %s",
            GCS_SYSTEM_BUCKET_KEY, systemBucket);
        throw new FileNotFoundException(msg);
      }
    }

    log.debug("GHFS.configureBuckets:=>");
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

    log.debug("GHFS.deleteOnExit: %s", f);
    boolean result = super.deleteOnExit(f);
    log.debug("GHFS.deleteOnExit:=> %s", result);
    return result;
  }

  @Override
  protected void processDeleteOnExit() {
    log.debug("GHFS.processDeleteOnExit:");
    super.processDeleteOnExit();
  }

  @Override
  public ContentSummary getContentSummary(Path f)
      throws IOException {
    log.debug("GHFS.getContentSummary: %s", f);
    ContentSummary result = super.getContentSummary(f);
    log.debug("GHFS.getContentSummary:=> %s", result);
    return result;
  }

  @Override
  public Token<?> getDelegationToken(String renewer)
      throws IOException {
    log.debug("GHFS.getDelegationToken: renewer: %s", renewer);
    Token<?> result = super.getDelegationToken(renewer);
    log.debug("GHFS.getDelegationToken:=> %s", result);
    return result;
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
      Path[] srcs, Path dst)
      throws IOException {
    log.debug("GHFS.copyFromLocalFile: delSrc: %s, overwrite: %s, #srcs: %d, dst: %s",
        delSrc, overwrite, srcs.length, dst);
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    log.debug("GHFS.copyFromLocalFile:=> ");
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
      Path src, Path dst)
      throws IOException {
    log.debug("GHFS.copyFromLocalFile: delSrc: %s, overwrite: %s, src: %s, dst: %s",
        delSrc, overwrite, src, dst);
    super.copyFromLocalFile(delSrc, overwrite, src, dst);
    log.debug("GHFS.copyFromLocalFile:=> ");
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
      throws IOException {
    log.debug("GHFS.copyToLocalFile: delSrc: %s, src: %s, dst: %s", delSrc, src, dst);
    super.copyToLocalFile(delSrc, src, dst);
    log.debug("GHFS.copyToLocalFile:=> ");
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    log.debug("GHFS.startLocalOutput: out: %s, tmp: %s", fsOutputFile, tmpLocalFile);
    Path result = super.startLocalOutput(fsOutputFile, tmpLocalFile);
    log.debug("GHFS.startLocalOutput:=> %s", result);
    return result;
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    log.debug("GHFS.startLocalOutput: out: %s, tmp: %s", fsOutputFile, tmpLocalFile);
    super.completeLocalOutput(fsOutputFile, tmpLocalFile);
    log.debug("GHFS.completeLocalOutput:=> ");
  }

  @Override
  public void close()
      throws IOException {
    log.debug("GHFS.close:");
    super.close();

    // NB: We must *first* have the superclass close() before we close the underlying gcsfs since
    // the superclass may decide to perform various heavyweight cleanup operations (such as
    // deleteOnExit).
    if (gcsfs != null) {
      gcsfs.close();
      gcsfs = null;
    }
    logCounters();
    log.debug("GHFS.close:=> ");
  }

  @Override
  public long getUsed()
      throws IOException{
    log.debug("GHFS.getUsed:");
    long result = super.getUsed();
    log.debug("GHFS.getUsed:=> %d", result);
    return result;
  }

  @Override
  public long getDefaultBlockSize() {
    log.debug("GHFS.getDefaultBlockSize:");
    long result = defaultBlockSize;
    log.debug("GHFS.getDefaultBlockSize:=> %s", result);
    return result;
  }

  @Override
  public FileChecksum getFileChecksum(Path f)
      throws IOException {
    log.debug("GHFS.getFileChecksum:");
    FileChecksum result = super.getFileChecksum(f);
    log.debug("GHFS.getFileChecksum:=> %s", result);
    return result;
  }

  @Override
  public Path makeQualified(Path path) {
    log.debug("GHFS.makeQualified:");
    Path result = super.makeQualified(path);
    log.debug("GHFS.makeQualified:=> %s", result);
    return result;
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    log.debug("GHFS.setVerifyChecksum:");
    super.setVerifyChecksum(verifyChecksum);
    log.debug("GHFS.setVerifyChecksum:=> ");
  }

  @Override
  public void setPermission(Path p, FsPermission permission)
      throws IOException {
    log.debug("GHFS.setPermission: path: %s, perm: %s", p, permission);
    super.setPermission(p, permission);
    log.debug("GHFS.setPermission:=> ");
  }

  @Override
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    log.debug("GHFS.setOwner: path: %s, user: %s, group: %s", p, username, groupname);
    super.setOwner(p, username, groupname);
    log.debug("GHFS.setOwner:=> ");
  }

  @Override
  public void setTimes(Path p, long mtime, long atime)
      throws IOException {
    log.debug("GHFS.setTimes: path: %s, mtime: %d, atime: %d", p, mtime, atime);
    super.setTimes(p, mtime, atime);
    log.debug("GHFS.setTimes:=> ");
  }
}
