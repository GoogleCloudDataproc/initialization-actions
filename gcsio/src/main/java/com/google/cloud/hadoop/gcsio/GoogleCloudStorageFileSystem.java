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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.TimestampUpdatePredicate;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a POSIX like file system layered on top of Google Cloud Storage (GCS).
 *
 * All file system aspects (eg, path) are encapsulated in this class, they are
 * not exposed to the underlying layer. That is, all interactions with the
 * underlying layer are strictly in terms of buckets and objects.
 */
public class GoogleCloudStorageFileSystem {

  // URI scheme for GCS.
  public static final String SCHEME = "gs";

  // URI of the root path.
  public static final URI GCS_ROOT = URI.create(SCHEME + ":/");

  // Logger.
  public static final Logger LOG =
      LoggerFactory.getLogger(GoogleCloudStorageFileSystem.class);

  // GCS access instance.
  private GoogleCloudStorage gcs;

  private final PathCodec pathCodec;

  // FS options
  private final GoogleCloudStorageFileSystemOptions options;

  // Executor for updating directory timestamps.
  private ExecutorService updateTimestampsExecutor = createUpdateTimestampsExecutor();
  // Comparator used for sorting paths.
  //
  // For some bulk operations, we need to operate on parent directories before
  // we operate on their children. To achieve this, we sort paths such that
  // shorter paths appear before longer paths. Also, we sort lexicographically
  // within paths of the same length (this is not strictly required but helps when
  // debugging/testing).
  @VisibleForTesting
  static final Comparator<URI> PATH_COMPARATOR =
      new Comparator<URI>() {
        @Override
        public int compare(URI a, URI b) {
          String as = a.toString();
          String bs = b.toString();
          return (as.length() == bs.length())
              ? as.compareTo(bs)
              : Integer.compare(as.length(), bs.length());
        }
      };

  // Comparator used for sorting a collection of FileInfo items based on path comparison.
  @VisibleForTesting
  static final Comparator<FileInfo> FILE_INFO_PATH_COMPARATOR =
      new Comparator<FileInfo>() {
        @Override
        public int compare(FileInfo file1, FileInfo file2) {
          return PATH_COMPARATOR.compare(file1.getPath(), file2.getPath());
        }
      };

  private static final Comparator<FileInfo> STRING_LENGTH_COMPARATOR =
      new Comparator<FileInfo>() {
        @Override
        public int compare(FileInfo file1, FileInfo file2) {
          return Integer.compare(
              file1.getPath().toString().length(), file2.getPath().toString().length());
        }
      };

  /**
   * A PathCodec that maintains compatibility with versions of GCS FS < 1.4.5.
   */
  public static final PathCodec LEGACY_PATH_CODEC = new LegacyPathCodec();

  /**
   * A PathCodec that expects URIs to be of the form:
   * gs://authority/properly/encoded/path.
   */
  public static final PathCodec URI_ENCODED_PATH_CODEC = new UriEncodingPathCodec();

  /**
   * Constructs an instance of GoogleCloudStorageFileSystem.
   *
   * @param credential OAuth2 credential that allows access to GCS.
   * @param options Options for how this filesystem should operate and configure its
   *    underlying storage.
   * @throws IOException
   */
  public GoogleCloudStorageFileSystem(
      Credential credential,
      GoogleCloudStorageFileSystemOptions options) throws IOException {
    LOG.debug("GCSFS({})", options.getCloudStorageOptions().getAppName());
    options.throwIfNotValid();

    checkArgument(credential != null, "credential must not be null");

    this.options = options;
    this.gcs = new GoogleCloudStorageImpl(options.getCloudStorageOptions(), credential);
    this.pathCodec = options.getPathCodec();

    if (options.isPerformanceCacheEnabled()) {
      gcs = new PerformanceCachingGoogleCloudStorage(gcs, options.getPerformanceCacheOptions());
    }
  }

  /**
   * Constructs a GoogleCloudStorageFilesystem based on an already-configured underlying
   * GoogleCloudStorage {@code gcs}.
   */
  public GoogleCloudStorageFileSystem(GoogleCloudStorage gcs) throws IOException {
    this(gcs, GoogleCloudStorageFileSystemOptions.newBuilder()
        .setImmutableCloudStorageOptions(gcs.getOptions())
        .build());
  }

  /**
   * Constructs a GoogleCloudStorageFilesystem based on an already-configured underlying
   * GoogleCloudStorage {@code gcs}. Any options pertaining to GCS creation will be ignored.
   */
  public GoogleCloudStorageFileSystem(
      GoogleCloudStorage gcs, GoogleCloudStorageFileSystemOptions options) throws IOException {
    this.gcs = gcs;
    this.options = options;
    this.pathCodec = options.getPathCodec();
  }

  @VisibleForTesting
  void setUpdateTimestampsExecutor(ExecutorService executor) {
    this.updateTimestampsExecutor = executor;
  }

  private static ExecutorService createUpdateTimestampsExecutor() {
    ThreadPoolExecutor service =
        new ThreadPoolExecutor(
            /* corePoolSize= */ 2,
            /* maximumPoolSize= */ 2,
            /* keepAliveTime= */ 2,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(1000),
            new ThreadFactoryBuilder()
                .setNameFormat("gcsfs-timestamp-updates-%d")
                .setDaemon(true)
                .build());
    // allowCoreThreadTimeOut needs to be enabled for cases where the encapsulating class does not
    service.allowCoreThreadTimeOut(true);
    return service;
  }

  /**
   * Retrieve the options that were used to create this
   * GoogleCloudStorageFileSystem.
   */
  public GoogleCloudStorageFileSystemOptions getOptions() {
    return options;
  }

  /**
   * Creates and opens an object for writing.
   * If the object already exists, it is deleted.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for writing to the given object.
   * @throws IOException
   */
  public WritableByteChannel create(URI path) throws IOException {
    LOG.debug("create({})", path);
    return create(path, CreateFileOptions.DEFAULT);
  }

  /**
   * Convert {@code CreateFileOptions} to {@code CreateObjectOptions}.
   */
  public static CreateObjectOptions objectOptionsFromFileOptions(CreateFileOptions options) {
    return new CreateObjectOptions(options.overwriteExisting(), options.getContentType(),
        options.getAttributes());
  }

  /**
   * Creates and opens an object for writing.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for writing to the given object.
   * @throws IOException
   */
  public WritableByteChannel create(URI path, CreateFileOptions options)
      throws IOException {

    LOG.debug("create({})", path);
    Preconditions.checkNotNull(path);
    if (FileInfo.isDirectoryPath(path)) {
      throw new IOException(String.format(
          "Cannot create a file whose name looks like a directory. Got '%s'", path));
    }

    // Check if a directory of that name exists.
    if (options.checkNoDirectoryConflict()) {
      URI dirPath = FileInfo.convertToDirectoryPath(pathCodec, path);
      if (exists(dirPath)) {
        throw new FileAlreadyExistsException("A directory with that name exists: " + path);
      }
    }

    // Ensure that parent directories exist.
    if (options.ensureParentDirectoriesExist()) {
      URI parentPath = getParentPath(path);
      if (parentPath != null) {
        mkdirs(parentPath);
      }
    }

    return createInternal(path, options);
  }

  /**
   * Creates and opens an object for writing.
   * If the object already exists, it is deleted.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for writing to the given object.
   * @throws IOException
   */
  WritableByteChannel createInternal(URI path, CreateFileOptions options)
      throws IOException {

    // Validate the given path. false == do not allow empty object name.
    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, false);
    if (options.getExistingGenerationId() != StorageResourceId.UNKNOWN_GENERATION_ID) {
      resourceId = new StorageResourceId(
          resourceId.getBucketName(),
          resourceId.getObjectName(),
          options.getExistingGenerationId());
    }
    WritableByteChannel channel = gcs.create(resourceId, objectOptionsFromFileOptions(options));
    tryUpdateTimestampsForParentDirectories(ImmutableList.of(path), ImmutableList.<URI>of());
    return channel;
  }

  /**
   * Opens an object for reading.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  public SeekableByteChannel open(URI path)
      throws IOException {
    return open(path, GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Opens an object for reading.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @param readOptions Fine-grained options for behaviors of retries, buffering, etc.
   * @return A channel for reading from the given object.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException if object exists but cannot be opened.
   */
  public SeekableByteChannel open(URI path, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    LOG.debug("open({}, {})", path, readOptions);
    Preconditions.checkNotNull(path);
    checkArgument(!FileInfo.isDirectoryPath(path), "Cannot open a directory for reading: %s", path);

    // Validate the given path. false == do not allow empty object name.
    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, false);
    return gcs.open(resourceId, readOptions);
  }

  /**
   * Deletes one or more items indicated by the given path.
   *
   * If path points to a directory:
   * -- if recursive is true,
   *    all items under that path are recursively deleted followed by
   *    deletion of the directory.
   * -- else,
   *    -- the directory is deleted if it is empty,
   *    -- else, an IOException is thrown.
   *
   * The recursive parameter is ignored for a file.
   *
   * @param path Path of the item to delete.
   * @param recursive If true, all sub-items are also deleted.
   *
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException
   */
  public void delete(URI path, boolean recursive)
      throws IOException {

    LOG.debug("delete({}, {})", path, recursive);
    Preconditions.checkNotNull(path);
    checkArgument(!path.equals(GCS_ROOT), "Cannot delete root path.");

    // Throw FileNotFoundException if the path does not exist.
    FileInfo fileInfo = getFileInfo(path);
    if (!fileInfo.exists()) {
      throw getFileNotFoundException(path);
    }

    List<URI> itemsToDelete = new ArrayList<>();
    List<URI> bucketsToDelete = new ArrayList<>();

    // Delete sub-items if it is a directory.
    if (fileInfo.isDirectory()) {
      List<URI> subpaths = listFileNames(fileInfo, recursive);
      if (recursive) {
        itemsToDelete.addAll(subpaths);
      } else {
        if (subpaths.size() > 0) {
          throw new DirectoryNotEmptyException("Cannot delete a non-empty directory.");
        }
      }
    }

    if (fileInfo.getItemInfo().isBucket()) {
      bucketsToDelete.add(fileInfo.getPath());
    } else {
      itemsToDelete.add(fileInfo.getPath());
    }

    deleteInternal(itemsToDelete, bucketsToDelete);

    // if we deleted a bucket, then there no need to update timestamps
    if (bucketsToDelete.isEmpty()) {
      // Any path that was deleted, we should update the parent except for parents we also deleted
      tryUpdateTimestampsForParentDirectories(itemsToDelete, itemsToDelete);
    }
  }

  /** Deletes all items in the given path list followed by all bucket items. */
  private void deleteInternal(List<URI> paths, List<URI> bucketPaths) throws IOException {
    // TODO(user): We might need to separate out children into separate batches from parents to
    // avoid deleting a parent before somehow failing to delete a child.

    // Delete children before their parents.
    //
    // Note: we modify the input list, which is ok for current usage.
    // We should make a copy in case that changes in future.
    Collections.sort(paths, PATH_COMPARATOR.reversed());

    if (paths.size() > 0) {
      List<StorageResourceId> objectsToDelete = new ArrayList<>();
      for (URI path : paths) {
        StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, false);
        objectsToDelete.add(resourceId);
      }
      gcs.deleteObjects(objectsToDelete);
    }

    if (bucketPaths.size() > 0) {
      List<String> bucketsToDelete = new ArrayList<>();
      for (URI path : bucketPaths) {
        StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, true);
        gcs.waitForBucketEmpty(resourceId.getBucketName());
        bucketsToDelete.add(resourceId.getBucketName());
      }
      if (options.enableBucketDelete()) {
        gcs.deleteBuckets(bucketsToDelete);
      } else {
        LOG.info("Skipping deletion of buckets because enableBucketDelete is false: {}",
            bucketsToDelete);
      }
    }
  }

  /**
   * Indicates whether the given item exists.
   *
   * @param path Path of the item to check.
   * @return true if the given item exists, false otherwise.
   * @throws IOException
   */
  public boolean exists(URI path)
      throws IOException {
    LOG.debug("exists({})", path);
    return getFileInfo(path).exists();
  }

  /**
   * Creates the list of directories specified in {@code exactDirPaths}; doesn't perform validation
   * and doesn't create their parent dirs if their parent dirs don't already exist. Use with
   * caution.
   */
  public void repairDirs(List<URI> exactDirPaths)
      throws IOException{
    LOG.debug("repairDirs({})", exactDirPaths);
    List<StorageResourceId> dirsToCreate = new ArrayList<>();
    for (URI dirUri : exactDirPaths) {
      StorageResourceId resourceId = pathCodec.validatePathAndGetId(dirUri, true);
      if (resourceId.isStorageObject()) {
        resourceId = FileInfo.convertToDirectoryPath(resourceId);
        dirsToCreate.add(resourceId);
      }
    }

    if (dirsToCreate.isEmpty()) {
      return;
    }

    /*
     * Note that in both cases, we do not update parent directory timestamps. The idea is that:
     * 1) directory repair isn't a user-invoked action, 2) directory repair shouldn't be thought
     * of "creating" directories, instead it drops markers to help GCSFS and GHFS find directories
     * that already "existed" and 3) it's extra RPCs on top of the list and create empty object RPCs
     */
    gcs.createEmptyObjects(dirsToCreate);

    LOG.warn("Successfully repaired {} directories.", dirsToCreate.size());
  }

  /**
   * Creates a directory at the specified path. Also creates any parent
   * directories as necessary. Similar to 'mkdir -p' command.
   *
   * @param path Path of the directory to create.
   * @throws IOException
   */
  public void mkdirs(URI path)
      throws IOException {
    LOG.debug("mkdirs({})", path);
    Preconditions.checkNotNull(path);

    if (path.equals(GCS_ROOT)) {
      // GCS_ROOT directory always exists, no need to go through the rest of the method.
      return;
    }

    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, true);

    // Ensure that the path looks like a directory path.
    if (resourceId.isStorageObject()) {
      resourceId = FileInfo.convertToDirectoryPath(resourceId);
      path = pathCodec.getPath(resourceId.getBucketName(), resourceId.getObjectName(), false);
    }

    // Create a list of all intermediate paths.
    // for example,
    // gs://foo/bar/zoo/ => (gs://foo/, gs://foo/bar/, gs://foo/bar/zoo/)
    //
    // We also need to find out if any of the subdir path item exists as a file
    // therefore, also create a list of file equivalent of each subdir path.
    // for example,
    // gs://foo/bar/zoo/ => (gs://foo/bar, gs://foo/bar/zoo)
    //
    // Note that a bucket cannot exist as a file therefore no need to
    // check for gs://foo above.
    List<URI> subDirPaths = new ArrayList<>();
    List<String> subdirs = getSubDirs(resourceId.getObjectName());
    for (String subdir : subdirs) {
      URI subPath = pathCodec.getPath(resourceId.getBucketName(), subdir, true);
      subDirPaths.add(subPath);
      LOG.debug("mkdirs: sub-path: {}", subPath);
      if (!Strings.isNullOrEmpty(subdir)) {
        URI subFilePath =
            pathCodec.getPath(
                resourceId.getBucketName(), subdir.substring(0, subdir.length() - 1), true);
        subDirPaths.add(subFilePath);
        LOG.debug("mkdirs: sub-path: {}", subFilePath);
      }
    }

    // Add the bucket portion.
    URI bucketPath = pathCodec.getPath(resourceId.getBucketName(), null, true);
    subDirPaths.add(bucketPath);
    LOG.debug("mkdirs: sub-path: {}", bucketPath);

    // Get status of each intermediate path.
    List<FileInfo> subDirInfos = getFileInfos(subDirPaths);

    // Each intermediate path must satisfy one of the following conditions:
    // -- it does not exist or
    // -- if it exists, it is a directory
    //
    // If any of the intermediate paths violates these requirements then
    // bail out early so that we do not end up with partial set of
    // created sub-directories. It is possible that the status of intermediate
    // paths can change after we make this check therefore this is a
    // good faith effort and not a guarantee.
    for (FileInfo fileInfo : subDirInfos) {
      if (fileInfo.exists() && !fileInfo.isDirectory()) {
        throw new FileAlreadyExistsException(
            "Cannot create directories because of existing file: "
            + fileInfo.getPath());
      }
    }

    // Create missing sub-directories in order of shortest prefix first.
    Collections.sort(subDirInfos, STRING_LENGTH_COMPARATOR);

    // Make buckets immediately, otherwise collect directories into a list for batch creation.
    List<StorageResourceId> dirsToCreate = new ArrayList<>();
    for (FileInfo fileInfo : subDirInfos) {
      if (fileInfo.isDirectory() && !fileInfo.exists()) {
        StorageResourceId dirId = fileInfo.getItemInfo().getResourceId();
        checkArgument(!dirId.isRoot(), "Cannot create root directory.");
        if (dirId.isBucket()) {
          gcs.create(dirId.getBucketName());
          continue;
        }

        // Ensure that the path looks like a directory path.
        dirId = FileInfo.convertToDirectoryPath(dirId);
        dirsToCreate.add(dirId);
      }
    }

    gcs.createEmptyObjects(dirsToCreate);

    List<URI> createdDirectories =
        Lists.transform(dirsToCreate, new Function<StorageResourceId, URI>() {
          @Override
          public URI apply(StorageResourceId resourceId) {
            return pathCodec.getPath(resourceId.getBucketName(), resourceId.getObjectName(), false);
          }
        });

    // Update parent directories, but not the ones we just created because we just created them.
    tryUpdateTimestampsForParentDirectories(createdDirectories, createdDirectories);
  }

  /**
   * Renames the given item's path.
   *
   * The operation is disallowed if any of the following is true:
   * -- src == GCS_ROOT
   * -- src is a file and dst == GCS_ROOT
   * -- src does not exist
   * -- dst is a file that already exists
   * -- parent of the destination does not exist.
   *
   * Otherwise, the expected behavior is as follows:
   * -- if src is a directory
   *    -- dst is an existing file => disallowed
   *    -- dst is a directory => rename the directory.
   *
   * -- if src is a file
   *    -- dst is a file => rename the file.
   *    -- dst is a directory => similar to the previous case after
   *                             appending src file-name to dst
   *
   * Note:
   * This function is very expensive to call for directories that
   * have many sub-items.
   *
   * @param src Path of the item to rename.
   * @param dst New path of the item.
   * @throws FileNotFoundException if src does not exist.
   * @throws IOException
   */
  public void rename(URI src, URI dst) throws IOException {
    LOG.debug("rename({}, {})", src, dst);
    Preconditions.checkNotNull(src);
    Preconditions.checkNotNull(dst);
    checkArgument(!src.equals(GCS_ROOT), "Root path cannot be renamed.");

    // Leaf item of the source path.
    String srcItemName = getItemName(src);

    // Parent of the destination path.
    URI dstParent = getParentPath(dst);

    // Obtain info on source, destination and destination-parent.
    List<URI> paths = new ArrayList<>();
    paths.add(src);
    paths.add(dst);
    if (dstParent != null) {
      // dstParent is null if dst is GCS_ROOT.
      paths.add(dstParent);
    }
    List<FileInfo> fileInfo = getFileInfos(paths);
    FileInfo srcInfo = fileInfo.get(0);
    FileInfo dstInfo = fileInfo.get(1);
    FileInfo dstParentInfo = null;
    if (dstParent != null) {
      dstParentInfo = fileInfo.get(2);
    }

    // Make sure paths match what getFileInfo() returned (it can add / at the end).
    src = srcInfo.getPath();
    dst = dstInfo.getPath();

    // Throw if the source file does not exist.
    if (!srcInfo.exists()) {
      throw getFileNotFoundException(src);
    }

    // Throw if src is a file and dst == GCS_ROOT
    if (!srcInfo.isDirectory() && dst.equals(GCS_ROOT)) {
      throw new IOException("A file cannot be created in root.");
    }

    // Throw if the destination is a file that already exists and it's not a source file.
    if (dstInfo.exists()
        && !dstInfo.isDirectory()
        && (srcInfo.isDirectory() || !dst.equals(src))) {
      throw new IOException("Cannot overwrite existing file: " + dst);
    }

    // Rename operation cannot be completed if parent of destination does not exist.
    if ((dstParentInfo != null) && !dstParentInfo.exists()) {
      throw new IOException("Cannot rename because path does not exist: " + dstParent);
    }

    // Having taken care of the initial checks, apply the regular rules.
    // After applying the rules, we will be left with 2 paths such that:
    // -- either both are files or both are directories
    // -- src exists and dst leaf does not exist
    if (srcInfo.isDirectory()) {
      // -- if src is a directory
      //    -- dst is an existing file => disallowed
      //    -- dst is a directory => rename the directory.

      // The first case (dst is an existing file) is already checked earlier.
      // If the destination path looks like a file, make it look like a
      // directory path. This is because users often type 'mv foo bar'
      // rather than 'mv foo bar/'.
      if (!dstInfo.isDirectory()) {
        dst = FileInfo.convertToDirectoryPath(pathCodec, dst);
        dstInfo = getFileInfo(dst);
      }

      // Throw if renaming directory to self - this is forbidden
      if (src.equals(dst)) {
        throw new IOException("Rename dir to self is forbidden");
      }

      URI dstRelativeToSrc = src.relativize(dst);
      // Throw if dst URI relative to src is not equal to dst,
      // because this means that src is a parent directory of dst
      // and src can not be "renamed" to its subdirectory
      if (!dstRelativeToSrc.equals(dst)) {
        throw new IOException("Rename to subdir is forbidden");
      }

      if (dstInfo.exists()) {
        if (dst.equals(GCS_ROOT)) {
          dst = pathCodec.getPath(srcItemName, null, true);
        } else {
          dst = FileInfo.convertToDirectoryPath(pathCodec, dst.resolve(srcItemName));
        }
      }
    } else {
      // -- src is a file
      //    -- dst is a file => rename the file.
      //    -- dst is a directory => similar to the previous case after
      //                             appending src file-name to dst

      if (dstInfo.isDirectory()) {
        if (!dstInfo.exists()) {
          throw new IOException("Cannot rename because path does not exist: " + dstInfo.getPath());
        } else {
          dst = dst.resolve(srcItemName);
        }
      } else {
        // Destination is a file.
        // See if there is a directory of that name.
        URI dstDir = FileInfo.convertToDirectoryPath(pathCodec, dst);
        FileInfo dstDirInfo = getFileInfo(dstDir);
        if (dstDirInfo.exists()) {
          dst = dstDir.resolve(srcItemName);
        }
      }
    }

    // if src and dst are equal then do nothing
    if (src.equals(dst)) {
      return;
    }

    renameInternal(srcInfo, dst);
  }

  /**
   * Composes inputs into a single GCS object. This performs a GCS Compose. Objects will be composed
   * according to the order they appear in the input. The destination object, if already present,
   * will be overwritten. Sources and destination are assumed to be in the same bucket.
   *
   * @param sources the list of URIs to be composed
   * @param destination the resulting URI with composed sources
   * @param contentType content-type of the composed object
   * @throws IOException if the Compose operation was unsuccessful
   */
  public void compose(List<URI> sources, URI destination, String contentType) throws IOException {
    StorageResourceId destResource = StorageResourceId.fromObjectName(destination.toString());
    List<String> sourceObjects =
        Lists.transform(
            sources,
            new Function<URI, String>() {
              @Override
              public String apply(URI uri) {
                return StorageResourceId.fromObjectName(uri.toString()).getObjectName();
              }
            });
    gcs.compose(
        destResource.getBucketName(), sourceObjects, destResource.getObjectName(), contentType);
  }

  /**
   * Renames the given path without checking any parameters.
   *
   * <p>GCS does not support atomic renames therefore a rename is implemented as copying source
   * metadata to destination and then deleting source metadata. Note that only the metadata is
   * copied and not the content of any file.
   */
  private void renameInternal(FileInfo srcInfo, URI dst) throws IOException {
    if (srcInfo.isDirectory()) {
      renameDirectoryInternal(srcInfo, dst);
    } else {
      URI src = srcInfo.getPath();
      StorageResourceId srcResourceId = pathCodec.validatePathAndGetId(src, true);
      StorageResourceId dstResourceId = pathCodec.validatePathAndGetId(dst, true);

      gcs.copy(
          srcResourceId.getBucketName(),
          ImmutableList.of(srcResourceId.getObjectName()),
          dstResourceId.getBucketName(),
          ImmutableList.of(dstResourceId.getObjectName()));

      tryUpdateTimestampsForParentDirectories(ImmutableList.of(dst), ImmutableList.<URI>of());

      gcs.deleteObjects(ImmutableList.of(srcResourceId));

      // Any path that was deleted, we should update the parent except for parents we also deleted
      tryUpdateTimestampsForParentDirectories(ImmutableList.of(src), ImmutableList.<URI>of());
    }
  }

  /**
   * Renames given directory.
   *
   * @see #renameInternal
   */
  private void renameDirectoryInternal(FileInfo srcInfo, URI dst) throws IOException {
    checkArgument(srcInfo.isDirectory(), "'%s' should be a directory", srcInfo);

    Pattern markerFilePattern = options.getMarkerFilePattern();

    // Mapping from each src to its respective dst.
    // Sort src items so that parent directories appear before their children.
    // That allows us to copy parent directories before we copy their children.
    Map<URI, URI> srcToDstItemNames = new TreeMap<>(PATH_COMPARATOR);
    Map<URI, URI> srcToDstMarkerItemNames = new TreeMap<>(PATH_COMPARATOR);

    // List of individual paths to rename;
    // we will try to carry out the copies in this list's order.
    List<URI> srcItemNames = listFileNames(srcInfo, true);

    // Create the destination directory.
    dst = FileInfo.convertToDirectoryPath(pathCodec, dst);
    mkdir(dst);

    // Create a list of sub-items to copy.
    String prefix = srcInfo.getPath().toString();
    for (URI srcItemName : srcItemNames) {
      String relativeItemName = srcItemName.toString().substring(prefix.length());
      URI dstItemName = dst.resolve(relativeItemName);
      if (markerFilePattern != null && markerFilePattern.matcher(relativeItemName).matches()) {
        srcToDstMarkerItemNames.put(srcItemName, dstItemName);
      } else {
        srcToDstItemNames.put(srcItemName, dstItemName);
      }
    }

    // First, copy all items excpet marker items
    copyInternal(srcToDstItemNames);
    // Finally, copy marker items (if any) to mark rename operation success
    copyInternal(srcToDstMarkerItemNames);

    // So far, only the destination directories are updated. Only do those now:
    if (!srcToDstItemNames.isEmpty() || !srcToDstMarkerItemNames.isEmpty()) {
      List<URI> allDestinationUris =
          new ArrayList<>(srcToDstItemNames.size() + srcToDstMarkerItemNames.size());
      allDestinationUris.addAll(srcToDstItemNames.values());
      allDestinationUris.addAll(srcToDstMarkerItemNames.values());

      tryUpdateTimestampsForParentDirectories(allDestinationUris, allDestinationUris);
    }

    List<URI> bucketsToDelete = new ArrayList<>(1);
    List<URI> srcItemsToDelete = new ArrayList<>(srcToDstItemNames.size() + 1);
    srcItemsToDelete.addAll(srcToDstItemNames.keySet());
    if (srcInfo.getItemInfo().isBucket()) {
      bucketsToDelete.add(srcInfo.getPath());
    } else {
      // If src is a directory then srcItemNames does not contain its own name,
      // therefore add it to the list before we delete items in the list.
      srcItemsToDelete.add(srcInfo.getPath());
    }

    // First delete marker files from the src
    deleteInternal(new ArrayList<>(srcToDstMarkerItemNames.keySet()), new ArrayList<URI>());
    // Then delete rest of the items that we successfully copied.
    deleteInternal(srcItemsToDelete, bucketsToDelete);

    // if we deleted a bucket, then there no need to update timestamps
    if (bucketsToDelete.isEmpty()) {
      // Any path that was deleted, we should update the parent except for parents we also deleted
      tryUpdateTimestampsForParentDirectories(srcItemNames, srcItemNames);
    }
  }

  /** Copies items in given map that maps source items to destination items. */
  private void copyInternal(Map<URI, URI> srcToDstItemNames) throws IOException {
    if (srcToDstItemNames.isEmpty()) {
      return;
    }

    String srcBucketName = null;
    String dstBucketName = null;
    List<String> srcObjectNames = new ArrayList<>(srcToDstItemNames.size());
    List<String> dstObjectNames = new ArrayList<>(srcToDstItemNames.size());

    // Prepare list of items to copy.
    for (Map.Entry<URI, URI> srcToDstItemName : srcToDstItemNames.entrySet()) {
      StorageResourceId srcResourceId =
          pathCodec.validatePathAndGetId(srcToDstItemName.getKey(), true);
      srcBucketName = srcResourceId.getBucketName();
      String srcObjectName = srcResourceId.getObjectName();
      srcObjectNames.add(srcObjectName);

      StorageResourceId dstResourceId =
          pathCodec.validatePathAndGetId(srcToDstItemName.getValue(), true);
      dstBucketName = dstResourceId.getBucketName();
      String dstObjectName = dstResourceId.getObjectName();
      dstObjectNames.add(dstObjectName);
    }

    // Perform copy.
    gcs.copy(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames);
  }

  /**
   * If the given item is a directory then the paths of its immediate
   * children are returned, otherwise the path of the given item is returned.
   *
   * @param fileInfo FileInfo of an item.
   * @return Paths of children (if directory) or self path.
   * @throws IOException
   */
  public List<URI> listFileNames(FileInfo fileInfo)
      throws IOException {
    return listFileNames(fileInfo, false);
  }

  /**
   * If the given item is a directory then the paths of its
   * children are returned, otherwise the path of the given item is returned.
   *
   * @param fileInfo FileInfo of an item.
   * @param recursive If true, path of all children are returned;
   *                  else, only immediate children are returned.
   * @return Paths of children (if directory) or self path.
   * @throws IOException
   */
  public List<URI> listFileNames(FileInfo fileInfo, boolean recursive)
      throws IOException {

    Preconditions.checkNotNull(fileInfo);
    URI path = fileInfo.getPath();
    LOG.debug("listFileNames({})", path);
    List<URI> paths = new ArrayList<>();
    List<String> childNames;

    // If it is a directory, obtain info about its children.
    if (fileInfo.isDirectory()) {
      if (fileInfo.exists()) {
        if (fileInfo.isGlobalRoot()) {
          childNames = gcs.listBucketNames();

          // Obtain path for each child.
          for (String childName : childNames) {
            URI childPath = pathCodec.getPath(childName, null, true);
            paths.add(childPath);
            LOG.debug("listFileNames: added: {}", childPath);
          }
        } else {
          // A null delimiter asks GCS to return all objects with a given prefix,
          // regardless of their 'directory depth' relative to the prefix;
          // that is what we want for a recursive list. On the other hand,
          // when a delimiter is specified, only items with relative depth
          // of 1 are returned.
          String delimiter = recursive ? null : GoogleCloudStorage.PATH_DELIMITER;

          // Obtain paths of children.
          childNames = gcs.listObjectNames(
              fileInfo.getItemInfo().getBucketName(),
              fileInfo.getItemInfo().getObjectName(),
              delimiter);

          // Obtain path for each child.
          for (String childName : childNames) {
            URI childPath = pathCodec
                .getPath(fileInfo.getItemInfo().getBucketName(), childName, false);
            paths.add(childPath);
            LOG.debug("listFileNames: added: {}", childPath);
          }
        }
      }
    } else {
      paths.add(path);
      LOG.debug("listFileNames: added single original path since !isDirectory(): {}", path);
    }

    return paths;
  }

  /**
   * Checks that {@code path} doesn't already exist as a directory object, and if so, performs
   * an object listing using the full path as the match prefix so that if there are any objects
   * that imply {@code path} is a parent directory, we will discover its existence as a returned
   * GCS 'prefix'. In such a case, the directory object will be explicitly created.
   *
   * @return true if a repair was successfully made, false if a repair was unnecessary or failed.
   */
  public boolean repairPossibleImplicitDirectory(URI path)
      throws IOException {
    LOG.debug("repairPossibleImplicitDirectory({})", path);
    Preconditions.checkNotNull(path);

    // First, obtain information about the given path.
    FileInfo pathInfo = getFileInfo(path);

    pathInfo = repairPossibleImplicitDirectory(pathInfo);

    if (pathInfo.exists()) {
      // Definitely didn't exist before, and now it does exist.
      LOG.debug("Successfully repaired path '{}'", path);
      return true;
    } else {
      LOG.debug("Repair claimed to succeed, but somehow failed for path '{}'", path);
      return false;
    }
  }

  /**
   * Helper for repairing possible implicit directories, taking a previously obtained FileInfo
   * and returning a re-fetched FileInfo after attemping the repair. The returned FileInfo
   * may still report !exists() if the repair failed.
   */
  private FileInfo repairPossibleImplicitDirectory(FileInfo pathInfo)
      throws IOException {
    if (pathInfo.exists()) {
      // It already exists, so there's nothing to repair; there must have been a mistake.
      return pathInfo;
    }

    if (pathInfo.isGlobalRoot() || pathInfo.getItemInfo().isBucket()
        || pathInfo.getItemInfo().getObjectName().equals(GoogleCloudStorage.PATH_DELIMITER)) {
      // Implicit directories are only applicable for non-trivial object names.
      return pathInfo;
    }

    // TODO(user): Refactor the method name and signature to make this less hacky; the logic of
    // piggybacking on auto-repair within listObjectInfo is sound because listing with prefixes
    // is a natural prerequisite for verifying that an implicit directory indeed exists. We just
    // need to make it more clear that the method is actually "list and maybe repair".
    try {
      gcs.listObjectInfo(
          pathInfo.getItemInfo().getBucketName(),
          FileInfo.convertToFilePath(pathInfo.getItemInfo().getObjectName()),
          GoogleCloudStorage.PATH_DELIMITER);
    } catch (IOException ioe) {
      LOG.error("Got exception trying to listObjectInfo on " + pathInfo, ioe);
      // It's possible our repair succeeded anyway.
    }

    pathInfo = getFileInfo(pathInfo.getPath());
    return pathInfo;
  }

  /**
   * Equivalent to a recursive listing of {@code prefix}, except that {@code prefix} doesn't
   * have to represent an actual object but can just be a partial prefix string, and there
   * is no auto-repair of implicit directories since we can't detect implicit directories
   * without listing by 'delimiter'. The 'authority' component of the {@code prefix} *must*
   * be the complete authority, however; we can only list prefixes of *objects*, not buckets.
   *
   * @param prefix the prefix to use to list all matching objects.
   */
  public List<FileInfo> listAllFileInfoForPrefix(URI prefix)
      throws IOException {
    LOG.debug("listAllFileInfoForPrefix({})", prefix);
    Preconditions.checkNotNull(prefix);

    StorageResourceId prefixId = pathCodec.validatePathAndGetId(prefix, true);
    Preconditions.checkState(
        !prefixId.isRoot(), "Prefix must not be global root, got '%s'", prefix);
    // Use 'null' for delimiter to get full 'recursive' listing.
    List<GoogleCloudStorageItemInfo> itemInfos =
        gcs.listObjectInfo(prefixId.getBucketName(), prefixId.getObjectName(), null);
    List<FileInfo> fileInfos = FileInfo.fromItemInfos(pathCodec, itemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  /**
   * If the given path points to a directory then the information about its
   * children is returned, otherwise information about the given file is returned.
   *
   * Note:
   * This function is expensive to call, especially for a directory with many
   * children. Use the alternative
   * {@link GoogleCloudStorageFileSystem#listFileNames(FileInfo)} if you only need
   * names of children and no other attributes.
   *
   * @param path Given path.
   * @param enableAutoRepair if true, attempt to repair implicit directories when detected.
   * @return Information about a file or children of a directory.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException
   */
  public List<FileInfo> listFileInfo(URI path, boolean enableAutoRepair)
      throws IOException {
    LOG.debug("listFileInfo({}, {})", path, enableAutoRepair);
    Preconditions.checkNotNull(path);

    URI dirPath = FileInfo.convertToDirectoryPath(pathCodec, path);
    List<FileInfo> baseAndDirInfos = getFileInfosRaw(ImmutableList.of(path, dirPath));
    Preconditions.checkState(
        baseAndDirInfos.size() == 2, "Expected baseAndDirInfos.size() == 2, got %s",
        baseAndDirInfos.size());

    // If the non-directory object exists, return a single-element list directly.
    if (!baseAndDirInfos.get(0).isDirectory() && baseAndDirInfos.get(0).exists()) {
      List<FileInfo> listedInfo = new ArrayList<>();
      listedInfo.add(baseAndDirInfos.get(0));
      return listedInfo;
    }

    // The second element is definitely a directory-path FileInfo.
    FileInfo dirInfo = baseAndDirInfos.get(1);
    if (!dirInfo.exists()) {
      if (enableAutoRepair) {
        dirInfo = repairPossibleImplicitDirectory(dirInfo);
      } else if (options.getCloudStorageOptions()
                  .isInferImplicitDirectoriesEnabled()) {
        StorageResourceId dirId = dirInfo.getItemInfo().getResourceId();
        if (!dirInfo.isDirectory()) {
          dirId = FileInfo.convertToDirectoryPath(dirId);
        }
        dirInfo = FileInfo.fromItemInfo(pathCodec, getInferredItemInfo(dirId));
      }
    }

    // Still doesn't exist after attempted repairs (or repairs were disabled).
    if (!dirInfo.exists()) {
      throw getFileNotFoundException(path);
    }

    List<GoogleCloudStorageItemInfo> itemInfos;
    if (dirInfo.isGlobalRoot()) {
      itemInfos = gcs.listBucketInfo();
    } else {
      itemInfos = gcs.listObjectInfo(
          dirInfo.getItemInfo().getBucketName(),
          dirInfo.getItemInfo().getObjectName(),
          GoogleCloudStorage.PATH_DELIMITER);
    }
    List<FileInfo> fileInfos = FileInfo.fromItemInfos(pathCodec, itemInfos);
    Collections.sort(fileInfos, FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  /**
   * Gets information about the given path item.
   *
   * @param path The path we want information about.
   * @return Information about the given path item.
   * @throws IOException
   */
  public FileInfo getFileInfo(URI path)
      throws IOException {
    LOG.debug("getFileInfo({})", path);
    checkArgument(path != null, "path must not be null");

    // Validate the given path. true == allow empty object name.
    // One should be able to get info about top level directory (== bucket),
    // therefore we allow object name to be empty.
    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, true);
    GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(resourceId);
    // TODO(user): Here and below, just request foo and foo/ simultaneously in the same batch
    // request and choose the relevant one.
    if (!itemInfo.exists() && !FileInfo.isDirectory(itemInfo)) {
      // If the given file does not exist, see if a directory of
      // the same name exists.
      StorageResourceId newResourceId = FileInfo.convertToDirectoryPath(resourceId);
      LOG.debug("getFileInfo({}) : not found. trying: {}", path, newResourceId);
      GoogleCloudStorageItemInfo newItemInfo = gcs.getItemInfo(newResourceId);
      // Only swap out the old not-found itemInfo if the "converted" itemInfo actually exists; if
      // both forms do not exist, we will just go with the original non-converted itemInfo.
      if (newItemInfo.exists()) {
        LOG.debug(
            "getFileInfo: swapping not-found info: %s for converted info: %s",
            itemInfo, newItemInfo);
        itemInfo = newItemInfo;
        resourceId = newResourceId;
      }
    }

    if (!itemInfo.exists()
        && options.getCloudStorageOptions().isInferImplicitDirectoriesEnabled()
        && !itemInfo.isRoot()
        && !itemInfo.isBucket()) {
      StorageResourceId newResourceId = resourceId;
      if (!FileInfo.isDirectory(itemInfo)) {
        newResourceId = FileInfo.convertToDirectoryPath(resourceId);
      }
      LOG.debug("getFileInfo({}) : still not found, trying inferred: {}", path, newResourceId);
      GoogleCloudStorageItemInfo newItemInfo = getInferredItemInfo(resourceId);
      if (newItemInfo.exists()) {
        LOG.debug(
            "getFileInfo: swapping not-found info: %s for inferred info: %s",
            itemInfo, newItemInfo);
        itemInfo = newItemInfo;
        resourceId = newResourceId;
      }
    }

    FileInfo fileInfo = FileInfo.fromItemInfo(pathCodec, itemInfo);
    LOG.debug("getFileInfo: {}", fileInfo);
    return fileInfo;
  }

  /**
   * Gets information about each path in the given list; more efficient than calling getFileInfo()
   * on each path individually in a loop.
   *
   * @param paths List of paths.
   * @return Information about each path in the given list.
   * @throws IOException
   */
  public List<FileInfo> getFileInfos(List<URI> paths)
      throws IOException {
    LOG.debug("getFileInfos(list)");
    checkArgument(paths != null, "paths must not be null");

    // First, parse all the URIs into StorageResourceIds while validating them.
    List<StorageResourceId> resourceIdsForPaths = new ArrayList<>(paths.size());
    for (URI path : paths) {
      resourceIdsForPaths.add(pathCodec.validatePathAndGetId(path, true));
    }

    // Call the bulk getItemInfos method to retrieve per-id info.
    List<GoogleCloudStorageItemInfo> itemInfos = gcs.getItemInfos(resourceIdsForPaths);

    // Possibly re-fetch for "not found" items, which may require implicit casting to directory
    // paths (e.g., StorageObject that lacks a trailing slash). Hold mapping from post-conversion
    // StorageResourceId to the index of itemInfos the new item will replace.
    // NB: HashMap here is required; if we wish to use TreeMap we must implement Comparable in
    // StorageResourceId.
    // TODO(user): Use HashMultimap if it ever becomes possible for the input to list the same
    // URI multiple times and actually expects the returned list to list those duplicate values
    // the same multiple times.
    Map<StorageResourceId, Integer> convertedIdsToIndex = new HashMap<>();
    for (int i = 0; i < itemInfos.size(); ++i) {
      if (!itemInfos.get(i).exists() && !FileInfo.isDirectory(itemInfos.get(i))) {
        StorageResourceId convertedId =
            FileInfo.convertToDirectoryPath(itemInfos.get(i).getResourceId());
        LOG.debug("getFileInfos({}) : not found. trying: {}",
            itemInfos.get(i).getResourceId(), convertedId);
        convertedIdsToIndex.put(convertedId, i);
      }
    }

    // If we found potential items needing re-fetch after converting to a directory path, we issue
    // a new bulk fetch and then patch the returned items into their respective indices in the list.
    if (!convertedIdsToIndex.isEmpty()) {
      List<StorageResourceId> convertedResourceIds = new ArrayList<>(convertedIdsToIndex.keySet());
      List<GoogleCloudStorageItemInfo> convertedInfos = gcs.getItemInfos(convertedResourceIds);
      for (int i = 0; i < convertedResourceIds.size(); ++i) {
        if (convertedInfos.get(i).exists()) {
          int replaceIndex = convertedIdsToIndex.get(convertedResourceIds.get(i));
          LOG.debug("getFileInfos: swapping not-found info: {} for converted info: {}",
              itemInfos.get(replaceIndex), convertedInfos.get(i));
          itemInfos.set(replaceIndex, convertedInfos.get(i));
        }
      }
    }

    // If we are inferring directories and we still have some items that
    // are not found, run through the items again looking for inferred
    // directories.
    if (options.getCloudStorageOptions().isInferImplicitDirectoriesEnabled()) {
      Map<StorageResourceId, Integer> inferredIdsToIndex = new HashMap<>();
      for (int i = 0; i < itemInfos.size(); ++i) {
        if (!itemInfos.get(i).exists()) {
          StorageResourceId inferredId = itemInfos.get(i).getResourceId();
          if (!FileInfo.isDirectory(itemInfos.get(i))) {
            inferredId = FileInfo.convertToDirectoryPath(inferredId);
          }
          LOG.debug("getFileInfos({}) : still not found, trying inferred: {}",
              itemInfos.get(i).getResourceId(), inferredId);
          inferredIdsToIndex.put(inferredId, i);
        }
      }

      if (!inferredIdsToIndex.isEmpty()) {
        List<StorageResourceId> inferredResourceIds = new ArrayList<>(inferredIdsToIndex.keySet());
        List<GoogleCloudStorageItemInfo> inferredInfos =
            getInferredItemInfos(inferredResourceIds);
        for (int i = 0; i < inferredResourceIds.size(); ++i) {
          if (inferredInfos.get(i).exists()) {
            int replaceIndex =
                inferredIdsToIndex.get(inferredResourceIds.get(i));
            LOG.debug("getFileInfos: swapping not-found info: "
                + "%s for inferred info: %s",
                itemInfos.get(replaceIndex), inferredInfos.get(i));
            itemInfos.set(replaceIndex, inferredInfos.get(i));
          }
        }
      }
    }

    // Finally, plug each GoogleCloudStorageItemInfo into a respective FileInfo before returning.
    return FileInfo.fromItemInfos(pathCodec, itemInfos);
  }

  /**
   * Efficiently gets info about each path in the list without performing auto-retry with casting
   * paths to "directory paths". This means that even if "foo/" exists, fetching "foo" will return
   * a !exists() info, unlike {@link #getFileInfos(List<URI>)}.
   *
   * @param paths List of paths.
   * @return Information about each path in the given list.
   * @throws IOException
   */
  private List<FileInfo> getFileInfosRaw(List<URI> paths)
      throws IOException {
    LOG.debug("getFileInfosRaw({})", paths);
    checkArgument(paths != null, "paths must not be null");

    // First, parse all the URIs into StorageResourceIds while validating them.
    List<StorageResourceId> resourceIdsForPaths = new ArrayList<>();
    for (URI path : paths) {
      resourceIdsForPaths.add(pathCodec.validatePathAndGetId(path, true));
    }

    // Call the bulk getItemInfos method to retrieve per-id info.
    List<GoogleCloudStorageItemInfo> itemInfos = gcs.getItemInfos(resourceIdsForPaths);

    // Finally, plug each GoogleCloudStorageItemInfo into a respective FileInfo before returning.
    return FileInfo.fromItemInfos(pathCodec, itemInfos);
  }

  /**
   * Gets information about an inferred object that represents a directory
   * but which is not explicitly represented in GCS.
   *
   * @param dirId identifies either root, a Bucket, or a StorageObject
   * @return information about the given item
   * @throws IOException on IO error
   */
  private GoogleCloudStorageItemInfo getInferredItemInfo(
      StorageResourceId dirId) throws IOException {

    if (dirId.isRoot() || dirId.isBucket()) {
      return GoogleCloudStorageItemInfo.createNotFound(dirId);
    }

    StorageResourceId bucketId = new StorageResourceId(dirId.getBucketName());
    if (!gcs.getItemInfo(bucketId).exists()) {
      // If the bucket does not exist, don't try to look for children.
      return GoogleCloudStorageItemInfo.createNotFound(dirId);
    }

    dirId = FileInfo.convertToDirectoryPath(dirId);

    String bucketName = dirId.getBucketName();
    // We have ensured that the path ends in the delimiter,
    // so now we can just use that path as the prefix.
    String objectNamePrefix = dirId.getObjectName();
    String delimiter = GoogleCloudStorage.PATH_DELIMITER;

    List<String> objectNames = gcs.listObjectNames(
        bucketName, objectNamePrefix, delimiter, 1);

    if (objectNames.size() > 0) {
      // At least one object with that prefix exists, so infer a directory.
      return GoogleCloudStorageItemInfo.createInferredDirectory(dirId);
    } else {
      return GoogleCloudStorageItemInfo.createNotFound(dirId);
    }
  }

  /**
   * Gets information about multiple inferred objects and/or buckets.
   * Items that are "not found" will still have an entry in the returned list;
   * exists() will return false for these entries.
   *
   * @param resourceIds names of the GCS StorageObjects or
   *        Buckets for which to retrieve info.
   * @return information about the given resourceIds.
   * @throws IOException on IO error
   */
  private List<GoogleCloudStorageItemInfo> getInferredItemInfos(
      List<StorageResourceId> resourceIds) throws IOException {
    List<GoogleCloudStorageItemInfo> itemInfos = new ArrayList<>();
    for (int i = 0; i < resourceIds.size(); ++i) {
      itemInfos.add(getInferredItemInfo(resourceIds.get(i)));
    }
    return itemInfos;
  }

  /**
   * Releases resources used by this instance.
   */
  public void close() {
    if (gcs != null) {
      LOG.debug("close()");
      try {
        gcs.close();
      } finally {
        gcs = null;
      }
    }

    if (updateTimestampsExecutor != null) {
      updateTimestampsExecutor.shutdown();
      try {
        if (!updateTimestampsExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          LOG.warn("Forcibly shutting down timestamp update threadpool.");
          updateTimestampsExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted awaiting timestamp update threadpool.");
      }
      updateTimestampsExecutor = null;
    }
  }

  /**
   * Creates a directory at the specified path.
   *
   * There are two conventions for using objects as directories in GCS.
   * 1. An object of zero size with name ending in /
   * 2. An object of zero size with name ending in _$folder$
   *
   * #1 is the recommended convention by the GCS team. We use it when
   * creating a directory.
   *
   * However, some old tools still use #2. We will decide based on customer
   * use cases whether to support #2 as well. For now, we only support #1.
   *
   * Note that a bucket is always considered a directory.
   * Doesn't create parent directories; normal use cases should only call mkdirs().
   */
  @VisibleForTesting
  public void mkdir(URI path)
      throws IOException {

    LOG.debug("mkdir({})", path);
    Preconditions.checkNotNull(path);
    checkArgument(!path.equals(GCS_ROOT), "Cannot create root directory.");

    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, true);

    // If this is a top level directory, create the corresponding bucket.
    if (resourceId.isBucket()) {
      gcs.create(resourceId.getBucketName());
      return;
    }

    // Ensure that the path looks like a directory path.
    resourceId = FileInfo.convertToDirectoryPath(resourceId);

    // Not a top-level directory, create 0 sized object.
    gcs.createEmptyObject(resourceId);

    tryUpdateTimestampsForParentDirectories(ImmutableList.of(path), ImmutableList.<URI>of());
  }

  /**
   * For each listed modified object, attempt to update the modification time
   * of the parent directory.
   * @param modifiedObjects The objects that have been modified
   * @param excludedParents A list of parent directories that we shouldn't attempt to update.
   */
  protected void updateTimestampsForParentDirectories(
      List<URI> modifiedObjects, List<URI> excludedParents) throws IOException {
    LOG.debug("updateTimestampsForParentDirectories({}, {})", modifiedObjects, excludedParents);

    TimestampUpdatePredicate updatePredicate =
        options.getShouldIncludeInTimestampUpdatesPredicate();
    Set<URI> excludedParentPathsSet = new HashSet<>(excludedParents);

    HashSet<URI> parentUrisToUpdate = new HashSet<>(modifiedObjects.size());
    for (URI modifiedObjectUri : modifiedObjects) {
      URI parentPathUri = getParentPath(modifiedObjectUri);
      if (!excludedParentPathsSet.contains(parentPathUri)
          && updatePredicate.shouldUpdateTimestamp(parentPathUri)) {
        parentUrisToUpdate.add(parentPathUri);
      }
    }

    Map<String, byte[]> modificationAttributes = new HashMap<>();
    FileInfo.addModificationTimeToAttributes(modificationAttributes, Clock.SYSTEM);

    List<UpdatableItemInfo> itemUpdates = new ArrayList<>(parentUrisToUpdate.size());

    for (URI parentUri : parentUrisToUpdate) {
      StorageResourceId resourceId = pathCodec.validatePathAndGetId(parentUri, true);
      if (!resourceId.isBucket() && !resourceId.isRoot()) {
        itemUpdates.add(new UpdatableItemInfo(resourceId, modificationAttributes));
      }
    }

    if (!itemUpdates.isEmpty()) {
      gcs.updateItems(itemUpdates);
    } else {
      LOG.debug("All paths were excluded from directory timestamp updating.");
    }
  }

  /**
   * For each listed modified object, attempt to update the modification time
   * of the parent directory.
   *
   * This method will log & swallow exceptions thrown by the GCSIO layer.
   * @param modifiedObjects The objects that have been modified
   * @param excludedParents A list of parent directories that we shouldn't attempt to update.
   */
  protected void tryUpdateTimestampsForParentDirectories(
      final List<URI> modifiedObjects, final List<URI> excludedParents) {
    LOG.debug("tryUpdateTimestampsForParentDirectories({}, {})", modifiedObjects, excludedParents);

    // If we're calling tryUpdateTimestamps, we don't actually care about the results. Submit
    // these requests via a background thread and continue on.
    try {
      @SuppressWarnings("unused") // go/futurereturn-lsc
      Future<?> possiblyIgnoredError =
          updateTimestampsExecutor.submit(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    updateTimestampsForParentDirectories(modifiedObjects, excludedParents);
                  } catch (IOException ioe) {
                    LOG.debug(
                        "Exception caught when trying to update parent directory timestamps.", ioe);
                  }
                }
              });
    } catch (RejectedExecutionException ree) {
      LOG.debug("Exhausted thread pool and queue space while updating parent timestamps", ree);
    }
  }

  /**
   * For objects whose name looks like a path (foo/bar/zoo),
   * returns intermediate sub-paths.
   *
   * for example,
   * foo/bar/zoo => returns: (foo/, foo/bar/)
   * foo => returns: ()
   *
   * @param objectName Name of an object.
   * @return List of sub-directory like paths.
   */
  static List<String> getSubDirs(String objectName) {
    List<String> subdirs = new ArrayList<>();
    if (!Strings.isNullOrEmpty(objectName)) {
      // Create a list of all subdirs.
      // for example,
      // foo/bar/zoo => (foo/, foo/bar/)
      int currentIndex = 0;
      while (currentIndex < objectName.length()) {
        int index = objectName.indexOf('/', currentIndex);
        if (index < 0) {
          break;
        }
        subdirs.add(objectName.substring(0, index + 1));
        currentIndex = index + 1;
      }
    }

    return subdirs;
  }

  /**
   * Validate the given bucket name to make sure that it can be used
   * as a part of a file system path.
   *
   * Note: this is not designed to duplicate the exact checks that GCS
   * would perform on the server side. We make some checks
   * that are relevant to using GCS as a file system.
   *
   * @param bucketName Bucket name to check.
   */
  static String validateBucketName(String bucketName) {

    // If the name ends with /, remove it.
    bucketName = FileInfo.convertToFilePath(bucketName);

    if (Strings.isNullOrEmpty(bucketName)) {
      throw new IllegalArgumentException(
          "Google Cloud Storage bucket name cannot be empty.");
    }

    if (bucketName.indexOf('/') >= 0) {
      throw new IllegalArgumentException(
          "Google Cloud Storage bucket name must not contain '/' character.");
    }

    return bucketName;
  }

  /**
   * Validate the given object name to make sure that it can be used
   * as a part of a file system path.
   *
   * Note: this is not designed to duplicate the exact checks that GCS
   * would perform on the server side. We make some checks
   * that are relevant to using GCS as a file system.
   *
   * @param objectName Object name to check.
   * @param allowEmptyObjectName If true, a missing object name is not considered invalid.
   */
  static String validateObjectName(String objectName, boolean allowEmptyObjectName) {
    LOG.debug("validateObjectName('{}', {})", objectName, allowEmptyObjectName);

    if (isNullOrEmpty(objectName) || objectName.equals(GoogleCloudStorage.PATH_DELIMITER)) {
      if (allowEmptyObjectName) {
        objectName = "";
      } else {
        throw new IllegalArgumentException(
            "Google Cloud Storage path must include non-empty object name.");
      }
    }

    // We want objectName to look like a traditional file system path,
    // therefore, disallow objectName with consecutive '/' chars.
    for (int i = 0; i < (objectName.length() - 1); i++) {
      if (objectName.charAt(i) == '/' && objectName.charAt(i + 1) == '/') {
        throw new IllegalArgumentException(
            String.format(
                "Google Cloud Storage path must not have consecutive '/' characters, got '%s'",
                objectName));
      }
    }

    // Remove leading '/' if it exists.
    if (objectName.startsWith(GoogleCloudStorage.PATH_DELIMITER)) {
      objectName = objectName.substring(1);
    }

    LOG.debug("validateObjectName -> '{}'", objectName);
    return objectName;
  }

  /**
   * Gets the leaf item of the given path.
   */
  String getItemName(URI path) {
    Preconditions.checkNotNull(path);

    // There is no leaf item for the root path.
    if (path.equals(GCS_ROOT)) {
      return null;
    }

    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, true);

    if (resourceId.isBucket()) {
      return resourceId.getBucketName();
    } else {
      int index;
      if (FileInfo.objectHasDirectoryPath(resourceId.getObjectName())) {
        index = resourceId.getObjectName().lastIndexOf(
            GoogleCloudStorage.PATH_DELIMITER, resourceId.getObjectName().length() - 2);
      } else {
        index = resourceId.getObjectName().lastIndexOf(GoogleCloudStorage.PATH_DELIMITER);
      }
      if (index < 0) {
        return resourceId.getObjectName();
      } else {
        return resourceId.getObjectName().substring(index + 1);
      }
    }
  }

  /**
   * Gets the parent directory of the given path.
   *
   * @param path Path to convert.
   * @return Path of parent directory of the given item or null for root path.
   */
  public URI getParentPath(URI path) {
    return getParentPath(getPathCodec(), path);
  }

  /**
   * Creates FileNotFoundException with a suitable message.
   */
  static FileNotFoundException getFileNotFoundException(URI path) {
    return new FileNotFoundException(
        String.format("Item not found: %s", path));
  }

  /**
   * Retrieve our internal gcs.
   */
  public GoogleCloudStorage getGcs() {
    return gcs;
  }

  /**
   * The PathCodec in use by this file system.
   */
  public PathCodec getPathCodec() {
    return pathCodec;
  }

  /**
   * Validate a URI using the legacy path codec and return a StorageResourceId.
   *
   * @deprecated This method is deprecated as each instance of GCS FS can be configured
   *             with a codec.
   */
  @Deprecated
  public static StorageResourceId validatePathAndGetId(URI uri, boolean allowEmptyObjectNames) {
    return LEGACY_PATH_CODEC.validatePathAndGetId(uri, allowEmptyObjectNames);
  }

  /**
   * Construct a URI using the legacy path codec.
   *
   * @deprecated This method is deprecated as each instance of GCS FS can be configured
   *             with a codec.
   */
  @Deprecated
  public static URI getPath(String bucketName, String objectName, boolean allowEmptyObjectName) {
    return LEGACY_PATH_CODEC.getPath(bucketName, objectName, allowEmptyObjectName);
  }

  /**
   * Construct a URI using the legacy path codec.
   *
   * @deprecated This method is deprecated as each instance of GCS FS can be configured
   *             with a codec.
   */
  @Deprecated
  public static URI getPath(String bucketName) {
    return LEGACY_PATH_CODEC.getPath(
        bucketName, null, true /* allow empty object name */);
  }
  /**
   * Construct a URI using the legacy path codec.
   *
   * @deprecated This method is deprecated as each instance of GCS FS can be configured
   *             with a codec.
   */
  @Deprecated
  public static URI getPath(String bucketName, String objectName) {
    return LEGACY_PATH_CODEC.getPath(
        bucketName, objectName, false /* do not allow empty object */);
  }

  /**
   * Gets the parent directory of the given path.
   *
   * @deprecated This static method is included as a transitional utility and the
   *             instance method variant should be preferred.
   * @param path Path to convert.
   * @return Path of parent directory of the given item or null for root path.
   */
  @Deprecated
  public static URI getParentPath(PathCodec pathCodec, URI path) {
    Preconditions.checkNotNull(path);

    // Root path has no parent.
    if (path.equals(GCS_ROOT)) {
      return null;
    }

    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, true);

    if (resourceId.isBucket()) {
      return GCS_ROOT;
    } else {
      int index;
      if (FileInfo.objectHasDirectoryPath(resourceId.getObjectName())) {
        index = resourceId.getObjectName().lastIndexOf(
            GoogleCloudStorage.PATH_DELIMITER, resourceId.getObjectName().length() - 2);
      } else {
        index = resourceId.getObjectName().lastIndexOf(GoogleCloudStorage.PATH_DELIMITER);
      }
      if (index < 0) {
        return pathCodec.getPath(resourceId.getBucketName(), null, true);
      } else {
        return pathCodec.getPath(resourceId.getBucketName(),
            resourceId.getObjectName().substring(0, index + 1), false);
      }
    }
  }
}
