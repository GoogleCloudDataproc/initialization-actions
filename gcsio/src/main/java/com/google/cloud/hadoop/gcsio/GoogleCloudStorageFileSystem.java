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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toCollection;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage.ListPage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.TimestampUpdatePredicate;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Provides a POSIX like file system layered on top of Google Cloud Storage (GCS).
 *
 * All file system aspects (eg, path) are encapsulated in this class, they are
 * not exposed to the underlying layer. That is, all interactions with the
 * underlying layer are strictly in terms of buckets and objects.
 */
public class GoogleCloudStorageFileSystem {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final ThreadFactory DAEMON_THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("gcsfs-thread-%d").setDaemon(true).build();

  // URI scheme for GCS.
  public static final String SCHEME = "gs";

  // URI of the root path.
  public static final URI GCS_ROOT = URI.create(SCHEME + ":/");

  // 14x faster (20ns vs 280ns) than "^[a-z0-9_.-]+$" regex
  private static final CharMatcher BUCKET_NAME_CHAR_MATCHER =
      CharMatcher.ascii()
          .and(
              CharMatcher.inRange('0', '9')
                  .or(CharMatcher.inRange('a', 'z'))
                  .or(CharMatcher.anyOf("_.-")))
          .precomputed();

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
      comparing(
          URI::toString,
          (as, bs) ->
              (as.length() == bs.length())
                  ? as.compareTo(bs)
                  : Integer.compare(as.length(), bs.length()));

  // Comparator used for sorting a collection of FileInfo items based on path comparison.
  @VisibleForTesting
  static final Comparator<FileInfo> FILE_INFO_PATH_COMPARATOR =
      comparing(FileInfo::getPath, PATH_COMPARATOR);

  /** A PathCodec that maintains compatibility with versions of GCS FS < 1.4.5. */
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
    logger.atFine().log("GCSFS(%s)", options.getCloudStorageOptions().getAppName());
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
            /* keepAliveTime= */ 5, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
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

  /** Convert {@code CreateFileOptions} to {@code CreateObjectOptions}. */
  public static CreateObjectOptions objectOptionsFromFileOptions(CreateFileOptions options) {
    return new CreateObjectOptions(
        options.overwriteExisting(), options.getContentType(), options.getAttributes());
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
    logger.atFine().log("create(%s)", path);
    return create(path, CreateFileOptions.DEFAULT);
  }

  /**
   * Creates and opens an object for writing.
   *
   * @param path Object full path of the form gs://bucket/object-path.
   * @return A channel for writing to the given object.
   * @throws IOException
   */
  public WritableByteChannel create(URI path, CreateFileOptions options) throws IOException {
    logger.atFine().log("create(%s)", path);
    Preconditions.checkNotNull(path, "path could not be null");
    if (FileInfo.isDirectoryPath(path)) {
      throw new IOException(
          String.format("Cannot create a file whose name looks like a directory. Got '%s'", path));
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
    logger.atFine().log("open(%s, %s)", path, readOptions);
    Preconditions.checkNotNull(path);
    checkArgument(!FileInfo.isDirectoryPath(path), "Cannot open a directory for reading: %s", path);

    // Validate the given path. false == do not allow empty object name.
    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, false);
    return gcs.open(resourceId, readOptions);
  }

  /**
   * Deletes one or more items indicated by the given path.
   *
   * <p>If path points to a directory:
   *
   * <ul>
   *   <li>if recursive is true, all items under that path are recursively deleted followed by
   *       deletion of the directory.
   *   <li>else,
   *       <ul>
   *         <li>the directory is deleted if it is empty,
   *         <li>else, an IOException is thrown.
   *       </ul>
   * </ul>
   *
   * <p>The recursive parameter is ignored for a file.
   *
   * @param path Path of the item to delete.
   * @param recursive If true, all sub-items are also deleted.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException
   */
  public void delete(URI path, boolean recursive) throws IOException {
    Preconditions.checkNotNull(path, "path can not be null");
    checkArgument(!path.equals(GCS_ROOT), "Cannot delete root path (%s)", path);
    logger.atFine().log("delete(%s, %s)", path, recursive);

    Future<FileInfo> parentInfoFuture = null;
    if (options.getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled()) {
      ExecutorService parentExecutor = Executors.newSingleThreadExecutor(DAEMON_THREAD_FACTORY);
      parentInfoFuture = parentExecutor.submit(() -> getFileInfo(getParentPath(path)));
      parentExecutor.shutdown();
    }

    // Throw FileNotFoundException if the path does not exist.
    FileInfo fileInfo = getFileInfo(path);
    if (!fileInfo.exists()) {
      throw new FileNotFoundException("Item not found: " + path);
    }

    List<FileInfo> itemsToDelete = new ArrayList<>();
    List<FileInfo> bucketsToDelete = new ArrayList<>();

    // Delete sub-items if it is a directory.
    if (fileInfo.isDirectory()) {
      itemsToDelete =
          recursive
              ? listAllFileInfoForPrefix(fileInfo.getPath())
              : listFileInfo(fileInfo.getPath());
      if (!itemsToDelete.isEmpty() && !recursive) {
        throw new DirectoryNotEmptyException("Cannot delete a non-empty directory.");
      }
    }

    if (fileInfo.getItemInfo().isBucket()) {
      bucketsToDelete.add(fileInfo);
    } else {
      itemsToDelete.add(fileInfo);
    }

    deleteInternal(itemsToDelete, bucketsToDelete);

    // if we deleted a bucket, then there no need to update timestamps
    if (bucketsToDelete.isEmpty()) {
      List<URI> itemsToDeleteNames =
          itemsToDelete.stream().map(FileInfo::getPath).collect(toCollection(ArrayList::new));
      // Any path that was deleted, we should update the parent except for parents we also deleted
      tryUpdateTimestampsForParentDirectories(itemsToDeleteNames, itemsToDeleteNames);
    }

    if (options.getCloudStorageOptions().isAutoRepairImplicitDirectoriesEnabled()) {
      FileInfo parentInfo;
      try {
        parentInfo = checkNotNull(parentInfoFuture, "parentInfoFuture should not be null").get();
      } catch (InterruptedException e) {
        throw new IOException("Failed to get parent info for: " + path, e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new IOException("Failed to get parent info for: " + path, e.getCause());
      }
      createDirectoryIfDoesNotExist(parentInfo);
    }
  }

  /** Deletes all items in the given path list followed by all bucket items. */
  private void deleteInternal(List<FileInfo> itemsToDelete, List<FileInfo> bucketsToDelete)
      throws IOException {
    // TODO(user): We might need to separate out children into separate batches from parents to
    // avoid deleting a parent before somehow failing to delete a child.

    // Delete children before their parents.
    //
    // Note: we modify the input list, which is ok for current usage.
    // We should make a copy in case that changes in future.
    itemsToDelete.sort(FILE_INFO_PATH_COMPARATOR.reversed());

    if (!itemsToDelete.isEmpty()) {
      List<StorageResourceId> objectsToDelete = new ArrayList<>(itemsToDelete.size());
      for (FileInfo fileInfo : itemsToDelete) {
        // TODO(b/110833109): populate generation ID in StorageResourceId when listing infos?
        objectsToDelete.add(
            new StorageResourceId(
                fileInfo.getItemInfo().getBucketName(),
                fileInfo.getItemInfo().getObjectName(),
                fileInfo.getItemInfo().getContentGeneration()));
      }
      gcs.deleteObjects(objectsToDelete);
    }

    if (!bucketsToDelete.isEmpty()) {
      List<String> bucketNames = new ArrayList<>(bucketsToDelete.size());
      for (FileInfo bucketInfo : bucketsToDelete) {
        StorageResourceId resourceId = bucketInfo.getItemInfo().getResourceId();
        gcs.waitForBucketEmpty(resourceId.getBucketName());
        bucketNames.add(resourceId.getBucketName());
      }
      if (options.enableBucketDelete()) {
        gcs.deleteBuckets(bucketNames);
      } else {
        logger.atInfo().log(
            "Skipping deletion of buckets because enableBucketDelete is false: %s", bucketNames);
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
    logger.atFine().log("exists(%s)", path);
    return getFileInfo(path).exists();
  }

  /**
   * Creates a directory at the specified path. Also creates any parent directories as necessary.
   * Similar to 'mkdir -p' command.
   *
   * @param path Path of the directory to create.
   * @throws IOException
   */
  public void mkdirs(URI path) throws IOException {
    logger.atFine().log("mkdirs(%s)", path);
    Preconditions.checkNotNull(path);

    if (path.equals(GCS_ROOT)) {
      // GCS_ROOT directory always exists, no need to go through the rest of the method.
      return;
    }

    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, true);
    resourceId = FileInfo.convertToDirectoryPath(resourceId);

    // Create a list of all intermediate paths.
    // For example,
    // gs://foo/bar/zoo/ => (gs://foo/, gs://foo/bar/, gs://foo/bar/zoo/)
    //
    // We also need to find out if any of the subdir path item exists as a file
    // therefore, also create a list of file equivalent of each subdir path.
    // For example,
    // gs://foo/bar/zoo/ => (gs://foo/bar, gs://foo/bar/zoo)
    List<String> subdirs = getSubDirs(resourceId.getObjectName());
    List<StorageResourceId> itemIds = new ArrayList<>(subdirs.size() * 2 + 1);
    for (String subdir : subdirs) {
      itemIds.add(new StorageResourceId(resourceId.getBucketName(), subdir));
      if (!Strings.isNullOrEmpty(subdir)) {
        itemIds.add(
            new StorageResourceId(resourceId.getBucketName(), FileInfo.convertToFilePath(subdir)));
      }
    }
    // Add the bucket portion.
    itemIds.add(new StorageResourceId(resourceId.getBucketName()));
    logger.atFine().log("mkdirs: items: %s", itemIds);

    List<GoogleCloudStorageItemInfo> itemInfos = gcs.getItemInfos(itemIds);

    // Each intermediate path must satisfy one of the following conditions:
    // -- it does not exist or
    // -- if it exists, it is a directory
    //
    // If any of the intermediate paths violates these requirements then
    // bail out early so that we do not end up with partial set of
    // created sub-directories. It is possible that the status of intermediate
    // paths can change after we make this check therefore this is a
    // good faith effort and not a guarantee.
    GoogleCloudStorageItemInfo bucketInfo = null;
    List<StorageResourceId> subdirsToCreate = new ArrayList<>(subdirs.size());
    for (GoogleCloudStorageItemInfo info : itemInfos) {
      if (info.isBucket()) {
        checkState(bucketInfo == null, "bucketInfo should be null");
        bucketInfo = info;
      } else if (info.getResourceId().isDirectory() && !info.exists()) {
        subdirsToCreate.add(info.getResourceId());
      } else if (!info.getResourceId().isDirectory() && info.exists()) {
        throw new FileAlreadyExistsException(
            "Cannot create directories because of existing file: " + info.getResourceId());
      }
    }

    if (!checkNotNull(bucketInfo, "bucketInfo should not be null").exists()) {
      gcs.create(bucketInfo.getBucketName());
    }
    gcs.createEmptyObjects(subdirsToCreate);

    // Update parent directories, but not the ones we just created because we just created them.
    List<URI> createdDirectories =
        subdirsToCreate.stream()
            .map(s -> pathCodec.getPath(s.getBucketName(), s.getObjectName(), false))
            .collect(toImmutableList());
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
    logger.atFine().log("rename(%s, %s)", src, dst);
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
    FileInfo dstParentInfo = dstParent == null ? null : fileInfo.get(2);

    // Make sure paths match what getFileInfo() returned (it can add / at the end).
    src = srcInfo.getPath();
    dst = dstInfo.getPath();

    // Throw if the source file does not exist.
    if (!srcInfo.exists()) {
      throw new FileNotFoundException("Item not found: " + src);
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
            sources, uri -> StorageResourceId.fromObjectName(uri.toString()).getObjectName());
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
          srcResourceId.getBucketName(), ImmutableList.of(srcResourceId.getObjectName()),
          dstResourceId.getBucketName(), ImmutableList.of(dstResourceId.getObjectName()));

      tryUpdateTimestampsForParentDirectories(ImmutableList.of(dst), ImmutableList.<URI>of());

      // TODO(b/110833109): populate generation ID in StorageResourceId when getting info
      gcs.deleteObjects(
          ImmutableList.of(
              new StorageResourceId(
                  srcInfo.getItemInfo().getBucketName(),
                  srcInfo.getItemInfo().getObjectName(),
                  srcInfo.getItemInfo().getContentGeneration())));

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
    Map<FileInfo, URI> srcToDstItemNames = new TreeMap<>(FILE_INFO_PATH_COMPARATOR);
    Map<FileInfo, URI> srcToDstMarkerItemNames = new TreeMap<>(FILE_INFO_PATH_COMPARATOR);

    // List of individual paths to rename;
    // we will try to carry out the copies in this list's order.
    List<FileInfo> srcItemInfos = listAllFileInfoForPrefix(srcInfo.getPath());

    // Create the destination directory.
    dst = FileInfo.convertToDirectoryPath(pathCodec, dst);
    mkdir(dst);

    // Create a list of sub-items to copy.
    String prefix = srcInfo.getPath().toString();
    for (FileInfo srcItemInfo : srcItemInfos) {
      String relativeItemName = srcItemInfo.getPath().toString().substring(prefix.length());
      URI dstItemName = dst.resolve(relativeItemName);
      if (markerFilePattern != null && markerFilePattern.matcher(relativeItemName).matches()) {
        srcToDstMarkerItemNames.put(srcItemInfo, dstItemName);
      } else {
        srcToDstItemNames.put(srcItemInfo, dstItemName);
      }
    }

    // First, copy all items except marker items
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

    List<FileInfo> bucketsToDelete = new ArrayList<>(1);
    List<FileInfo> srcItemsToDelete = new ArrayList<>(srcToDstItemNames.size() + 1);
    srcItemsToDelete.addAll(srcToDstItemNames.keySet());
    if (srcInfo.getItemInfo().isBucket()) {
      bucketsToDelete.add(srcInfo);
    } else {
      // If src is a directory then srcItemInfos does not contain its own name,
      // therefore add it to the list before we delete items in the list.
      srcItemsToDelete.add(srcInfo);
    }

    // First delete marker files from the src
    deleteInternal(new ArrayList<>(srcToDstMarkerItemNames.keySet()), new ArrayList<>());
    // Then delete rest of the items that we successfully copied.
    deleteInternal(srcItemsToDelete, bucketsToDelete);

    // if we deleted a bucket, then there no need to update timestamps
    if (bucketsToDelete.isEmpty()) {
      List<URI> srcItemNames =
          srcItemInfos.stream().map(FileInfo::getPath).collect(toCollection(ArrayList::new));
      // Any path that was deleted, we should update the parent except for parents we also deleted
      tryUpdateTimestampsForParentDirectories(srcItemNames, srcItemNames);
    }
  }

  /** Copies items in given map that maps source items to destination items. */
  private void copyInternal(Map<FileInfo, URI> srcToDstItemNames) throws IOException {
    if (srcToDstItemNames.isEmpty()) {
      return;
    }

    String srcBucketName = null;
    String dstBucketName = null;
    List<String> srcObjectNames = new ArrayList<>(srcToDstItemNames.size());
    List<String> dstObjectNames = new ArrayList<>(srcToDstItemNames.size());

    // Prepare list of items to copy.
    for (Map.Entry<FileInfo, URI> srcToDstItemName : srcToDstItemNames.entrySet()) {
      StorageResourceId srcResourceId = srcToDstItemName.getKey().getItemInfo().getResourceId();
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
    logger.atFine().log("listFileNames(%s)", path);
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
            logger.atFine().log("listFileNames: added: %s", childPath);
          }
        } else {
          // A null delimiter asks GCS to return all objects with a given prefix,
          // regardless of their 'directory depth' relative to the prefix;
          // that is what we want for a recursive list. On the other hand,
          // when a delimiter is specified, only items with relative depth
          // of 1 are returned.
          String delimiter = recursive ? null : PATH_DELIMITER;

          GoogleCloudStorageItemInfo itemInfo = fileInfo.getItemInfo();
          // Obtain paths of children.
          childNames =
              gcs.listObjectNames(itemInfo.getBucketName(), itemInfo.getObjectName(), delimiter);

          // Obtain path for each child.
          for (String childName : childNames) {
            URI childPath = pathCodec.getPath(itemInfo.getBucketName(), childName, false);
            paths.add(childPath);
            logger.atFine().log("listFileNames: added: %s", childPath);
          }
        }
      }
    } else {
      paths.add(path);
      logger.atFine().log(
          "listFileNames: added single original path since !isDirectory(): %s", path);
    }

    return paths;
  }

  /**
   * Attempts to create the directory object explicitly for provided {@code info} if it doesn't
   * already exist as a directory object.
   */
  private void createDirectoryIfDoesNotExist(FileInfo info) {
    Preconditions.checkNotNull(info, "info can not be null");
    logger.atFine().log("createDirectoryIfDoesNotExist(%s)", info.getPath());
    StorageResourceId resourceId = info.getItemInfo().getResourceId();

    if (info.exists()
        || resourceId.isRoot()
        || resourceId.isBucket()
        || PATH_DELIMITER.equals(resourceId.getObjectName())) {
      return;
    }

    checkState(resourceId.isDirectory(), "'%s' should be a directory", resourceId);

    // Note that we do not update parent directory timestamps. The idea is that:
    // 1) directory repair isn't a user-invoked action
    // 2) directory repair shouldn't be thought of "creating" directories, instead it drops
    //    markers to help GCSFS and GHFS find directories that already "existed"
    // 3) it's extra RPCs on top of the list and create empty object RPCs
    try {
      gcs.createEmptyObject(resourceId);
      logger.atInfo().log("Successfully repaired '%s' directory.", resourceId);
    } catch (IOException e) {
      logger.atWarning().withCause(e).log("Failed to repair '%s' directory", resourceId);
    }
  }

  /**
   * Equivalent to a recursive listing of {@code prefix}, except that {@code prefix} doesn't have to
   * represent an actual object but can just be a partial prefix string. The 'authority' component
   * of the {@code prefix} <b>must</b> be the complete authority, however; we can only list prefixes
   * of <b>objects</b>, not buckets.
   *
   * @param prefix the prefix to use to list all matching objects.
   */
  public List<FileInfo> listAllFileInfoForPrefix(URI prefix) throws IOException {
    logger.atFine().log("listAllFileInfoForPrefixPage(%s)", prefix);
    StorageResourceId prefixId = getPrefixId(prefix);
    List<GoogleCloudStorageItemInfo> itemInfos =
        gcs.listObjectInfo(
            prefixId.getBucketName(), prefixId.getObjectName(), /* delimiter= */ null);
    List<FileInfo> fileInfos = FileInfo.fromItemInfos(pathCodec, itemInfos);
    fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
    return fileInfos;
  }

  /**
   * Equivalent to {@link #listAllFileInfoForPrefix} but returns {@link FileInfo}s listed by single
   * request (1 page).
   *
   * @param prefix the prefix to use to list all matching objects.
   * @param pageToken the page token to list
   */
  public ListPage<FileInfo> listAllFileInfoForPrefixPage(URI prefix, String pageToken)
      throws IOException {
    logger.atFine().log("listAllFileInfoForPrefixPage(%s, %s)", prefix, pageToken);
    StorageResourceId prefixId = getPrefixId(prefix);
    ListPage<GoogleCloudStorageItemInfo> itemInfosPage =
        gcs.listObjectInfoPage(
            prefixId.getBucketName(), prefixId.getObjectName(), /* delimiter= */ null, pageToken);
    List<FileInfo> fileInfosPage = FileInfo.fromItemInfos(pathCodec, itemInfosPage.getItems());
    fileInfosPage.sort(FILE_INFO_PATH_COMPARATOR);
    return new ListPage<>(fileInfosPage, itemInfosPage.getNextPageToken());
  }

  private StorageResourceId getPrefixId(URI prefix) {
    Preconditions.checkNotNull(prefix, "prefix could not be null");

    StorageResourceId prefixId = pathCodec.validatePathAndGetId(prefix, true);
    Preconditions.checkArgument(
        !prefixId.isRoot(), "prefix must not be global root, got '%s'", prefix);

    return prefixId;
  }

  /**
   * If the given path points to a directory then the information about its children is returned,
   * otherwise information about the given file is returned.
   *
   * <p>Note: This function is expensive to call, especially for a directory with many children. Use
   * the alternative {@link GoogleCloudStorageFileSystem#listFileNames(FileInfo)} if you only need
   * names of children and no other attributes.
   *
   * @param path Given path.
   * @return Information about a file or children of a directory.
   * @throws FileNotFoundException if the given path does not exist.
   * @throws IOException
   */
  public List<FileInfo> listFileInfo(URI path) throws IOException {
    Preconditions.checkNotNull(path, "path can not be null");
    logger.atFine().log("listFileInfo(%s)", path);

    StorageResourceId pathId = pathCodec.validatePathAndGetId(path, true);
    StorageResourceId dirId =
        pathCodec.validatePathAndGetId(FileInfo.convertToDirectoryPath(pathCodec, path), true);

    // To improve performance start to list directory items right away.
    ExecutorService dirExecutor = Executors.newSingleThreadExecutor(DAEMON_THREAD_FACTORY);
    try {
      Future<GoogleCloudStorageItemInfo> dirFuture =
          dirExecutor.submit(() -> gcs.getItemInfo(dirId));
      dirExecutor.shutdown();

      if (!pathId.isDirectory()) {
        GoogleCloudStorageItemInfo pathInfo = gcs.getItemInfo(pathId);
        if (pathInfo.exists()) {
          List<FileInfo> listedInfo = new ArrayList<>();
          listedInfo.add(FileInfo.fromItemInfo(pathCodec, pathInfo));
          return listedInfo;
        }
      }

      try {
        GoogleCloudStorageItemInfo dirInfo = dirFuture.get();
        List<GoogleCloudStorageItemInfo> dirItemInfos =
            dirId.isRoot()
                ? gcs.listBucketInfo()
                : gcs.listObjectInfo(dirId.getBucketName(), dirId.getObjectName(), PATH_DELIMITER);
        if (!dirInfo.exists() && dirItemInfos.isEmpty()) {
          throw new FileNotFoundException("Item not found: " + path);
        }

        List<FileInfo> fileInfos = FileInfo.fromItemInfos(pathCodec, dirItemInfos);
        fileInfos.sort(FILE_INFO_PATH_COMPARATOR);
        return fileInfos;
      } catch (InterruptedException | ExecutionException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException(String.format("Failed to listFileInfo for '%s'", path), e);
      }
    } finally {
      dirExecutor.shutdownNow();
    }
  }

  /**
   * Gets information about the given path item.
   *
   * @param path The path we want information about.
   * @return Information about the given path item.
   * @throws IOException
   */
  public FileInfo getFileInfo(URI path) throws IOException {
    logger.atFine().log("getFileInfo(%s)", path);
    checkArgument(path != null, "path must not be null");
    // Validate the given path. true == allow empty object name.
    // One should be able to get info about top level directory (== bucket),
    // therefore we allow object name to be empty.
    StorageResourceId resourceId = pathCodec.validatePathAndGetId(path, true);
    FileInfo fileInfo = FileInfo.fromItemInfo(pathCodec, getFileInfoInternal(resourceId));
    logger.atFine().log("getFileInfo: %s", fileInfo);
    return fileInfo;
  }

  /** @see #getFileInfo(URI) */
  private GoogleCloudStorageItemInfo getFileInfoInternal(StorageResourceId resourceId)
      throws IOException {
    if (resourceId.isRoot() || resourceId.isBucket()) {
      return gcs.getItemInfo(resourceId);
    }
    StorageResourceId dirId = FileInfo.convertToDirectoryPath(resourceId);
    // To improve performance get directory and its child right away.
    ExecutorService dirExecutor = Executors.newSingleThreadExecutor(DAEMON_THREAD_FACTORY);
    try {
      Future<GoogleCloudStorageItemInfo> dirFuture =
          resourceId.isDirectory()
              ? Futures.immediateFuture(gcs.getItemInfo(resourceId))
              : dirExecutor.submit(() -> gcs.getItemInfo(dirId));
      dirExecutor.shutdown();

      if (!resourceId.isDirectory()) {
        GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(resourceId);
        if (itemInfo.exists()) {
          return itemInfo;
        }
      }

      try {
        GoogleCloudStorageItemInfo dirInfo = dirFuture.get();
        if (dirInfo.exists()) {
          return dirInfo;
        }

        List<String> dirChild =
            gcs.listObjectNames(dirId.getBucketName(), dirId.getObjectName(), PATH_DELIMITER, 1);
        if (dirChild.isEmpty()) {
          return GoogleCloudStorageItemInfo.createNotFound(resourceId);
        }

        return gcs.getOptions().isInferImplicitDirectoriesEnabled()
            ? GoogleCloudStorageItemInfo.createInferredDirectory(dirId)
            : GoogleCloudStorageItemInfo.createNotFound(dirId);
      } catch (ExecutionException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException(String.format("Filed to get file info for '%s'", resourceId), e);
      }
    } finally {
      dirExecutor.shutdownNow();
    }
  }

  /**
   * Gets information about each path in the given list; more efficient than calling getFileInfo()
   * on each path individually in a loop.
   *
   * @param paths List of paths.
   * @return Information about each path in the given list.
   * @throws IOException
   */
  public List<FileInfo> getFileInfos(List<URI> paths) throws IOException {
    checkArgument(paths != null, "paths must not be null");
    logger.atFine().log("getFileInfos(%d paths)", paths.size());

    if (paths.size() == 1) {
      return new ArrayList<>(Collections.singleton(getFileInfo(paths.get(0))));
    }

    int maxThreads = gcs.getOptions().getBatchThreads();
    ExecutorService fileInfoExecutor =
        maxThreads == 0
            ? MoreExecutors.newDirectExecutorService()
            : Executors.newFixedThreadPool(
                Math.min(maxThreads, paths.size()), DAEMON_THREAD_FACTORY);
    try {
      List<Future<FileInfo>> infoFutures = new ArrayList<>(paths.size());
      for (URI path : paths) {
        infoFutures.add(fileInfoExecutor.submit(() -> getFileInfo(path)));
      }
      fileInfoExecutor.shutdown();

      List<FileInfo> infos = new ArrayList<>(paths.size());
      for (Future<FileInfo> infoFuture : infoFutures) {
        try {
          infos.add(infoFuture.get());
        } catch (InterruptedException | ExecutionException e) {
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          throw new IOException(
              String.format("Failed to getFileInfos for %d paths", paths.size()), e);
        }
      }
      return infos;
    } finally {
      fileInfoExecutor.shutdownNow();
    }
  }

  /** Releases resources used by this instance. */
  public void close() {
    if (gcs != null) {
      logger.atFine().log("close()");
      try {
        gcs.close();
      } finally {
        gcs = null;

        if (updateTimestampsExecutor != null) {
          updateTimestampsExecutor.shutdown();
          try {
            if (!updateTimestampsExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
              logger.atWarning().log("Forcibly shutting down timestamp update thread pool.");
              updateTimestampsExecutor.shutdownNow();
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.atFine().withCause(e).log(
                "Failed to await termination: forcibly shutting down timestamp update thread pool");
            updateTimestampsExecutor.shutdownNow();
          } finally {
            updateTimestampsExecutor = null;
          }
        }
      }
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

    logger.atFine().log("mkdir(%s)", path);
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
    logger.atFine().log(
        "updateTimestampsForParentDirectories(%s, %s)", modifiedObjects, excludedParents);

    TimestampUpdatePredicate updatePredicate =
        options.getShouldIncludeInTimestampUpdatesPredicate();
    Set<URI> excludedParentPathsSet = new HashSet<>(excludedParents);

    Set<URI> parentUrisToUpdate = Sets.newHashSetWithExpectedSize(modifiedObjects.size());
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
      logger.atFine().log("All paths were excluded from directory timestamp updating.");
    }
  }

  /**
   * For each listed modified object, attempt to update the modification time of the parent
   * directory.
   *
   * <p>This method will log & swallow exceptions thrown by the GCSIO layer.
   *
   * @param modifiedObjects The objects that have been modified
   * @param excludedParents A list of parent directories that we shouldn't attempt to update.
   */
  protected void tryUpdateTimestampsForParentDirectories(
      final List<URI> modifiedObjects, final List<URI> excludedParents) {
    logger.atFine().log(
        "tryUpdateTimestampsForParentDirectories(%s, %s)", modifiedObjects, excludedParents);

    // If we're calling tryUpdateTimestamps, we don't actually care about the results. Submit
    // these requests via a background thread and continue on.
    try {
      @SuppressWarnings("unused") // go/futurereturn-lsc
      Future<?> possiblyIgnoredError =
          updateTimestampsExecutor.submit(
              () -> {
                try {
                  updateTimestampsForParentDirectories(modifiedObjects, excludedParents);
                } catch (IOException ioe) {
                  logger.atFine().withCause(ioe).log(
                      "Exception caught when trying to update parent directory timestamps.");
                }
              });
    } catch (RejectedExecutionException ree) {
      logger.atFine().withCause(ree).log(
          "Exhausted thread pool and queue space while updating parent timestamps");
    }
  }

  /**
   * For objects whose name looks like a path (foo/bar/zoo), returns intermediate sub-paths.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>foo/bar/zoo => returns: (foo/, foo/bar/)
   *   <li>foo => returns: ()
   * </ul>
   *
   * @param objectName Name of an object.
   * @return List of sub-directory like paths.
   */
  static List<String> getSubDirs(String objectName) {
    List<String> subdirs = new ArrayList<>();
    if (!Strings.isNullOrEmpty(objectName)) {
      int currentIndex = 0;
      while (currentIndex < objectName.length()) {
        int index = objectName.indexOf(PATH_DELIMITER, currentIndex);
        if (index < 0) {
          break;
        }
        subdirs.add(objectName.substring(0, index + PATH_DELIMITER.length()));
        currentIndex = index + PATH_DELIMITER.length();
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

    if (!BUCKET_NAME_CHAR_MATCHER.matchesAllOf(bucketName)) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid bucket name '%s': bucket name must contain only 'a-z0-9_.-' characters.",
              bucketName));
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
    logger.atFine().log("validateObjectName('%s', %s)", objectName, allowEmptyObjectName);

    if (isNullOrEmpty(objectName) || objectName.equals(PATH_DELIMITER)) {
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
    if (objectName.startsWith(PATH_DELIMITER)) {
      objectName = objectName.substring(1);
    }

    logger.atFine().log("validateObjectName -> '%s'", objectName);
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
    }

    int index;
    String objectName = resourceId.getObjectName();
    if (FileInfo.objectHasDirectoryPath(objectName)) {
      index = objectName.lastIndexOf(PATH_DELIMITER, objectName.length() - 2);
    } else {
      index = objectName.lastIndexOf(PATH_DELIMITER);
    }
    return index < 0 ? objectName : objectName.substring(index + 1);
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
    }

    int index;
    String objectName = resourceId.getObjectName();
    if (FileInfo.objectHasDirectoryPath(objectName)) {
      index = objectName.lastIndexOf(PATH_DELIMITER, objectName.length() - 2);
    } else {
      index = objectName.lastIndexOf(PATH_DELIMITER);
    }
    return index < 0
        ? pathCodec.getPath(resourceId.getBucketName(), null, true)
        : pathCodec.getPath(resourceId.getBucketName(), objectName.substring(0, index + 1), false);
  }
}
