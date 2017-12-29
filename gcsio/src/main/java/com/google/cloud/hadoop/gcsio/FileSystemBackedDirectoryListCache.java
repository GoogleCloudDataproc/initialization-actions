/**
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileSystemBackedDirectoryListCache mirrors all GCS directory and file creation into a matching
 * tree of directories + empty files inside a configured local filesystem, as accessed via
 * java.nio.Path and java.io.File, all under a base directory configured at construction-time.
 * If a regular local filesystem path is used, then this cache thus enables immediate list
 * consistency for all processes that use cooperating GCS-connector classes on the same machine;
 * alternatively, if the path is an NFS mount point, then it is possible to enforce cluster-wide
 * immediate list consistency as long as all cooperating nodes configure the same NFS directory.
 * <p>
 * This class does *not* support caching the GoogleCloudStorageItemInfo alongside filenames;
 * it is strictly for supplementing listed filenames, and any returned CacheEntry should not
 * be expected to contain a GoogleCloudStorageItemInfo regardless of info-expiration settings.
 * <p>
 * This class is thread-safe.
 */
public class FileSystemBackedDirectoryListCache extends DirectoryListCache {
  /**
   * Defines a basic run(File) method for internal usage, e.g. efficiently traversing large
   * directory trees without having to assemble a complete list.
   */
  private static interface FileCallback {
    void run(File file) throws IOException;
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemBackedDirectoryListCache.class);

  // Instead of opting for explicitly file-locking, we recognize that garbage-collection only
  // occurs rarely compared to cache additions, and that in general overwriting existing
  // cache entries is 'safe', since the cache is strictly supplemental to real GCS "list"
  // results. So, failing to create a pre-existing file is safe, deleting a nonexistent file
  // is safe, and we only worry about deletes interfering with a 'create' by deleting a
  // parent directory after the 'create' already checked for the parent directory's existence.
  // In such a case, we just peform hard retries at the putResourceId level, where one of the
  // concurrent "create" attempts should successfully create the parent directories, and as
  // long as the cache expiration isn't instantaneous, the parent directory won't be deleted
  // until much later.
  static final int MAX_RETRIES_FOR_CREATE = 5;

  // The root at which we mirror all GCS objects; buckets are directories directly under this
  // basePath, and objects are located under those bucket directories. Path validation is
  // designed to prevent accidentally escaping up to any parent or sibling directory of this
  // basePath.
  // Generally not mutable, except some test setups need to have a single long-lived instance
  // that can reconfigured across different tests.
  private Path basePath = null;

  // Allows a user of this class to inject a listener that gets called immediately prior
  // to any createNewFile() or mkdir() invocation.
  private Function<StorageResourceId, Void> createMirrorFileListener = null;

  /**
   * Returns an instance not bound to a basePath for testing purposes.
   */
  @VisibleForTesting
  public static FileSystemBackedDirectoryListCache getUninitializedInstanceForTest() {
    return new FileSystemBackedDirectoryListCache();
  }

  /**
   * @param basePathStr The absolute path under which to place all mirrored directory trees;
   *     Paths.get(basePathStr).isAbsolute() must return true.
   */
  public FileSystemBackedDirectoryListCache(String basePathStr) {
    setBasePath(basePathStr);
  }

  /**
   * No-arg constructor that should only ever be used in tests.
   */
  private FileSystemBackedDirectoryListCache() {
  }

  /**
   * We use a real filesystem as our authoritative cache, and thus CacheEntries are not shared
   * references.
   */
  @Override
  public boolean supportsCacheEntryByReference() {
    return false;
  }

  /**
   * Since our backing store has no notion of "implicit" directories, automatically cache directory
   * objects for 'implicit' directories, by virtue of having to create those real directories
   * to insert the child object at all.
   */
  @Override
  public boolean containsEntriesForImplicitDirectories() {
    return true;
  }

  /**
   * Helper to resolve {@code resourceId} against this instance's {@code basePath}, appropriately
   * resolving bucket and object names against the basePath as necessary after validating
   * the bucket/object names to ensure we don't touch unexpected parts of the local filesystem.
   */
  @VisibleForTesting
  Path getMirrorPath(StorageResourceId resourceId) {
    Preconditions.checkArgument(!Paths.get(resourceId.getBucketName()).isAbsolute(),
        "Bucket name must not look like an absolute path for resourceId '%s'", resourceId);
    Path resolvedPath = basePath.resolve(resourceId.getBucketName());
    if (resourceId.isStorageObject()) {
      Preconditions.checkArgument(!Paths.get(resourceId.getObjectName()).isAbsolute(),
          "Object name must not look like an absolute path for resourceId '%s'", resourceId);
      resolvedPath = resolvedPath.resolve(resourceId.getObjectName());
    }

    // Fold any '.' or '..' elements that may have been introduced; if any folding happens,
    // reject the path. Any legitimate cases of folding relative components should have already
    // occurred in a higher layer.
    Path normalizedPath = resolvedPath.normalize();

    // Since Path.resolve doesn't pre-emptively collapse ".." elements, we can detect the presence
    // of ".." elements in the resourceId if the normalizedPath != resolvedPath.
    Preconditions.checkArgument(normalizedPath.toString().equals(resolvedPath.toString()),
        "Normalized path '%s' doesn't match raw resolved path '%s', resolvedPath; "
        + "relative components are not allowed.", normalizedPath, resolvedPath);
    Preconditions.checkState(normalizedPath.startsWith(basePath),
        "Somehow got a normalized resolved path '%s' that no longer starts with '%s'!",
        normalizedPath, basePath);

    return normalizedPath;
  }

  /**
   * Helper for checking against the {@code mirrorFile} corresponding to {@code resourceId}
   * once the caller has already performed the logic for resolving {@mirrorFile}, just to do
   * the boilerplate of checking for existence and whether isDirectory of the file matches
   * the resourceId.
   *
   * @param resourceId The StorageResourceId used as a cache key.
   * @param mirrorFile The File corresponding to {@code resourceId}.
   * @param allowDirectoryMismatch If true, directory-mismatch will return 'null', if false,
   *     directory-mismatch will throw an IOException.
   */
  private CacheEntry getCacheEntryInternal(
      StorageResourceId resourceId, File mirrorFile, boolean allowDirectoryMismatch)
      throws IOException {
    if (!mirrorFile.exists()) {
      return null;
    }

    // Since java path resolution removes trailing slahes, to preserve the correct behavior
    // of returning null for a directory object when we've inserted a file-object or vice-versa
    // we explicitly compare the "isDirectory" status of the two. In general, this case should
    // be rare.
    boolean mirrorIsDirectory = mirrorFile.isDirectory();
    boolean resourceIdIsDirectory = resourceId.isDirectory();
    if (mirrorIsDirectory != resourceIdIsDirectory) {
      String errorMessage = String.format(
          "Existing mirrorFile and resourceId don't match isDirectory status! "
          + "'%s' (dir: '%s') vs '%s' (dir: '%s')",
          mirrorFile, mirrorIsDirectory, resourceId, resourceIdIsDirectory);
      if (allowDirectoryMismatch) {
        LOG.info(errorMessage);
        return null;
      } else {
        throw new IOException(errorMessage);
      }
    }

    return new CacheEntry(resourceId, mirrorFile.lastModified());
  }

  /**
   * Helper to calculate directly based on StorageResourceId the implied parent-directory's
   * StorageResourceId.
   *
   * @param resourceId A StorageObject for which to calculate a parent StorageResourceId,
   *     must not be root or a plain bucket.
   */
  private StorageResourceId getParentResourceId(StorageResourceId resourceId) {
    Preconditions.checkArgument(resourceId.isStorageObject(),
        "resourceId must be a StorageObject, got '%s'", resourceId);

    // Both "foo/bar" and "foo/bar/" inputs will map to "foo" as the parent, which we then
    // append the directory suffix to form "foo/" as the parent objectName. "foo" would map
    // to null in Path.getParent(), and we'd report the parent as the plain bucket.
    Path parentObjectPath = Paths.get(resourceId.getObjectName()).getParent();
    StorageResourceId parentResourceId;
    if (parentObjectPath == null) {
      // Parent is bucket.
      parentResourceId = new StorageResourceId(resourceId.getBucketName());
    } else {
      // Parent is object.
      parentResourceId = new StorageResourceId(
          resourceId.getBucketName(),
          StorageResourceId.convertToDirectoryPath(parentObjectPath.toString()));
    }
    return parentResourceId;
  }

  /**
   * Helper to check for existence of one-level-up parent directory, and if it
   * doesn't exist, explicitly putResourceId with the StorageResourceId
   * corresponding to that parent directory, which will itself recursively
   * ensure parent directories until all parent directories are created. We use
   * this instead of a simple "mkdirs" because we need to control
   * lastModifiedTime at every level.
   *
   * @throws IOException if any general IOException occurs or if the parent exists already as
   *     a non-directory file.
   */
  private void ensureParentDirectoriesExist(StorageResourceId resourceId, File mirrorFile)
      throws IOException {
    // Make sure the parent directory exists first; call recursively instead of just using
    // mkdirs() because we need to be able to control the lastModified time of the parentDirectory
    // at every stage.
    File parentFile = mirrorFile.getParentFile();
    if (!parentFile.exists()) {
      // If we're at the 'bucket' level, its parent should be the basePath, which we should have
      // already verified exists in the constructor.
      Preconditions.checkState(
          !resourceId.isBucket(), "Parent directory doesn't exist for bucket '%s'", resourceId);

      // Since we're not a bucket, there's a nonempty objectName; first try to derive parent
      // directly from resourceId.
      StorageResourceId parentResourceId = getParentResourceId(resourceId);

      // Sanity check that the notion of StorageResourceId parents matches with our mirrored
      // filesystem's notion of parents.
      Path parentPathFromFile = parentFile.toPath();
      Path parentPathFromResourceId = getMirrorPath(parentResourceId);
      Preconditions.checkState(parentPathFromFile.equals(parentPathFromResourceId),
          "parentFile.toPath() '%s' doesn't match getMirrorPath of parentResourceId '%s'",
          parentPathFromFile, parentPathFromResourceId);

      // Putting the parentResourceId also creates the parent directory so that we can actually
      // add the child object.
      LOG.debug("Caching nonexistent parent resourceId '{}', which is path '{}'",
          parentResourceId, parentFile);
      putResourceId(parentResourceId);
    }

    Preconditions.checkState(parentFile.exists(),
        "Parent file '%s' still doesn't exist adding resourceId '%s' with file '%s'",
        parentFile, resourceId, mirrorFile);

    if (!parentFile.isDirectory()) {
      // Throw IOException rather than Preconditions.checkState because it's not an internally
      // controllable state; the user can potentially get into this situation by creating
      // a file of the same name as an implied directory.
      throw new IOException(String.format(
          "Parent file '%s' already exists and isn't a directory, trying to add resourceId "
          + "'%s' with file '%s'", parentFile, resourceId, mirrorFile));
    }
  }

  /**
   * Helper to actually create the {@code mirrorFile} corresponding to {@code resourceId},
   * assuming parent directories have already been adequately created.
   */
  private void createMirrorFileInternal(StorageResourceId resourceId, File mirrorFile)
      throws IOException {
    if (createMirrorFileListener != null) {
      createMirrorFileListener.apply(resourceId);
    }

    // Since java Path resolution removes trailing slashes, we rely on the StorageResourceId
    // representation to indicate whether we're dealing with a directory.
    if (resourceId.isDirectory()) {
      try {
        LOG.debug("Creating directory '{}' for resourceId '{}'", mirrorFile, resourceId);
        Files.createDirectory(mirrorFile.toPath());
        LOG.debug("Successfully created directory '{}' for resourceId '{}'",
            mirrorFile, resourceId);
      } catch (FileAlreadyExistsException faee) {
        LOG.debug("Directory '{}' already exists, with FileAlreadyExistsException message: '{}'",
            mirrorFile, faee.getMessage());
        if (!mirrorFile.isDirectory()) {
          throw new IOException(String.format(
              "Mirror file '%s' for resourceId '%s' already exists and isn't a directory!",
              mirrorFile, resourceId), faee);
        }
      } catch (Throwable t) {
        // Log and throw because things like 'hadoop fs' like to silently swallow the useful bits
        // of exceptions.
        LOG.debug("Exception thrown during creation", t);
        throw t;
      }
    } else {
      try {
        LOG.debug("Creating empty file '{}' for resourceId '{}'", mirrorFile, resourceId);
        Files.createFile(mirrorFile.toPath());
        LOG.debug("Successfully created empty file '{}' for resourceId '{}'",
            mirrorFile, resourceId);
      } catch (FileAlreadyExistsException faee) {
        LOG.debug("File '{}' already exists, with FileAlreadyExistsException message: '{}'",
            mirrorFile, faee.getMessage());
        if (mirrorFile.isDirectory()) {
          throw new IOException(String.format(
              "Mirror file '%s' for resourceId '%s' already exists and is a directory!",
              mirrorFile, resourceId), faee);
        }
      } catch (Throwable t) {
        // Log and throw because things like 'hadoop fs' like to silently swallow the useful bits
        // of exceptions.
        LOG.debug("Exception thrown during creation", t);
        throw t;
      }
    }

    long currentTime = clock.currentTimeMillis();
    LOG.debug("Setting lastModified on '{}' to '{}'", mirrorFile, currentTime);
    mirrorFile.setLastModified(currentTime);
  }

  @Override
  public CacheEntry putResourceId(StorageResourceId resourceId) throws IOException {
    LOG.debug("putResourceId({})", resourceId);
    validateResourceId(resourceId);

    Path mirrorPath = getMirrorPath(resourceId);
    File mirrorFile = mirrorPath.toFile();

    CacheEntry createdOrExistingEntry = null;
    IOException lastException = null;
    for (int retry = 0; createdOrExistingEntry == null && retry < MAX_RETRIES_FOR_CREATE; ++retry) {
      try {
        // Check if it already exists.
        boolean allowDirectoryMismatch = false;
        createdOrExistingEntry =
            getCacheEntryInternal(resourceId, mirrorFile, allowDirectoryMismatch);
        if (createdOrExistingEntry != null) {
          LOG.debug("Returning immediately with pre-existing file '{}' for resourceId '{}'",
              mirrorFile, resourceId);
        } else {
          // Make sure parent directories are ready.
          ensureParentDirectoriesExist(resourceId, mirrorFile);

          // Stash away the parent-directory's lastModified time so we can restore it after creating
          // the new file.
          // NB: This is vulnerable to a race-condition since this is intended to be done in a
          // multi-process environment, but both outcomes are 'safe':
          // 1. The parentFile accidentally gets a *newer* lastModified time; it will be
          //     garbage-collected later than intended, but no harm done.
          // 2. Between reading the timestamp and restoring it, a garbage-collector already came
          //     in, identified an expired entry, deleted it, and another process already came and
          //     recreated the parent with a fresh lastModified. Then there are two other possible
          // outcomes:
          //     2a. A child object gets added to the parentFile before the next GC-run; the GC-run
          //         is unable to delete a non-empty directory, no harm done.
          //     2b. The GC-run kicks in quickly and deletes the directory before a child file can
          //         be created; the child-file creation fails, our optimistic retries of
          //         parent-directory creation kick in, and the directory gets created with a fresh
          //         timestamp as normal.
          File parentFile = mirrorFile.getParentFile();
          long parentLastModified = parentFile.lastModified();

          // Create the actual mirrorFile.
          createMirrorFileInternal(resourceId, mirrorFile);

          // Restore parent's lastModified time.
          if (!basePath.equals(parentFile.toPath())) {
            LOG.debug("Restoring parentLastModified to '{}' for '{}'",
                parentLastModified, parentFile);
            parentFile.setLastModified(parentLastModified);
          }

          createdOrExistingEntry = new CacheEntry(resourceId, mirrorFile.lastModified());
        }
      } catch (NoSuchFileException nsfe) {
        LOG.info(String.format(
            "Possible concurrently deleted parent dir for file '%s', id '%s', retrying...",
            mirrorFile, resourceId), nsfe);
        lastException = nsfe;
      }
    }

    if (createdOrExistingEntry == null) {
      throw new IOException(String.format(
          "Exhausted all retries creating mirrorFile '%s' for resourceId '%s'",
          mirrorFile, resourceId), lastException);
    }
    return createdOrExistingEntry;
  }

  @Override
  public CacheEntry getCacheEntry(StorageResourceId resourceId) throws IOException {
    LOG.debug("getCacheEntry({})", resourceId);
    validateResourceId(resourceId);

    Path mirrorPath = getMirrorPath(resourceId);
    File mirrorFile = mirrorPath.toFile();

    return getCacheEntryInternal(resourceId, mirrorFile, true);
  }

  @Override
  public void removeResourceId(StorageResourceId resourceId) throws IOException {
    LOG.debug("removeResourceId({})", resourceId);
    validateResourceId(resourceId);

    Path mirrorPath = getMirrorPath(resourceId);
    File mirrorFile = mirrorPath.toFile();

    // Use getCacheEntry to validate existence and matching isDirectory status.
    if (getCacheEntry(resourceId) != null) {
      long parentLastModified = mirrorFile.getParentFile().lastModified();
      // File.delete() (unlike Files.delete(Path)) returns false rather than throwing any
      // exception for nonexistent files or non-empty directories; instead of making multiple
      // calls to check for, e.g., exists() && isDirectory() && isDirectoryEmpty(), which would
      // be non-atomic anyways, we'll just call delete() and let it fail if it already no longer
      // exists or is a nonempty directory.
      if (!mirrorFile.delete()) {
        LOG.debug("Failed to delete file '{}' for resourceId '{}'",
            mirrorFile, resourceId);
      }
      if (!basePath.equals(mirrorFile.getParentFile().toPath())) {
        LOG.debug("Restoring parentListModified to '{}' for '{}'",
            parentLastModified, mirrorFile.getParentFile());
        mirrorFile.getParentFile().setLastModified(parentLastModified);
      }
    } else {
      LOG.debug("Tried to remove nonexistent file '{}' for resourceId '{}'",
          mirrorFile, resourceId);
    }
  }

  @Override
  public List<CacheEntry> getBucketList() throws IOException {
    LOG.debug("getBucketList()");
    // Get initial listing of everything in our basePath to represent buckets.
    File[] bucketList = basePath.toFile().listFiles();

    if (bucketList == null) {
      throw new IOException("Failed to list buckets: " + basePath);
    }

    if (bucketList.length == 0) {
      return ImmutableList.of();
    }

    List<CacheEntry> bucketEntries = new ArrayList<>();
    Set<StorageResourceId> expiredBuckets = new HashSet<>();
    for (File bucket : bucketList) {
      if (!bucket.isDirectory()) {
        LOG.error("Found non-directory file '{}' in bucket listing! Ignoring it.", bucket);
        continue;
      }
      StorageResourceId bucketId = new StorageResourceId(bucket.getName());
      CacheEntry entry = new CacheEntry(bucketId, bucket.lastModified());
      if (isCacheEntryExpired(entry)) {
        // Keep a set of expired buckets to handle after the loop. We may not be able to garbage-
        // collect it because of inner StorageObjects, but we at least won't list it anymore.
        expiredBuckets.add(bucketId);
      } else {
        bucketEntries.add(entry);
      }
    }

    // Handle bucket expiration; try to delete it, and delete will return false if the directory
    // is non-empty.
    // TODO(user): Explore possibly offloading this to a separate threadpool so we can return more
    // quickly.
    for (StorageResourceId expiredBucket : expiredBuckets) {
      LOG.debug("About to delete expired entry for resourceId '{}'", expiredBucket);
      removeResourceId(expiredBucket);
    }
    return bucketEntries;
  }

  @Override
  public List<CacheEntry> getRawBucketList() throws IOException {
    LOG.debug("getRawBucketList()");

    // Get initial listing of everything in our basePath to represent buckets.
    File[] bucketList = basePath.toFile().listFiles();

    if (bucketList == null) {
      throw new IOException("Failed to list buckets: " + basePath);
    }

    if (bucketList.length == 0) {
      return ImmutableList.of();
    }

    List<CacheEntry> bucketEntries = new ArrayList<>();
    for (File bucket : bucketList) {
      if (!bucket.isDirectory()) {
        LOG.error("Found non-directory file '{}' in bucket listing! Ignoring it.", bucket);
        continue;
      }
      StorageResourceId bucketId = new StorageResourceId(bucket.getName());
      CacheEntry entry = new CacheEntry(bucketId, bucket.lastModified());
      bucketEntries.add(entry);
    }
    return bucketEntries;
  }

  @Override
  public List<CacheEntry> getObjectList(
      final String bucketName, final String objectNamePrefix, final String delimiter,
      Set<String> returnedPrefixes)
      throws IOException {
    LOG.debug("getObjectList({}, {}, {})", bucketName, objectNamePrefix, delimiter);
    Preconditions.checkArgument(delimiter == null || "/".equals(delimiter),
        "Only null or '/' are supported as delimiters for file-backed caching, got '%s'.",
        delimiter);

    // Compute the 'parent' StorageResourceId in which we will list before doing the shared
    // match logic.
    StorageResourceId listBase;
    if (Strings.isNullOrEmpty(objectNamePrefix)) {
      // If no prefix provided, then we'll do initial listing in just the bucket itself.
      listBase = new StorageResourceId(bucketName);
    } else {
      int indexOfDelim = objectNamePrefix.lastIndexOf('/');
      if (indexOfDelim == -1) {
        // If no delim present in prefix, we also just list in the bucket directly.
        listBase = new StorageResourceId(bucketName);
      } else {
        // Get the substring up through the last index of the delimiter, inclusive.
        String objectToList = objectNamePrefix.substring(0, indexOfDelim + 1);
        listBase = new StorageResourceId(bucketName, objectToList);
      }
    }

    LOG.debug("Using '{}' as the listBase", listBase);
    validateResourceId(listBase);
    final Path listBasePath = getMirrorPath(listBase);
    File listBaseFile = listBasePath.toFile();

    if (!listBaseFile.exists()) {
      LOG.debug("listBaseFile '{}' corresponding to lastBase '{}' doesn't exist; returning null",
          listBaseFile, listBase);
      return null;
    }

    // The list of cacheEntries to return.
    final List<CacheEntry> cacheEntries = new ArrayList<>();

    // We'll need to relativize each file against this bucketBasePath to extract the effective
    // 'objectName' portion of the paths.
    final Path bucketBasePath = getMirrorPath(new StorageResourceId(bucketName));

    // Handler for visiting each file in-place, possibly deleting it due to expiration or otherwise
    // matching against our list prefix and possibly adding an entry to return.
    final FileCallback fileVisitor = new FileCallback() {
      @Override
      public void run(File candidateFile) throws IOException {
        // This should chop off the entire prefix up to the bucket base.
        Path objectNamePath = bucketBasePath.relativize(candidateFile.toPath());

        // If the candidateFile is a directory, we must explicitly make the corresponding
        // StorageResourceId have a directory-path object string.
        String objectNamePathString = objectNamePath.toString();
        if (candidateFile.isDirectory()) {
          objectNamePathString = StorageResourceId.convertToDirectoryPath(objectNamePathString);
        }

        StorageResourceId objectId = new StorageResourceId(bucketName, objectNamePathString);
        CacheEntry entry = new CacheEntry(objectId, candidateFile.lastModified());
        if (isCacheEntryExpired(entry)) {
          // Handle entry expiration; try to delete it, and delete will return false if the
          // directory is non-empty.
          LOG.debug("About to delete expired entry for resourceId '{}'", objectId);
          removeResourceId(objectId);
        } else {
          String objectName = objectId.getObjectName();
          String matchedName = GoogleCloudStorageStrings.matchListPrefix(
              objectNamePrefix, delimiter, objectName);
          if (matchedName != null && objectName.equals(matchedName)) {
            // We expect only "exact matches" here, since the only way to get a "prefix match" here
            // would be if we had listed *recursively* in the presence of a delimiter. Since we
            // already handled the presence of a delimiter by doing a flat list instead of a
            // recursive walkFileTree, we won't need to deal with prefix matches here.
            LOG.debug("Adding matching entry '{}'", objectId);
            cacheEntries.add(entry);
          }
        }
      }
    };

    if (delimiter != null) {
      // Since there's a delimiter, all we're expected to do is to flat-list a single level.
      File[] fileList = listBaseFile.listFiles();
      if (fileList != null) {
        for (File object : fileList) {
          fileVisitor.run(object);
        }
      } else {
        // A race condition can occur in rare cases if another client deletes listBaseFile between
        // the time we checked listBaseFile.exists() and calling listBaseFile.listFiles().
        LOG.warn("Got null fileList for listBaseFile '{}' even though exists() was true!",
                 listBaseFile);
        return null;
      }
    } else {
      Files.walkFileTree(listBasePath, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException ioe) throws IOException {
          if (!listBasePath.equals(dir)) {
            fileVisitor.run(dir.toFile());
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          fileVisitor.run(file.toFile());
          return FileVisitResult.CONTINUE;
        }
      });
    }

    return cacheEntries;
  }

  @Override
  public int getInternalNumBuckets() throws IOException {
    File[] bucketList = basePath.toFile().listFiles();
    if (bucketList == null) {
      throw new IOException("Failed to list buckets: " + basePath);
    }
    return bucketList.length;
  }

  @Override
  public int getInternalNumObjects() throws IOException {
    // Use int[1] so that the anonymous class can increment this 'count'.
    final int[] count = new int[1];
    Files.walkFileTree(basePath, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException ioe) throws IOException {
        if (!basePath.equals(dir) && !basePath.equals(dir.getParent())) {
          // Don't count basePath nor buckets (detected as dir.getParent() equaling basePath).
          LOG.debug("Counting '{}'", dir);
          count[0]++;
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        LOG.debug("Counting '{}'", file);
        count[0]++;
        return FileVisitResult.CONTINUE;
      }
    });
    return count[0];
  }

  /**
   * Sets {@code listener} as a function to be called immediately prior to all mkdir() or
   * createNewFile() invocations, passing it the StorageResourceId that is about to be created.
   */
  @VisibleForTesting
  void setCreateMirrorFileListener(Function<StorageResourceId, Void> listener) {
    this.createMirrorFileListener = listener;
  }

  /**
   * Only exposed for testing purposes; shouldn't be called outside of tests.
   */
  @VisibleForTesting
  public void setBasePath(String basePathStr) {
    this.basePath = Paths.get(basePathStr);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(basePathStr), "basePathStr '%s' can't be null/empty!", basePathStr);
    Preconditions.checkArgument(
        basePath.isAbsolute(), "basePathStr '%s' must be absolute!", basePathStr);

    if (!basePath.toFile().exists()) {
      LOG.info("Creating '{}' with createDirectories()...", basePath);
      try {
        Files.createDirectories(basePath);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    Preconditions.checkArgument(
        basePath.toFile().exists(), "basePathStr '%s' must exist", basePathStr);
    Preconditions.checkArgument(
        basePath.toFile().isDirectory(), "basePathStr '%s' must be a directory!", basePathStr);
  }
}
