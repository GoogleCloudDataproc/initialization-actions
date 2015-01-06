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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class provides a Hadoop compatible File System on top of Google Cloud Storage (GCS).
 *
 *  It is implemented as a thin abstraction layer on top of GCS. The layer hides any specific
 * characteristics of the underlying store and exposes FileSystem interface understood by the Hadoop
 * engine.
 * <p>
 * Users interact with the files in the storage using fully qualified URIs. The file system exposed
 * by this class is identified using the 'gs' scheme. For example, {@code gs://dir1/dir2/file1.txt}.
 * <p>
 * This implementation translates paths between hadoop Path and GCS URI with the convention that the
 * Hadoop root directly corresponds to the GCS "root", e.g. gs:/. This is convenient for many
 * reasons, such as data portability and close equivalence to gsutil paths, but imposes certain
 * inherited constraints, such as files not being allowed in root (only 'directories' can be placed
 * in root), and directory names inside root have a more limited set of allowed characters.
 * <p>
 * One of the main goals of this implementation is to maintain compatibility with behavior of HDFS
 * implementation when accessed through FileSystem interface. HDFS implementation is not very
 * consistent about the cases when it throws versus the cases when methods return false. We run GHFS
 * tests and HDFS tests against the same test data and use that as a guide to decide whether to
 * throw or to return false.
 *
 * @deprecated This implementation is deprecated and relies on the deprecated systemBucket property.
 *             Use GoogleHadoopFileSystem instead.
 */
@Deprecated
public class GoogleHadoopGlobalRootedFileSystem
    extends GoogleHadoopFileSystemBase {

  /**
   * Constructs an instance of GoogleHadoopGlobalRootedFileSystem; the internal
   * GoogleCloudStorageFileSystem will be set up with config settings when initialize() is called.
   */
  public GoogleHadoopGlobalRootedFileSystem() {
    super();
  }

  /**
   * Constructs an instance of GoogleHadoopGlobalRootedFileSystem using the provided
   * GoogleCloudStorageFileSystem; initialize() will not re-initialize it.
   */
  public GoogleHadoopGlobalRootedFileSystem(GoogleCloudStorageFileSystem gcsfs) {
    super(gcsfs);
  }

  /**
   * Returns an unqualified path without any leading slash, relative to the filesystem root,
   * which serves as the home directory of the current user; see {@code getHomeDirectory} for
   * a description of what the home directory means.
   */
  @Override
  protected String getHomeDirectorySubpath() {
    // Since we're globally-rooted, it's best to make the home directory based off the system bucket
    // since otherwise bucket collisions will abound.
    return systemBucket + "/user/" + System.getProperty("user.name");
  }

  /**
   * Gets Hadoop path corresponding to the given GCS path.
   *
   * @param gcsPath Fully-qualified GCS path, of the form gs://<bucket>/<object>.
   */
  @Override
  public Path getHadoopPath(URI gcsPath) {
    log.debug("GHFS.getHadoopPath: %s", gcsPath);

    // Handle root.
    if (gcsPath.equals(getGcsPath(getFileSystemRoot()))) {
      return getFileSystemRoot();
    }

    // Hadoop code is inconsistent with respect to its handling of paths.
    // Some code uses a path as-is and other code only uses path.toUri().getPath().
    // This causes problems because GCS paths use authority component to store
    // bucket name therefore some code sees full path and some code only
    // sees the object component.
    //
    // We address this issue by moving the bucket name into path component.
    // That is, by converting gs://bucket/object to gs:/bucket/object
    // for internal operations. We perform reverse conversion when we
    // convert a Hadoop path to GCS path (in getGcsPath()) when calling
    // the GCS FS layer.

    StorageResourceId resourceId = GoogleCloudStorageFileSystem.validatePathAndGetId(gcsPath, true);
    Path hadoopPath = getHadoopPathFromResourceId(resourceId);
    log.debug("GHFS.getHadoopPath: %s -> %s", gcsPath, hadoopPath);
    return hadoopPath;
  }

  /**
   * Gets Hadoop path corresponding to the given storageResourceId. Since this helper is
   * very specific to the global-rooted FileSystem semantics, any methods which use this helper
   * likely need to be overridden in derived classes which are not globally-rooted.
   */
  @VisibleForTesting
  Path getHadoopPathFromResourceId(StorageResourceId resourceId) {
    Preconditions.checkArgument(!resourceId.isRoot(),
        "resourceId must be a bucket or object.");

    String bucketName = resourceId.getBucketName();
    String objectName = resourceId.getObjectName();
    if (objectName == null) {
      objectName = "";
    }

    // For this global-rooted GHFS implementation, the hadoop path's scheme is exactly equal to
    // the underlying GCSFS scheme.
    URI pathUri = null;
    String path = getScheme() + ":/" + bucketName + "/" + objectName;
    try {
      pathUri = new URI(path);
    } catch (URISyntaxException e) {
      String msg = String.format("Invalid path: %s / %s", bucketName, objectName);
      throw new IllegalArgumentException(msg, e);
    }
    // Use the single-String constructor instead of passing the URI directly in order to force
    // its normalization logic to kick in.
    Path hadoopPath = new Path(pathUri.toString());
    return hadoopPath;
  }

  @Override
  public URI getGcsPath(Path hadoopPath) {
    log.debug("GHFS.getGcsPath: %s", hadoopPath);

    // Convert to fully qualified absolute path; the Path object will callback to get our current
    // workingDirectory as part of fully resolving the path.
    Path resolvedPath = hadoopPath.makeQualified(this);

    // Handle root.
    if (resolvedPath.equals(getFileSystemRoot())) {
      return GoogleCloudStorageFileSystem.GCS_ROOT;
    }

    // Need to convert scheme to GCS scheme and possibly move bucket into authority
    String authorityString = null;
    if (!Strings.isNullOrEmpty(resolvedPath.toUri().getAuthority())) {
      authorityString = "/" + resolvedPath.toUri().getAuthority();
    } else {
      authorityString = "";
    }
    // Construct GCS path uri.
    String path = GoogleCloudStorageFileSystem.SCHEME + ":/" + authorityString
        + resolvedPath.toUri().getPath();
    URI gcsPath = null;
    try {
      gcsPath = new URI(path);
    } catch (URISyntaxException e) {
      String msg = String.format("Invalid path: %s", hadoopPath);
      throw new IllegalArgumentException(msg, e);
    }

    log.debug("GHFS.getGcsPath: %s -> %s", hadoopPath, gcsPath);
    return gcsPath;
  }

  // =================================================================
  // Methods implementing FileSystemDescriptor interface; these define the way
  // paths are translated between Hadoop and GCS.
  // =================================================================

  @Override
  public Path getFileSystemRoot() {
    return new Path(getScheme() + ":/");
  }

  /**
   * As the global-rooted FileSystem, our hadoop-path "scheme" is distinct from GCS's scheme.
   */
  @Override
  public String getScheme() {
    return "gsg";
  }

  /**
   * Gets the default value of working directory.
   */
  public Path getDefaultWorkingDirectory() {
    return new Path(getFileSystemRoot(), systemBucket);
  }
}
