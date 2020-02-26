/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.UriPaths;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.base.Strings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class HadoopFileSystemIntegrationHelper
    extends GoogleCloudStorageFileSystemIntegrationHelper {

  FileSystem ghfs;
  FileSystemDescriptor ghfsFileSystemDescriptor;

  /**
   * FS statistics mode.
   */
  public enum FileSystemStatistics {
    // No statistics available.
    NONE,

    // Statistics matches number of bytes written/read by caller.
    EXACT,

    // Statistics values reported are often greater than number of bytes
    // written/read by caller because of hidden underlying operations
    // involving check-summing.
    GREATER_OR_EQUAL,

    // We skip all FS statistics tests
    IGNORE,
  }

  // FS statistics mode of the FS tested by this class.
  FileSystemStatistics statistics = FileSystemStatistics.IGNORE;

  public HadoopFileSystemIntegrationHelper(
      FileSystem hfs, FileSystemDescriptor ghfsFileSystemDescriptor) throws IOException {
    super(new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage()));
    this.ghfs = hfs;
    this.ghfsFileSystemDescriptor = ghfsFileSystemDescriptor;
  }

  /**
   * Turn off statistics collection.
   */
  public void setIgnoreStatistics() {
    statistics = FileSystemStatistics.IGNORE;
  }

  /**
   * Renames src path to dst path.
   */
  @Override
  protected boolean rename(URI src, URI dst)
      throws IOException {
    Path srcHadoopPath = castAsHadoopPath(src);
    Path dstHadoopPath = castAsHadoopPath(dst);

    return ghfs.rename(srcHadoopPath, dstHadoopPath);
  }

  /** Deletes the given path. */
  @Override
  protected boolean delete(URI path, boolean recursive) throws IOException {
    Path hadoopPath = castAsHadoopPath(path);
    if (recursive) {
      // Allows delete(URI) to be covered by test.
      // Note that delete(URI) calls delete(URI, true).
      return ghfs.delete(hadoopPath);
    } else {
      return ghfs.delete(hadoopPath, recursive);
    }
  }

  /** Deletes the given item. */
  @Override
  protected void delete(String bucketName) throws IOException {
    Path path = createSchemeCompatibleHadoopPath(bucketName, null);
    ghfs.delete(path, false);
  }

  /** Deletes the given object. */
  @Override
  protected void delete(String bucketName, String objectName) throws IOException {
    Path path = createSchemeCompatibleHadoopPath(bucketName, objectName);
    ghfs.delete(path, false);
  }

  /** Creates the given directory and any non-existent parent directories. */
  @Override
  protected boolean mkdirs(URI path) throws IOException {
    Path hadoopPath = castAsHadoopPath(path);
    return ghfs.mkdirs(hadoopPath);
  }

  /**
   * Indicates whether the given path exists.
   */
  @Override
  protected boolean exists(URI path)
      throws IOException {
    Path hadoopPath = castAsHadoopPath(path);
    try {
      ghfs.getFileStatus(hadoopPath);
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Indicates whether the given path is directory.
   */
  @Override
  protected boolean isDirectory(URI path)
      throws IOException {
    Path hadoopPath = castAsHadoopPath(path);
    try {
      FileStatus status = ghfs.getFileStatus(hadoopPath);
      return status.isDir();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Opens the given object for reading.
   */
  @Override
  protected SeekableByteChannel open(String bucketName, String objectName)
      throws IOException {
    return null;
  }

  /**
   * Opens the given object for writing.
   */
  @Override
  protected WritableByteChannel create(
      String bucketName, String objectName, CreateFileOptions options) throws IOException {
    return null;
  }

  /** Helper which reads the entire file as a String. */
  @Override
  public String readTextFile(String bucketName, String objectName) throws IOException {
    Path hadoopPath = createSchemeCompatibleHadoopPath(bucketName, objectName);
    return readTextFile(hadoopPath);
  }

  /**
   * Helper which reads the entire file as a String.
   */
  protected String readTextFile(Path hadoopPath)
      throws IOException {
    FSDataInputStream readStream = null;
    byte[] readBuffer = new byte[1024];
    StringBuilder returnBuffer = new StringBuilder();

    try {
      readStream =
          ghfs.open(
              hadoopPath,
              GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_BUFFER_SIZE.getDefault());
      int numBytesRead = readStream.read(readBuffer);
      while (numBytesRead > 0) {
        returnBuffer.append(new String(readBuffer, 0, numBytesRead, StandardCharsets.UTF_8));
        numBytesRead = readStream.read(readBuffer);
      }
    } finally {
      if (readStream != null) {
        readStream.close();
      }
    }
    return returnBuffer.toString();
  }

  /**
   * Helper that reads text from the given bucket+object at the given offset
   * and returns it. If checkOverflow is true, it will make sure that
   * no more than 'len' bytes were read.
   */
  @Override
  protected String readTextFile(
      String bucketName, String objectName, int offset, int len, boolean checkOverflow)
      throws IOException {
    Path hadoopPath = createSchemeCompatibleHadoopPath(bucketName, objectName);
    return readTextFile(hadoopPath, offset, len, checkOverflow);
  }

  /**
   * Helper that reads text from the given file at the given offset
   * and returns it. If checkOverflow is true, it will make sure that
   * no more than 'len' bytes were read.
   */
  protected String readTextFile(
      Path hadoopPath, int offset, int len, boolean checkOverflow)
      throws IOException {
    String text = null;
    FSDataInputStream readStream = null;
    long fileSystemBytesRead = 0;
    FileSystem.Statistics stats = FileSystem.getStatistics(
        ghfsFileSystemDescriptor.getScheme(), ghfs.getClass());
    if (stats != null) {
      // Let it be null in case no stats have been added for our scheme yet.
      fileSystemBytesRead =
          stats.getBytesRead();
    }

    try {
      int bufferSize = len;
      bufferSize += checkOverflow ? 1 : 0;
      byte[] readBuffer = new byte[bufferSize];
      readStream =
          ghfs.open(
              hadoopPath,
              GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_BUFFER_SIZE.getDefault());
      int numBytesRead;
      if (offset > 0) {
        numBytesRead = readStream.read(offset, readBuffer, 0, bufferSize);
      } else {
        numBytesRead = readStream.read(readBuffer);
      }
      assertThat(numBytesRead).isEqualTo(len);
      text = new String(readBuffer, 0, numBytesRead, StandardCharsets.UTF_8);
    } finally {
      if (readStream != null) {
        readStream.close();
      }
    }

    // After the read, the stats better be non-null for our ghfs scheme.
    stats = FileSystem.getStatistics(
        ghfsFileSystemDescriptor.getScheme(), ghfs.getClass());
    assertThat(stats).isNotNull();
    long endFileSystemBytesRead = stats.getBytesRead();
    int bytesReadStats = (int) (endFileSystemBytesRead - fileSystemBytesRead);
    if (statistics == FileSystemStatistics.EXACT) {
      assertWithMessage("FS statistics mismatch fetched from class '%s'", ghfs.getClass())
          .that(bytesReadStats)
          .isEqualTo(len);
    } else if (statistics == FileSystemStatistics.GREATER_OR_EQUAL) {
      assertWithMessage("Expected %d <= %d", len, bytesReadStats)
          .that(len <= bytesReadStats)
          .isTrue();
    } else if (statistics == FileSystemStatistics.NONE) {
      assertWithMessage("FS statistics expected to be 0").that(fileSystemBytesRead).isEqualTo(0);
      assertWithMessage("FS statistics expected to be 0").that(endFileSystemBytesRead).isEqualTo(0);
    } else if (statistics == FileSystemStatistics.IGNORE) {
      // NO-OP
    }

    return text;
  }

  /**
   * Creates a directory.
   */
  @Override
  protected void mkdir(String bucketName, String objectName)
      throws IOException {
    Path path = createSchemeCompatibleHadoopPath(bucketName, objectName);
    ghfs.mkdirs(path);
  }

  /**
   * Creates a directory.
   */
  @Override
  protected void mkdir(String bucketName)
      throws IOException {
    Path path = createSchemeCompatibleHadoopPath(bucketName, null);
    ghfs.mkdirs(path);
  }

  /** Deletes all objects from the given bucket. */
  @Override
  protected void clearBucket(String bucketName) throws IOException {
    Path hadoopPath = createSchemeCompatibleHadoopPath(bucketName, null);
    FileStatus[] statusList = null;
    try {
      statusList = ghfs.listStatus(hadoopPath);
    } catch (IOException ioe) {
      // Ignored.
    }

    if (statusList != null) {
      for (FileStatus status : statusList) {
        if (!ghfs.delete(status.getPath(), true)) {
          System.err.println(String.format("Failed to delete path: '%s'", status.getPath()));
        }
      }
    }
  }

  // -----------------------------------------------------------------
  // Overridable methods added by this class.
  // -----------------------------------------------------------------

  /**
   * Gets a Hadoop path using bucketName and objectName as components of a GCS URI, then casting
   * to a no-authority Hadoop path which follows the scheme indicated by the
   * ghfsFileSystemDescriptor.
   */
  protected Path createSchemeCompatibleHadoopPath(String bucketName, String objectName) {
    URI gcsPath = getPath(bucketName, objectName);
    return castAsHadoopPath(gcsPath);
  }

  /**
   * Synthesizes a Hadoop path for the given GCS path by casting straight into the scheme indicated
   * by the ghfsFileSystemDescriptor instance; if the URI contains an 'authority', the authority
   * is re-interpreted as the topmost path component of a URI sitting inside the fileSystemRoot
   * indicated by the ghfsFileSystemDescriptor.
   * <p>
   * Examples:
   *   gs:/// -> gsg:/
   *   gs://foo/bar -> gs://root-bucket/foo/bar
   *   gs://foo/bar -> hdfs:/foo/bar
   * <p>
   * Note that it cannot be generally assumed that GCS-based filesystems will "invert" this path
   * back into the same GCS path internally; for example, if a bucket-rooted filesystem is based
   * in 'my-system-bucket', then this method will convert:
   * <p>
   *   gs://foo/bar -> gs:/foo/bar
   * <p>
   * which will then be converted internally:
   * <p>
   *   gs:/foo/bar -> gs://my-system-bucket/foo/bar
   * <p>
   * when the bucket-rooted FileSystem creates actual data in the underlying GcsFs.
   */
  protected Path castAsHadoopPath(URI gcsPath) {
    String childPath = gcsPath.getRawPath();
    if (childPath != null && childPath.startsWith("/")) {
      childPath = childPath.substring(1);
    }
    String authority = gcsPath.getAuthority();
    if (Strings.isNullOrEmpty(authority)) {
      if (Strings.isNullOrEmpty(childPath)) {
        return ghfsFileSystemDescriptor.getFileSystemRoot();
      } else {
        return new Path(ghfsFileSystemDescriptor.getFileSystemRoot(), childPath);
      }
    } else {
      if (Strings.isNullOrEmpty(childPath)) {
        return new Path(ghfsFileSystemDescriptor.getFileSystemRoot(), authority);
      } else {
        return new Path(ghfsFileSystemDescriptor.getFileSystemRoot(), new Path(
            authority, childPath));
      }
    }
  }

  /**
   * Lists status of file(s) at the given path.
   */
  protected FileStatus[] listStatus(Path hadoopPath)
      throws IOException {
    return ghfs.listStatus(hadoopPath);
  }

  // -----------------------------------------------------------------
  // Misc test helpers.

  /**
   * Writes a file with the given buffer repeated numWrites times.
   *
   * @param bucketName Name of the bucket to create object in.
   * @param objectName Name of the object to create.
   * @param buffer Data to write
   * @param numWrites Number of times to repeat the data.
   * @return Number of bytes written.
   */
  @Override
  protected int writeFile(String bucketName, String objectName, byte[] buffer, int numWrites)
      throws IOException {
    Path hadoopPath = createSchemeCompatibleHadoopPath(bucketName, objectName);
    return writeFile(hadoopPath, buffer, numWrites, /* overwrite= */ true);
  }

  /**
   * Writes a file with the given buffer repeated numWrites times.
   *
   * @param hadoopPath Path of the file to create.
   * @param text Text data to write.
   * @param numWrites Number of times to repeat the data.
   * @param overwrite If true, overwrite any existing file.
   * @return Number of bytes written.
   */
  public int writeFile(Path hadoopPath, String text, int numWrites, boolean overwrite)
      throws IOException {
    return writeFile(hadoopPath, text.getBytes(UTF_8), numWrites, overwrite);
  }

  /**
   * Writes a file with the given buffer repeated numWrites times.
   *
   * @param hadoopPath Path of the file to create.
   * @param buffer Data to write.
   * @param numWrites Number of times to repeat the data.
   * @param overwrite If true, overwrite any existing file.
   * @return Number of bytes written.
   */
  public int writeFile(Path hadoopPath, byte[] buffer, int numWrites, boolean overwrite)
      throws IOException {
    int numBytesWritten = -1;
    int totalBytesWritten = 0;

    long fileSystemBytesWritten = 0;
    FileSystem.Statistics stats = FileSystem.getStatistics(
        ghfsFileSystemDescriptor.getScheme(), ghfs.getClass());
    if (stats != null) {
      // Let it be null in case no stats have been added for our scheme yet.
      fileSystemBytesWritten =
          stats.getBytesWritten();
    }
    try (FSDataOutputStream writeStream =
        ghfs.create(
            hadoopPath,
            FsPermission.getDefault(),
            overwrite,
            GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_BUFFER_SIZE.getDefault(),
            GoogleHadoopFileSystemBase.REPLICATION_FACTOR_DEFAULT,
            GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getDefault(),
            /* progress= */ null)) {
      for (int i = 0; i < numWrites; i++) {
        writeStream.write(buffer, 0, buffer.length);
        numBytesWritten = buffer.length;
        totalBytesWritten += numBytesWritten;
      }
    }

    // After the write, the stats better be non-null for our ghfs scheme.
    stats = FileSystem.getStatistics(ghfsFileSystemDescriptor.getScheme(), ghfs.getClass());
    assertThat(stats).isNotNull();
    long endFileSystemBytesWritten =
        stats.getBytesWritten();
    int bytesWrittenStats = (int) (endFileSystemBytesWritten - fileSystemBytesWritten);
    if (statistics == FileSystemStatistics.EXACT) {
      assertWithMessage("FS statistics mismatch fetched from class '%s'", ghfs.getClass())
          .that(bytesWrittenStats)
          .isEqualTo(totalBytesWritten);
    } else if (statistics == FileSystemStatistics.GREATER_OR_EQUAL) {
      assertWithMessage("Expected %d <= %d", totalBytesWritten, bytesWrittenStats)
          .that(totalBytesWritten <= bytesWrittenStats)
          .isTrue();
    } else if (statistics == FileSystemStatistics.NONE) {
      // Do not perform any check because stats are either not maintained or are erratic.
    } else if (statistics == FileSystemStatistics.IGNORE) {
      // NO-OP
    }

    return totalBytesWritten;
  }

  public URI getPath(String bucketName, String objectName, boolean allowEmpty) {
    return UriPaths.fromStringPathComponents(bucketName, objectName, allowEmpty);
  }
}
