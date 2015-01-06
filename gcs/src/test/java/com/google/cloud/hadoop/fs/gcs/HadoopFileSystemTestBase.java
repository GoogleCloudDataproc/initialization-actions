/**
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

package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.SeekableReadableByteChannel;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract base class with extended tests against the base Hadoop FileSystem interface but
 * with coverage of tricky edge cases relevant to GoogleHadoopFileSystem. Tests here are intended
 * to be useful for all different FileSystem implementations.
 *
 * We reuse test code from GoogleCloudStorageIntegrationTest and
 * GoogleCloudStorageFileSystemIntegrationTest.
 */
@RunWith(JUnit4.class)
public abstract class HadoopFileSystemTestBase
    extends GoogleCloudStorageFileSystemIntegrationTest {

  // GHFS access instance.
  static FileSystem ghfs;
  static FileSystemDescriptor ghfsFileSystemDescriptor;

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
  static FileSystemStatistics statistics = FileSystemStatistics.IGNORE;

  /**
   * Perform initialization after creating test instances.
   */
  public static void postCreateInit()
      throws IOException {
    GoogleCloudStorageFileSystemIntegrationTest.postCreateInit();

    // Ensures that we do not accidentally end up testing wrong functionality.
    gcs = null;
    gcsfs = null;
  }

  /**
   * Perform clean-up once after all tests are turn.
   */
  @AfterClass
  public static void afterAllTests()
      throws IOException {
    if (ghfs != null) {
      GoogleCloudStorageFileSystemIntegrationTest.afterAllTests();
      ghfs.close();
      ghfs = null;
      ghfsFileSystemDescriptor = null;
    }
  }

  // -----------------------------------------------------------------
  // Overridden methods from GCS FS test.
  // -----------------------------------------------------------------

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

  /**
   * Deletes the given path.
   */
  @Override
  protected boolean delete(URI path, boolean recursive)
      throws IOException {
    Path hadoopPath = castAsHadoopPath(path);
    if (recursive) {
      // Allows delete(URI) to be covered by test.
      // Note that delete(URI) calls delete(URI, true).
      return ghfs.delete(hadoopPath);
    } else {
      return ghfs.delete(hadoopPath, recursive);
    }
  }

  /**
   * Creates the given directory and any non-existent parent directories.
   */
  @Override
  protected boolean mkdirs(URI path)
      throws IOException {
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

  // -----------------------------------------------------------------
  // Overridden methods from GCS test.
  // -----------------------------------------------------------------

  /**
   * Actual logic for validating the GoogleHadoopFileSystemBase-specific FileStatus returned
   * by getFileStatus() or listStatus().
   */
  private void validateFileStatusInternal(
      String bucketName, String objectName, boolean expectedToExist, FileStatus fileStatus)
      throws IOException {
    Assert.assertEquals(
        String.format("Existence of bucketName '%s', objectName '%s'", bucketName, objectName),
        expectedToExist, fileStatus != null);

    if (fileStatus != null) {
      // File/dir exists, check its attributes.
      String message = (fileStatus.getPath()).toString();

      long expectedSize = getExpectedObjectSize(objectName, expectedToExist);
      if (expectedSize != Long.MIN_VALUE) {
        Assert.assertEquals(message, expectedSize, fileStatus.getLen());
      }

      boolean expectedToBeDir =
          Strings.isNullOrEmpty(objectName) || objectHasDirectoryPath(objectName);
      Assert.assertEquals(message, expectedToBeDir, fileStatus.isDir());

      Instant currentTime = Instant.now();
      Instant modificationTime = new Instant(fileStatus.getModificationTime());
      // We must subtract 1000, because some FileSystems, like LocalFileSystem, have only
      // second granularity, so we might have something like testStartTime == 1234123
      // and modificationTime == 1234000. Unfortunately, "Instant" doesn't support easy
      // conversions between units to clip to the "second" precision.
      // Alternatively, we should just use TimeUnit and formally convert "toSeconds".
      Assert.assertTrue(
          String.format(
              "Stale file? testStartTime: %s modificationTime: %s bucket: '%s' object: '%s'",
              testStartTime.toString(), modificationTime.toString(), bucketName, objectName),
          modificationTime.isEqual(testStartTime.minus(1000))
              || modificationTime.isAfter(testStartTime.minus(1000)));
      Assert.assertTrue(
          String.format(
              "Clock skew? currentTime: %s modificationTime: %s bucket: '%s' object: '%s'",
              currentTime.toString(), modificationTime.toString(), bucketName, objectName),
          modificationTime.isEqual(currentTime) || modificationTime.isBefore(currentTime));
    }
  }


  /**
   * Validates FileStatus for the given item.
   *
   * See {@link GoogleCloudStorageIntegrationTest.listObjectNamesAndGetItemInfo()} for more info.
   */
  @Override
  protected void validateGetItemInfo(
      String bucketName, String objectName, boolean expectedToExist)
      throws IOException {
    URI path = GoogleCloudStorageFileSystem.getPath(bucketName, objectName, true);
    Path hadoopPath = castAsHadoopPath(path);
    FileStatus fileStatus = null;

    try {
      fileStatus = ghfs.getFileStatus(hadoopPath);
    } catch (FileNotFoundException e) {
      // Leaves fileStatus == null on FileNotFoundException.
    }

    if (fileStatus != null) {
      Assert.assertEquals(
          "Hadoop paths for URI: " + path.toString(), hadoopPath, fileStatus.getPath());
    }
    validateFileStatusInternal(bucketName, objectName, expectedToExist, fileStatus);
  }

  /**
   * Validates FileInfo returned by listFileInfo().
   *
   * See {@link GoogleCloudStorage.listObjectNamesAndGetItemInfo()} for more info.
   */
  @Override
  protected void validateListNamesAndInfo(
      String bucketName, String objectNamePrefix,
      boolean pathExpectedToExist, String... expectedListedNames)
      throws IOException {
    boolean childPathsExpectedToExist =
        pathExpectedToExist && (expectedListedNames != null);
    boolean listRoot = bucketName == null;

    // Prepare list of expected paths.
    List<Path> expectedPaths = new ArrayList<>();
    // Also maintain a backwards mapping to keep track of which of "expectedListedNames" and
    // "bucketName" is associated with each path, so that we can supply validateFileStatusInternal
    // with the objectName and thus enable it to lookup the internally stored expected size,
    // directory status, etc., of the associated FileStatus.
    Map<Path, String[]> pathToComponents = new HashMap<>();
    if (childPathsExpectedToExist) {
      for (String expectedListedName : expectedListedNames) {
        String[] pathComponents = new String[2];
        if (listRoot) {
          pathComponents[0] = expectedListedName;
          pathComponents[1] = null;
        } else {
          pathComponents[0] = bucketName;
          pathComponents[1] = expectedListedName;
        }
        Path expectedPath = castAsHadoopPath(
            GoogleCloudStorageFileSystem.getPath(pathComponents[0], pathComponents[1], true));
        expectedPaths.add(expectedPath);
        pathToComponents.put(expectedPath, pathComponents);
      }
    }

    // Get list of actual paths.
    URI path = GoogleCloudStorageFileSystem.getPath(bucketName, objectNamePrefix, true);
    Path hadoopPath = castAsHadoopPath(path);
    FileStatus[] fileStatus = null;
    try {
      fileStatus = listStatus(hadoopPath);
    } catch (FileNotFoundException fnfe) {
      Assert.assertFalse(
          String.format("Hadoop path %s expected to exist", hadoopPath), pathExpectedToExist);
    }

    if (!ghfsFileSystemDescriptor.getScheme().equals("file")) {
      Assert.assertEquals(
          String.format("Hadoop path %s", hadoopPath.toString()),
          pathExpectedToExist, fileStatus != null);
    } else {
      // LocalFileSystem -> ChecksumFileSystem will return an empty array instead of null for
      // nonexistent paths.
      if (!pathExpectedToExist && fileStatus != null) {
        Assert.assertEquals(0, fileStatus.length);
      }
    }

    if (fileStatus != null) {
      Set<Path> actualPaths = new HashSet<>();
      for (FileStatus status : fileStatus) {
        Path actualPath = status.getPath();
        if (status.isDir()) {
          Assert.assertFalse(status.getPath().getName().isEmpty());
        }
        actualPaths.add(actualPath);
        String[] uriComponents = pathToComponents.get(actualPath);
        if (uriComponents != null) {
          // Only do fine-grained validation for the explicitly expected paths.
          validateFileStatusInternal(uriComponents[0], uriComponents[1], true, status);
        }
      }

      if (listRoot) {
        for (Path expectedPath : expectedPaths) {
          Assert.assertTrue(
              String.format("expected: <%s> in: <%s>", expectedPath, actualPaths),
              actualPaths.contains(expectedPath));
        }
      } else {
        // Used sorted arrays so that the test-failure output is makes it easy to match up each
        // expected element to each actual element.
        Path[] sortedExpectedPaths =
            new ArrayList<>(Sets.newTreeSet(expectedPaths)).toArray(new Path[0]);
        Path[] sortedActualPaths =
            new ArrayList<>(Sets.newTreeSet(actualPaths)).toArray(new Path[0]);
        String errorMessage = String.format(
            "expected: %s, actual: %s",
            Arrays.toString(sortedExpectedPaths),
            Arrays.toString(sortedActualPaths));
        Assert.assertArrayEquals(errorMessage, sortedExpectedPaths, sortedActualPaths);
      }
    }
  }

  /**
   * Opens the given object for reading.
   */
  @Override
  protected SeekableReadableByteChannel open(String bucketName, String objectName)
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
  protected int writeFile(String bucketName, String objectName, ByteBuffer buffer, int numWrites)
      throws IOException {
    Path hadoopPath = createSchemeCompatibleHadoopPath(bucketName, objectName);
    return HadoopFileSystemTestBase.writeFile(
        hadoopPath, buffer, numWrites, true);
  }

  /**
   * Helper which reads the entire file as a String.
   */
  @Override
  protected String readTextFile(String bucketName, String objectName)
      throws IOException {
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
    StringBuffer returnBuffer = new StringBuffer();

    try {
      readStream = ghfs.open(hadoopPath, GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT);
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
      readStream = ghfs.open(hadoopPath, GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT);
      int numBytesRead;
      if (offset > 0) {
        numBytesRead = readStream.read(offset, readBuffer, 0, bufferSize);
      } else {
        numBytesRead = readStream.read(readBuffer);
      }
      Assert.assertEquals(len, numBytesRead);
      text = new String(readBuffer, 0, numBytesRead, StandardCharsets.UTF_8);
    } finally {
      if (readStream != null) {
        readStream.close();
      }
    }

    // After the read, the stats better be non-null for our ghfs scheme.
    stats = FileSystem.getStatistics(
        ghfsFileSystemDescriptor.getScheme(), ghfs.getClass());
    Assert.assertNotNull(stats);
    long endFileSystemBytesRead = stats.getBytesRead();
    int bytesReadStats = (int) (endFileSystemBytesRead - fileSystemBytesRead);
    if (statistics == FileSystemStatistics.EXACT) {
      Assert.assertEquals(
          String.format("FS statistics mismatch fetched from class '%s'", ghfs.getClass()),
          len, bytesReadStats);
    } else if (statistics == FileSystemStatistics.GREATER_OR_EQUAL) {
      Assert.assertTrue(String.format("Expected %d <= %d", len, bytesReadStats),
          len <= bytesReadStats);
    } else if (statistics == FileSystemStatistics.NONE) {
      Assert.assertEquals("FS statistics expected to be 0", 0, fileSystemBytesRead);
      Assert.assertEquals("FS statistics expected to be 0", 0, endFileSystemBytesRead);
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

  /**
   * Deletes the given item.
   */
  @Override
  protected void delete(String bucketName)
      throws IOException {
    Path path = createSchemeCompatibleHadoopPath(bucketName, null);
    ghfs.delete(path, false);
  }

  /**
   * Deletes the given object.
   */
  @Override
  protected void delete(String bucketName, String objectName)
      throws IOException {
    Path path = createSchemeCompatibleHadoopPath(bucketName, objectName);
    ghfs.delete(path, false);
  }

  /**
   * Deletes all objects from the given bucket.
   */
  @Override
  protected void clearBucket(String bucketName)
      throws IOException {
    Path hadoopPath = createSchemeCompatibleHadoopPath(bucketName, null);
    FileStatus[] statusList = ghfs.listStatus(hadoopPath);
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
  // Tests that test behavior at GHFS layer.

  /**
   * Tests read() when invalid arguments are passed.
   */
  @Test
  public void testReadInvalidArgs()
      throws IOException {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = castAsHadoopPath(path);
    writeFile(hadoopPath, "file text", 1, true);
    FSDataInputStream readStream =
        ghfs.open(hadoopPath, GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT);
    byte[] buffer = new byte[1];

    // Verify that normal read works.
    int numBytesRead = readStream.read(buffer, 0, buffer.length);
    Assert.assertEquals("Expected exactly 1 byte to be read", 1, numBytesRead);

    // Null buffer.
    testReadInvalidArgsHelper(readStream, null, 0, 1, NullPointerException.class);

    // offset < 0
    testReadInvalidArgsHelper(readStream, buffer, -1, 1, IndexOutOfBoundsException.class);

    // length < 0
    testReadInvalidArgsHelper(readStream, buffer, 0, -1, IndexOutOfBoundsException.class);

    // length > buffer.length - offset
    testReadInvalidArgsHelper(readStream, buffer, 0, 2, IndexOutOfBoundsException.class);
  }

  private void testReadInvalidArgsHelper(
      FSDataInputStream readStream, byte[] buffer, int offset, int length,
      Class<? extends Exception> exceptionClass) {
    try {
      readStream.read(buffer, offset, length);
      Assert.fail("Expected " + exceptionClass.getName());
    } catch (Exception e) {
      if (e.getClass() != exceptionClass) {
        Assert.fail("Unexpected exception: " + e);
      }
    }
  }

  /**
   * Writes a file one byte at a time (exercises write(byte b)).
   */
  @Test
  public void testWrite1Byte()
      throws IOException {
    String text = "Hello World!";
    byte[] textBytes = text.getBytes("UTF-8");
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = castAsHadoopPath(path);
    FSDataOutputStream writeStream = null;
    writeStream = ghfs.create(hadoopPath);

    // Write one byte at a time.
    for (byte b : textBytes) {
      writeStream.write(b);
    }
    writeStream.close();

    // Read the file back and verify contents.
    String readText = readTextFile(hadoopPath, 0, textBytes.length, true);
    Assert.assertEquals("testWrite1Byte: write-read mismatch", text, readText);
  }

  /**
   * Validates delete().
   */
  @Test @Override
  public void testDelete()
      throws IOException {
    deleteHelper(new HdfsBehavior());
  }

  /**
   * Validates mkdirs().
   */
  @Test @Override
  public void testMkdirs()
      throws IOException, URISyntaxException {
    mkdirsHelper(new HdfsBehavior());
  }

  /**
   * Validates rename().
   */
  @Test @Override
  public void testRename()
      throws IOException {
    renameHelper(new HdfsBehavior());
  }

  /**
   * Validates that we can / cannot overwrite a file.
   */
  @Test
  public void testOverwrite()
      throws IOException {

    // Get a temp path and ensure that it does not already exist.
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = castAsHadoopPath(path);
    try {
      ghfs.getFileStatus(hadoopPath);
      Assert.fail("Expected FileNotFoundException");
    } catch (FileNotFoundException expected) {
      // Expected.
    }

    // Create a file.
    String text = "Hello World!";
    int numBytesWritten = writeFile(hadoopPath, text, 1, false);
    Assert.assertEquals(text.getBytes("UTF-8").length, numBytesWritten);

    // Try to create the same file again with overwrite == false.
    try {
      writeFile(hadoopPath, text, 1, false);
      Assert.fail("Expected IOException");
    } catch (IOException expected) {
      // Expected.
    }
  }

  /**
   * Validates append().
   */
  @Test
  public void testAppend()
      throws IOException {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = castAsHadoopPath(path);
    try {
      ghfs.append(hadoopPath, GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT, null);
      Assert.fail("Expected IOException");
    } catch (IOException expected) {
      // Expected.
    }
  }

  /**
   * Validates getDefaultReplication().
   */
  @Test
  public void testGetDefaultReplication()
      throws IOException {
    Assert.assertEquals(GoogleHadoopFileSystemBase.REPLICATION_FACTOR_DEFAULT,
        ghfs.getDefaultReplication());
  }

  /**
   * Validates functionality related to getting/setting current position.
   */
  @Test
  public void testFilePosition()
      throws IOException {

    // Write an object.
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = castAsHadoopPath(path);
    String text = "Hello World!";
    int numBytesWritten = writeFile(hadoopPath, text, 1, false);

    // Verify that position is at 0 for a newly opened stream.
    try (FSDataInputStream readStream =
        ghfs.open(hadoopPath, GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT)) {
      Assert.assertEquals(0, readStream.getPos());

      // Verify that position advances by 2 after reading 2 bytes.
      Assert.assertEquals(readStream.read(), (int) 'H');
      Assert.assertEquals(readStream.read(), (int) 'e');
      Assert.assertEquals(2, readStream.getPos());

      // Verify that setting position to the same value is a no-op.
      readStream.seek(2);
      Assert.assertEquals(2, readStream.getPos());
      readStream.seek(2);
      Assert.assertEquals(2, readStream.getPos());

      // Verify that position can be set to a valid position.
      readStream.seek(6);
      Assert.assertEquals(6, readStream.getPos());
      Assert.assertEquals(readStream.read(), (int) 'W');

      // Verify that position can be set to end of file.
      long posEOF = numBytesWritten - 1;
      int val;
      readStream.seek(posEOF);
      Assert.assertEquals(posEOF, readStream.getPos());
      val = readStream.read();
      Assert.assertTrue(val != -1);
      val = readStream.read();
      Assert.assertEquals(val, -1);
      readStream.seek(0);
      Assert.assertEquals(0, readStream.getPos());

      // Verify that position cannot be set to a negative position.
      // Note:
      // HDFS implementation allows seek(-1) to succeed.
      // It even sets the position to -1!
      // We cannot enable the following test till HDFS bug is fixed.
      // try {
      //   readStream.seek(-1);
      //   Assert.fail("Expected IOException");
      // } catch (IOException expected) {
      //   // Expected.
      // }

      // Verify that position cannot be set beyond end of file.
      try {
        readStream.seek(numBytesWritten + 1);
        Assert.fail("Expected IOException");
      } catch (IOException expected) {
        // Expected.
      }

      // Perform some misc checks.
      // TODO(user): Make it no longer necessary to do instanceof.
      if (ghfs instanceof GoogleHadoopFileSystemBase) {
        long someValidPosition = 2;
        Assert.assertEquals(false, readStream.seekToNewSource(someValidPosition));
        Assert.assertEquals(false, readStream.markSupported());
      }
    }
  }

  /**
   * More comprehensive testing of various "seek" calls backwards and forwards and around
   * the edge cases related to buffer sizes.
   */
  @Test
  public void testFilePositionInDepthSeeks()
      throws IOException {
    // Write an object.
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = castAsHadoopPath(path);

    byte[] testBytes = new byte[GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT * 3];
    // The value of each byte should be each to its integer index squared, cast as a byte.
    for (int i = 0; i < testBytes.length; ++i) {
      testBytes[i] = (byte) (i * i);
    }
    int numBytesWritten = writeFile(hadoopPath, ByteBuffer.wrap(testBytes), 1, false);
    Assert.assertEquals(testBytes.length, numBytesWritten);

    try (FSDataInputStream readStream =
        ghfs.open(hadoopPath, GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT)) {
      Assert.assertEquals(0, readStream.getPos());
      Assert.assertEquals(testBytes[0], (byte) readStream.read());
      Assert.assertEquals(1, readStream.getPos());
      Assert.assertEquals(testBytes[1], (byte) readStream.read());
      Assert.assertEquals(2, readStream.getPos());

      // Seek backwards after reads got us to the current position.
      readStream.seek(0);
      Assert.assertEquals(0, readStream.getPos());
      Assert.assertEquals(testBytes[0], (byte) readStream.read());
      Assert.assertEquals(1, readStream.getPos());
      Assert.assertEquals(testBytes[1], (byte) readStream.read());
      Assert.assertEquals(2, readStream.getPos());

      // Seek to same position, should be no-op, data should still be right.
      readStream.seek(2);
      Assert.assertEquals(2, readStream.getPos());
      Assert.assertEquals(testBytes[2], (byte) readStream.read());
      Assert.assertEquals(3, readStream.getPos());

      // Seek farther, but within the read buffersize.
      int midPos = (int) GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT / 2;
      readStream.seek(midPos);
      Assert.assertEquals(midPos, readStream.getPos());
      Assert.assertEquals(testBytes[midPos], (byte) readStream.read());

      // Seek backwards after we got here from some seeking, seek close to but not equal to
      // the beginning.
      readStream.seek(42);
      Assert.assertEquals(42, readStream.getPos());
      Assert.assertEquals(testBytes[42], (byte) readStream.read());

      // Seek to right before the end of the internal buffer.
      int edgePos = (int) GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT - 1;
      readStream.seek(edgePos);
      Assert.assertEquals(edgePos, readStream.getPos());
      Assert.assertEquals(testBytes[edgePos], (byte) readStream.read());

      // This read should put us over the buffer's limit and require a new read from the underlying
      // stream.
      Assert.assertEquals(edgePos + 1, readStream.getPos());
      Assert.assertEquals(testBytes[edgePos + 1], (byte) readStream.read());

      // Seek back to the edge and this time seek forward.
      readStream.seek(edgePos);
      Assert.assertEquals(edgePos, readStream.getPos());
      readStream.seek(edgePos + 1);
      Assert.assertEquals(edgePos + 1, readStream.getPos());
      Assert.assertEquals(testBytes[edgePos + 1], (byte) readStream.read());

      // Seek into buffer 2, then seek a bit further into it.
      int bufferTwoStart = GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT * 2;
      readStream.seek(bufferTwoStart);
      Assert.assertEquals(bufferTwoStart, readStream.getPos());
      Assert.assertEquals(testBytes[bufferTwoStart], (byte) readStream.read());
      readStream.seek(bufferTwoStart + 42);
      Assert.assertEquals(bufferTwoStart + 42, readStream.getPos());
      Assert.assertEquals(testBytes[bufferTwoStart + 42], (byte) readStream.read());

      // Seek backwards in-place inside buffer 2.
      readStream.seek(bufferTwoStart);
      Assert.assertEquals(bufferTwoStart, readStream.getPos());
      Assert.assertEquals(testBytes[bufferTwoStart], (byte) readStream.read());

      // Seek backwards by one buffer, but not all the way back to buffer 0.
      int bufferOneInternal = GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT + 42;
      readStream.seek(bufferOneInternal);
      Assert.assertEquals(bufferOneInternal, readStream.getPos());
      Assert.assertEquals(testBytes[bufferOneInternal], (byte) readStream.read());

      // Seek to the very beginning again and then seek to the very end.
      readStream.seek(0);
      Assert.assertEquals(0, readStream.getPos());
      Assert.assertEquals(testBytes[0], (byte) readStream.read());
      Assert.assertEquals(1, readStream.getPos());
      readStream.seek(testBytes.length - 1);
      Assert.assertEquals(testBytes[testBytes.length - 1], (byte) readStream.read());
      Assert.assertEquals(testBytes.length, readStream.getPos());
    }
  }

  /**
   * Validates when paths already contain a pre-escaped substring, e.g. file:///foo%3Abar/baz,
   * that the FileSystem doesn't accidentally unescape it along the way, e.g. translating into
   * file:///foo:bar/baz.
   */
  @Test
  public void testPreemptivelyEscapedPaths()
      throws IOException {
    URI parentUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path parentPath = castAsHadoopPath(parentUri);
    Path escapedPath = new Path(parentPath, new Path("foo%3Abar"));

    writeFile(escapedPath, "foo", 1, true);
    Assert.assertTrue(ghfs.exists(escapedPath));

    FileStatus status = ghfs.getFileStatus(escapedPath);
    Assert.assertEquals(escapedPath, status.getPath());

    // Cleanup.
    Assert.assertTrue(ghfs.delete(parentPath, true));
  }

  /**
   * Contains data needed for testing the setWorkingDirectory() operation.
   */
  static class WorkingDirData {

    // Path passed to setWorkingDirectory().
    Path path;

    // Expected working directory after calling setWorkingDirectory().
    // null == no change to working directory expected.
    Path expectedPath;

    /**
     * Constructs an instance of WorkingDirData.
     *
     * The given path and expectedPath are used without further modification.
     * The caller decides whether to pass absolute or relative paths.
     */
    private WorkingDirData(Path path, Path expectedPath) {
      this.path = path;
      this.expectedPath = expectedPath;
    }

    /**
     * Constructs an instance of WorkingDirData.
     *
     * The given objectName is converted to an absolute path by combining it with
     * the default test bucket. The resulting path is passed to setWorkingDirectory().
     * Similarly the expectedObjectName is also converted to an absolute path.
     */
    static WorkingDirData absolute(
        HadoopFileSystemTestBase ghfsit,
        String objectName, String expectedObjectName) {
      return new WorkingDirData(
          ghfsit.createSchemeCompatibleHadoopPath(bucketName, objectName),
          ghfsit.createSchemeCompatibleHadoopPath(bucketName, expectedObjectName));
    }

    /**
     * Constructs an instance of WorkingDirData.
     *
     * The given objectName is converted to an absolute path by combining it with
     * the default test bucket. The resulting path is passed to setWorkingDirectory().
     */
    static WorkingDirData absolute(
        HadoopFileSystemTestBase ghfsit,
        String objectName) {
      return new WorkingDirData(
          ghfsit.createSchemeCompatibleHadoopPath(bucketName, objectName),
          null);
    }

    /**
     * Constructs an instance of WorkingDirData.
     *
     * The given path and expectedPath are used without further modification.
     * The caller decides whether to pass absolute or relative paths.
     */
    static WorkingDirData any(Path path, Path expectedPath) {
      return new WorkingDirData(path, expectedPath);
    }
  }

  /**
   * Helper for creating the necessary objects for testing working directory settings, returns
   * a list of WorkingDirData where each element represents a different test condition.
   */
  protected List<WorkingDirData> setUpWorkingDirectoryTest()
      throws IOException {
    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "d0/",
      "d1/f1",
      "d1/d11/f1",
    };

    // -------------------------------------------------------
    // Create test objects.
    clearBucket(bucketName);
    createObjectsWithSubdirs(bucketName, objectNames);

    // -------------------------------------------------------
    // Initialize test data.
    List<WorkingDirData> wddList = new ArrayList<>();

    // Set working directory to root.
    Path rootPath = ghfsFileSystemDescriptor.getFileSystemRoot();
    wddList.add(WorkingDirData.any(rootPath, rootPath));

    // Set working directory to an existing directory (empty).
    wddList.add(WorkingDirData.absolute(this, "d0/", "d0/"));

    // Set working directory to an existing directory (non-empty).
    wddList.add(WorkingDirData.absolute(this, "d1/", "d1/"));


    // Set working directory to an existing directory (multi-level).
    wddList.add(WorkingDirData.absolute(this, "d1/d11/", "d1/d11/"));

    // Set working directory to an existing directory (bucket).
    wddList.add(WorkingDirData.absolute(this, (String) null, (String) null));

    return wddList;
  }

  /**
   * Validates setWorkingDirectory() and getWorkingDirectory().
   */
  @Test
  public void testWorkingDirectory()
      throws IOException {
    List<WorkingDirData> wddList = setUpWorkingDirectoryTest();

    // -------------------------------------------------------
    // Call setWorkingDirectory() for each path and verify the expected behavior.
    for (WorkingDirData wdd : wddList) {
      Path path = wdd.path;
      Path expectedWorkingDir = wdd.expectedPath;
      Path currentWorkingDir = ghfs.getWorkingDirectory();
      ghfs.setWorkingDirectory(path);
      Path newWorkingDir = ghfs.getWorkingDirectory();
      if (expectedWorkingDir != null) {
        Assert.assertEquals(expectedWorkingDir, newWorkingDir);
      } else {
        Assert.assertEquals(currentWorkingDir, newWorkingDir);
      }
    }
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because they do not make sense
  // in the context of this layer.
  // -----------------------------------------------------------------

  @Test @Override
  public void testGetFileInfos()
      throws IOException {
  }

  // -----------------------------------------------------------------
  // Misc test helpers.

  /**
   * Writes a file with the given buffer repeated numWrites times.
   *
   * @param hadoopPath Path of the file to create.
   * @param text Text data to write.
   * @param numWrites Number of times to repeat the data.
   * @param overwrite If true, overwrite any existing file.
   * @return Number of bytes written.
   */
  static int writeFile(Path hadoopPath, String text, int numWrites, boolean overwrite)
      throws IOException {
    return writeFile(hadoopPath, ByteBuffer.wrap(text.getBytes("UTF-8")), numWrites, overwrite);
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
  static int writeFile(Path hadoopPath, ByteBuffer buffer, int numWrites, boolean overwrite)
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
    FSDataOutputStream writeStream = null;
    boolean allWritesSucceeded = false;

    try {
      writeStream = ghfs.create(hadoopPath,
          FsPermission.getDefault(),
          overwrite,
          GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT,
          GoogleHadoopFileSystemBase.REPLICATION_FACTOR_DEFAULT,
          GoogleHadoopFileSystemBase.BLOCK_SIZE_DEFAULT,
          null);  // progressable

      for (int i = 0; i < numWrites; i++) {
        buffer.clear();
        writeStream.write(buffer.array(), 0, buffer.capacity());
        numBytesWritten = buffer.capacity();
        totalBytesWritten += numBytesWritten;
      }
      allWritesSucceeded = true;
    } finally {
      if (writeStream != null) {
        try {
          writeStream.close();
        } catch (IOException e) {
          // Ignore IO exceptions while closing if write failed otherwise the
          // exception that caused the write to fail gets superseded.
          // On the other hand, if all writes succeeded then we need to know about the exception
          // that was thrown during closing.
          if (allWritesSucceeded) {
            throw e;
          }
        }
      }
    }

    // After the write, the stats better be non-null for our ghfs scheme.
    stats = FileSystem.getStatistics(ghfsFileSystemDescriptor.getScheme(), ghfs.getClass());
    Assert.assertNotNull(stats);
    long endFileSystemBytesWritten =
        stats.getBytesWritten();
    int bytesWrittenStats = (int) (endFileSystemBytesWritten - fileSystemBytesWritten);
    if (statistics == FileSystemStatistics.EXACT) {
      Assert.assertEquals(
          String.format("FS statistics mismatch fetched from class '%s'", ghfs.getClass()),
          totalBytesWritten, bytesWrittenStats);
    } else if (statistics == FileSystemStatistics.GREATER_OR_EQUAL) {
      Assert.assertTrue(String.format("Expected %d <= %d", totalBytesWritten, bytesWrittenStats),
          totalBytesWritten <= bytesWrittenStats);
    } else if (statistics == FileSystemStatistics.NONE) {
      // Do not perform any check because stats are either not maintained or are erratic.
    } else if (statistics == FileSystemStatistics.IGNORE) {
      // NO-OP
    }

    return totalBytesWritten;
  }

  @Override
  public void testFileCreationSetsAttributes() throws IOException {
  }

  @Override
  public void testFileCreationUpdatesParentDirectoryModificationTimestamp()
      throws IOException, InterruptedException {
  }

  @Override
  public void testMkdirsUpdatesParentDirectoryModificationTimestamp()
      throws IOException, InterruptedException {
  }

  @Override
  public void testDeleteUpdatesDirectoryModificationTimestamps()
      throws IOException, InterruptedException {
  }

  @Override
  public void testRenameUpdatesParentDirectoryModificationTimestamps()
      throws IOException, InterruptedException {
  }

  @Override
  public void testPredicateIsConsultedForModificationTimestamps()
      throws IOException, InterruptedException {
  }
}
