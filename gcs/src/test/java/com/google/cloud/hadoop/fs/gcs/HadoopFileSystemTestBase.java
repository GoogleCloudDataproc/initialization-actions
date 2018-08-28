/*
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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopSyncableOutputStreamTest.hsync;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Abstract base class with extended tests against the base Hadoop FileSystem interface but with
 * coverage of tricky edge cases relevant to GoogleHadoopFileSystem. Tests here are intended to be
 * useful for all different FileSystem implementations.
 *
 * <p>We reuse test code from GoogleCloudStorageIntegrationTest and
 * GoogleCloudStorageFileSystemIntegrationTest.
 */
public abstract class HadoopFileSystemTestBase extends GoogleCloudStorageFileSystemIntegrationTest {

  // GHFS access instance.
  static FileSystem ghfs;
  static FileSystemDescriptor ghfsFileSystemDescriptor;

  protected static HadoopFileSystemIntegrationHelper ghfsHelper;

  public static void postCreateInit() throws IOException {
    postCreateInit(
        new HadoopFileSystemIntegrationHelper(ghfs, ghfsFileSystemDescriptor));
  }

  /**
   * Perform initialization after creating test instances.
   */
  public static void postCreateInit(HadoopFileSystemIntegrationHelper helper)
      throws IOException {
    ghfsHelper = helper;
    GoogleCloudStorageFileSystemIntegrationTest.postCreateInit(ghfsHelper);

    // Ensures that we do not accidentally end up testing wrong functionality.
    gcsfs = null;
  }

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(HadoopFileSystemTestBase.class) {
        @Override
        public void after() {
          if (ghfs != null) {
            if (ghfs instanceof GoogleHadoopFileSystemBase) {
              gcs = ((GoogleHadoopFileSystemBase) ghfs).getGcsFs().getGcs();
            }
            GoogleCloudStorageFileSystemIntegrationTest.storageResource.after();
            try {
              ghfs.close();
            } catch (IOException e) {
              throw new RuntimeException("Unexpected exception", e);
            }
            ghfs = null;
            ghfsFileSystemDescriptor = null;
          }
        }
      };

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

      long expectedSize =
          ghfsHelper.getExpectedObjectSize(objectName, expectedToExist);
      if (expectedSize != Long.MIN_VALUE) {
        Assert.assertEquals(message, expectedSize, fileStatus.getLen());
      }

      boolean expectedToBeDir =
          Strings.isNullOrEmpty(objectName)
          || ghfsHelper.objectHasDirectoryPath(objectName);
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
          modificationTime.isEqual(testStartTime.minus(Duration.millis(1000)))
              || modificationTime.isAfter(testStartTime.minus(Duration.millis(1000))));
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
   * <p>See {@link GoogleCloudStorageIntegrationTest#listObjectNamesAndGetItemInfo()} for more info.
   */
  @Override
  protected void validateGetItemInfo(String bucketName, String objectName, boolean expectedToExist)
      throws IOException {
    URI path = ghfsHelper.getPath(bucketName, objectName, true);
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
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
        Path expectedPath = ghfsHelper.castAsHadoopPath(
            ghfsHelper.getPath(pathComponents[0], pathComponents[1], true));
        expectedPaths.add(expectedPath);
        pathToComponents.put(expectedPath, pathComponents);
      }
    }

    // Get list of actual paths.
    URI path = ghfsHelper.getPath(bucketName, objectNamePrefix, true);
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    FileStatus[] fileStatus = null;
    try {
      fileStatus = ghfsHelper.listStatus(hadoopPath);
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

  // -----------------------------------------------------------------
  // Tests that test behavior at GHFS layer.

  /**
   * Tests read() when invalid arguments are passed.
   */
  @Test
  public void testReadInvalidArgs()
      throws IOException {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    ghfsHelper.writeFile(hadoopPath, "file text", 1, true);
    FSDataInputStream readStream =
        ghfs.open(hadoopPath, GoogleHadoopFileSystemConfiguration.BUFFERSIZE.getDefault());
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
    Exception e = assertThrows(Exception.class, () -> readStream.read(buffer, offset, length));
    if (e.getClass() != exceptionClass) {
        Assert.fail("Unexpected exception: " + e);
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
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    FSDataOutputStream writeStream = null;
    writeStream = ghfs.create(hadoopPath);

    // Write one byte at a time.
    for (byte b : textBytes) {
      writeStream.write(b);
    }
    writeStream.close();

    // Read the file back and verify contents.
    String readText =
        ghfsHelper.readTextFile(hadoopPath, 0, textBytes.length, true);
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
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(hadoopPath));

    // Create a file.
    String text = "Hello World!";
    int numBytesWritten = ghfsHelper.writeFile(hadoopPath, text, 1, false);
    Assert.assertEquals(text.getBytes("UTF-8").length, numBytesWritten);

    // Try to create the same file again with overwrite == false.
    assertThrows(IOException.class, () -> ghfsHelper.writeFile(hadoopPath, text, 1, false));
  }

  /**
   * Validates append().
   */
  @Test
  public void testAppend()
      throws IOException {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    assertThrows(
        IOException.class,
        () ->
            ghfs.append(
                hadoopPath, GoogleHadoopFileSystemConfiguration.BUFFERSIZE.getDefault(), null));
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
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    String text = "Hello World!";
    int numBytesWritten = ghfsHelper.writeFile(hadoopPath, text, 1, false);

    // Verify that position is at 0 for a newly opened stream.
    try (FSDataInputStream readStream =
        ghfs.open(hadoopPath, GoogleHadoopFileSystemConfiguration.BUFFERSIZE.getDefault())) {
      Assert.assertEquals(0, readStream.getPos());

      // Verify that position advances by 2 after reading 2 bytes.
      Assert.assertEquals((int) 'H', readStream.read());
      Assert.assertEquals((int) 'e', readStream.read());
      Assert.assertEquals(2, readStream.getPos());

      // Verify that setting position to the same value is a no-op.
      readStream.seek(2);
      Assert.assertEquals(2, readStream.getPos());
      readStream.seek(2);
      Assert.assertEquals(2, readStream.getPos());

      // Verify that position can be set to a valid position.
      readStream.seek(6);
      Assert.assertEquals(6, readStream.getPos());
      Assert.assertEquals((int) 'W', readStream.read());

      // Verify that position can be set to end of file.
      long posEOF = numBytesWritten - 1;
      int val;
      readStream.seek(posEOF);
      Assert.assertEquals(posEOF, readStream.getPos());
      val = readStream.read();
      Assert.assertTrue(val != -1);
      val = readStream.read();
      Assert.assertEquals(-1, val);
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
      assertThrows(IOException.class, () -> readStream.seek(numBytesWritten + 1));

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
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);

    int bufferSize = 8 * 1024 * 1024;

    byte[] testBytes = new byte[bufferSize * 3];
    // The value of each byte should be each to its integer index squared, cast as a byte.
    for (int i = 0; i < testBytes.length; ++i) {
      testBytes[i] = (byte) (i * i);
    }
    int numBytesWritten = ghfsHelper.writeFile(hadoopPath, ByteBuffer.wrap(testBytes), 1, false);
    Assert.assertEquals(testBytes.length, numBytesWritten);

    try (FSDataInputStream readStream = ghfs.open(hadoopPath, bufferSize)) {
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
      int midPos = bufferSize / 2;
      readStream.seek(midPos);
      Assert.assertEquals(midPos, readStream.getPos());
      Assert.assertEquals(testBytes[midPos], (byte) readStream.read());

      // Seek backwards after we got here from some seeking, seek close to but not equal to
      // the beginning.
      readStream.seek(42);
      Assert.assertEquals(42, readStream.getPos());
      Assert.assertEquals(testBytes[42], (byte) readStream.read());

      // Seek to right before the end of the internal buffer.
      int edgePos = bufferSize - 1;
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
      int bufferTwoStart = bufferSize * 2;
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
      int bufferOneInternal = bufferSize + 42;
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
    Path parentPath = ghfsHelper.castAsHadoopPath(parentUri);
    Path escapedPath = new Path(parentPath, new Path("foo%3Abar"));

    ghfsHelper.writeFile(escapedPath, "foo", 1, true);
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
        HadoopFileSystemIntegrationHelper ghfsHelper,
        String objectName, String expectedObjectName) {
      return new WorkingDirData(
          ghfsHelper.createSchemeCompatibleHadoopPath(sharedBucketName1, objectName),
          ghfsHelper.createSchemeCompatibleHadoopPath(sharedBucketName1, expectedObjectName));
    }

    /**
     * Constructs an instance of WorkingDirData.
     *
     * The given objectName is converted to an absolute path by combining it with
     * the default test bucket. The resulting path is passed to setWorkingDirectory().
     */
    static WorkingDirData absolute(
        HadoopFileSystemIntegrationHelper ghfsHelper, String objectName) {
      return new WorkingDirData(
          ghfsHelper.createSchemeCompatibleHadoopPath(sharedBucketName1, objectName), null);
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
    ghfsHelper.clearBucket(sharedBucketName1);
    ghfsHelper.createObjectsWithSubdirs(sharedBucketName1, objectNames);

    // -------------------------------------------------------
    // Initialize test data.
    List<WorkingDirData> wddList = new ArrayList<>();

    // Set working directory to root.
    Path rootPath = ghfsFileSystemDescriptor.getFileSystemRoot();
    wddList.add(WorkingDirData.any(rootPath, rootPath));

    // Set working directory to an existing directory (empty).
    wddList.add(WorkingDirData.absolute(ghfsHelper, "d0/", "d0/"));

    // Set working directory to an existing directory (non-empty).
    wddList.add(WorkingDirData.absolute(ghfsHelper, "d1/", "d1/"));


    // Set working directory to an existing directory (multi-level).
    wddList.add(WorkingDirData.absolute(ghfsHelper, "d1/d11/", "d1/d11/"));

    // Set working directory to an existing directory (bucket).
    wddList.add(WorkingDirData.absolute(
        ghfsHelper, (String) null, (String) null));

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

  @Test
  public void testHsync() throws Exception {
    internalTestHsync();
  }

  @Test
  public void testReadToEOFAndRewind() throws IOException {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();

    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    byte[] byteBuffer = new byte[1024];
    for (int i = 0; i < byteBuffer.length; i++) {
      byteBuffer[i] = (byte) (i % 255);
    }
    ghfsHelper.writeFile(hadoopPath, ByteBuffer.wrap(byteBuffer), 1, false /* overwrite */);
    try (FSDataInputStream input = ghfs.open(hadoopPath)) {
      byte[] readBuffer1 = new byte[512];
      input.seek(511);
      Assert.assertEquals(512, input.read(readBuffer1, 0, 512));
      input.seek(0);

      input.seek(511);
      Assert.assertEquals(512, input.read(readBuffer1, 0, 512));
    } finally {
      ghfs.delete(hadoopPath);
    }
  }

  protected void internalTestHsync() throws Exception {
    String line1 = "hello\n";
    byte[] line1Bytes = line1.getBytes("UTF-8");
    String line2 = "world\n";
    byte[] line2Bytes = line2.getBytes("UTF-8");
    String line3 = "foobar\n";
    byte[] line3Bytes = line3.getBytes("UTF-8");

    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    FSDataOutputStream writeStream = ghfs.create(hadoopPath);

    StringBuilder expected = new StringBuilder();

    // Write first line one byte at a time.
    for (byte b : line1Bytes) {
      writeStream.write(b);
    }
    expected.append(line1);

    hsync(writeStream);

    String readText = ghfsHelper.readTextFile(hadoopPath);
    Assert.assertEquals("Expected line1 after first sync()", expected.toString(), readText);

    // Write second line, sync() again.
    writeStream.write(line2Bytes, 0, line2Bytes.length);
    expected.append(line2);
    hsync(writeStream);
    readText = ghfsHelper.readTextFile(hadoopPath);
    Assert.assertEquals(
        "Expected line1 + line2 after second sync()", expected.toString(), readText);

    // Write third line, close() without sync().
    writeStream.write(line3Bytes, 0, line3Bytes.length);
    expected.append(line3);
    writeStream.close();
    readText = ghfsHelper.readTextFile(hadoopPath);
    Assert.assertEquals(
        "Expected line1 + line2 + line3 after close()", expected.toString(), readText);
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because they do not make sense
  // in the context of this layer.
  // -----------------------------------------------------------------

  @Override
  public void testGetFileInfos() {}

  @Override
  public void testFileCreationSetsAttributes() {}

  @Override
  public void testFileCreationUpdatesParentDirectoryModificationTimestamp() {}

  @Override
  public void testMkdirsUpdatesParentDirectoryModificationTimestamp() {}

  @Override
  public void testDeleteUpdatesDirectoryModificationTimestamps() {}

  @Override
  public void renameDirectoryShouldCopyMarkerFilesLast() {}

  @Override
  public void testRenameUpdatesParentDirectoryModificationTimestamps() {}

  @Override
  public void testPredicateIsConsultedForModificationTimestamps() {}

  @Override
  public void testComposeSuccess() throws IOException {}

  @Override
  public void testReadGenerationBestEffort() throws IOException {}

  @Override
  public void testReadGenerationStrict() throws IOException {}
}
