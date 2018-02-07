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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.expectThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(user): add tests for multi-threaded reads/writes
/**
 * Integration tests for GoogleCloudStorageFileSystem class.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemIntegrationTest {
  // Logger.
  protected static final Logger LOG =
      LoggerFactory.getLogger(GoogleCloudStorageFileSystemIntegrationTest.class);

  // GCS FS test access instance.
  protected static GoogleCloudStorageFileSystem gcsfs;

  // GCS instance used for cleanup
  protected static GoogleCloudStorage gcs;

  // My sister was once bitten by a moose, let's not update those paths
  protected static final String EXCLUDED_TIMESTAMP_SUBSTRING = "moose/";

  // I like turtles, let's always update those paths
  protected static final String INCLUDED_TIMESTAMP_SUBSTRING = "turtles/";

  protected static final Predicate<String> INCLUDE_SUBSTRINGS_PREDICATE = new Predicate<String>() {
    @Override
    public boolean apply(String path) {
       if (path.contains(INCLUDED_TIMESTAMP_SUBSTRING)) {
        return true; // Don't ignore
      }

      if (path.contains(EXCLUDED_TIMESTAMP_SUBSTRING)) {
        return false; // Ignore
      }

      return true; // Include everything else
    }
  };

  protected static GoogleCloudStorageFileSystemIntegrationHelper gcsiHelper;

  // Time when test started. Used for determining which objects got
  // created after the test started.
  protected static Instant testStartTime;

  protected static String sharedBucketName1;
  protected static String sharedBucketName2;

  // Name of the test object.
  protected static String objectName = "gcsio-test.txt";

  protected static final boolean GCS_FILE_SIZE_LIMIT_250GB_DEFAULT = true;
  protected static final int WRITE_BUFFERSIZE_DEFAULT = 64 * 1024 * 1024;

  /**
   * Perform initialization once before tests are run.
   */
  @BeforeClass
  public static void beforeAllTests() throws Exception {
    if (gcsfs == null) {
      Credential credential = GoogleCloudStorageTestHelper.getCredential();
      String appName = GoogleCloudStorageIntegrationHelper.APP_NAME;
      String projectId = TestConfiguration.getInstance().getProjectId();
      assertThat(projectId).isNotNull();

      GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
          GoogleCloudStorageFileSystemOptions.newBuilder();

      optionsBuilder
          .setIsMetadataCacheEnabled(true)
          .setEnableBucketDelete(true)
          .setShouldIncludeInTimestampUpdatesPredicate(INCLUDE_SUBSTRINGS_PREDICATE)
          .getCloudStorageOptionsBuilder()
          .setAppName(appName)
          .setProjectId(projectId)
          .getWriteChannelOptionsBuilder()
          .setFileSizeLimitedTo250Gb(GCS_FILE_SIZE_LIMIT_250GB_DEFAULT)
          .setUploadBufferSize(WRITE_BUFFERSIZE_DEFAULT);

      gcsfs = new GoogleCloudStorageFileSystem(
          credential,
          optionsBuilder.build());

      gcsfs.setUpdateTimestampsExecutor(MoreExecutors.newDirectExecutorService());

      gcs = gcsfs.getGcs();

      postCreateInit();
    }
  }

  public static void postCreateInit() throws IOException {
    postCreateInit(new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs));
  }

  /**
   * Perform initialization after creating test instances.
   */
  public static void postCreateInit(
      GoogleCloudStorageFileSystemIntegrationHelper helper)
      throws IOException {
    testStartTime = Instant.now();

    gcsiHelper = helper;
    gcsiHelper.beforeAllTests();
    sharedBucketName1 = gcsiHelper.sharedBucketName1;
    sharedBucketName2 = gcsiHelper.sharedBucketName2;
  }

  /** Perform clean-up once after all tests are turn. */
  @AfterClass
  public static void afterAllTests() throws IOException {
    if (gcs != null) {
      gcsiHelper.afterAllTests(gcs);
    }
    if (gcsfs != null) {
      gcsfs.close();
      gcsfs = null;
    }
  }

  // -----------------------------------------------------------------
  // Overridden methods to ensure that GCS FS functionality is used
  // instead of GCS functionality where applicable.
  // -----------------------------------------------------------------

  /**
   * Actual logic for validating the GoogleCloudStorageFileSystem-specific FileInfo returned by
   * getItemInfo() or listFileInfo().
   */
  private void validateFileInfoInternal(
      String bucketName, String objectName, boolean expectedToExist, FileInfo fileInfo)
      throws IOException {
    assertThat(fileInfo.exists()).isEqualTo(expectedToExist);

    long expectedSize = gcsiHelper.getExpectedObjectSize(objectName, expectedToExist);
    if (expectedSize != Long.MIN_VALUE) {
      assertThat(fileInfo.getSize()).isEqualTo(expectedSize);
    }

    boolean expectedDirectory =
        (objectName == null) || FileInfo.objectHasDirectoryPath(objectName);
    assertWithMessage("isDirectory for bucketName '%s' objectName '%s'", bucketName, objectName)
        .that(fileInfo.isDirectory())
        .isEqualTo(expectedDirectory);

    if (expectedToExist) {
      Instant currentTime = Instant.now();
      Instant fileCreationTime = new Instant(fileInfo.getCreationTime());

      assertWithMessage(
              "stale file? testStartTime: %s, fileCreationTime: %s",
              testStartTime, fileCreationTime)
          .that(fileCreationTime)
          .isAtLeast(testStartTime);
      assertWithMessage(
              "unexpected creation-time: clock skew? currentTime: %s, fileCreationTime: %s",
              currentTime, fileCreationTime)
          .that(fileCreationTime)
          .isAtMost(currentTime);
    } else {
      assertThat(fileInfo.getCreationTime()).isEqualTo(0);
    }

    assertThat(fileInfo.toString()).isNotEmpty();
  }

  /**
   * Validates FileInfo for the given item.
   * <p>
   * See {@link #testListObjectNamesAndGetItemInfo()} for more info.
   * <p>
   * Note: The test initialization code creates objects as text files.
   * Each text file contains name of its associated object.
   * Therefore, size of an object == objectName.getBytes("UTF-8").length.
   */
  protected void validateGetItemInfo(String bucketName, String objectName, boolean expectedToExist)
      throws IOException {
    URI path = gcsiHelper.getPath(bucketName, objectName);
    FileInfo fileInfo = gcsfs.getFileInfo(path);
    assertThat(fileInfo.getPath()).isEqualTo(path);
    validateFileInfoInternal(bucketName, objectName, expectedToExist, fileInfo);
  }

  /**
   * Validates FileInfo returned by listFileInfo().
   * <p>
   * See {@link #testListObjectNamesAndGetItemInfo()} for more info.
   */
  protected void validateListNamesAndInfo(String bucketName, String objectNamePrefix,
      boolean pathExpectedToExist, String... expectedListedNames)
      throws IOException {

    boolean childPathsExpectedToExist =
        pathExpectedToExist && (expectedListedNames != null);
    boolean listRoot = bucketName == null;

    // Prepare list of expected paths.
    List<URI> expectedPaths = new ArrayList<>();
    // Also maintain a backwards mapping to keep track of which of "expectedListedNames" and
    // "bucketName" is associated with each path, so that we can supply validateFileInfoInternal
    // with the objectName and thus enable it to lookup the internally stored expected size,
    // directory status, etc., of the associated FileStatus.
    Map<URI, String[]> pathToComponents = new HashMap<>();
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
        URI expectedPath = gcsiHelper.getPath(pathComponents[0], pathComponents[1]);
        expectedPaths.add(expectedPath);
        pathToComponents.put(expectedPath, pathComponents);
      }
    }

    // Get list of actual paths.
    URI path = gcsiHelper.getPath(bucketName, objectNamePrefix);
    List<FileInfo> fileInfos;

    try {
      fileInfos = gcsfs.listFileInfo(path, false);
      if (!pathExpectedToExist) {
        Assert.fail("Expected FileNotFoundException for path: " + path);
      }
    } catch (FileNotFoundException e) {
      fileInfos = new ArrayList<>();
      if (pathExpectedToExist) {
        Assert.fail("Did not expect FileNotFoundException for path: " + path);
      }
    }

    List<URI> actualPaths = new ArrayList<>();
    for (FileInfo fileInfo : fileInfos) {
      assertWithMessage("File exists? : " + fileInfo.getPath())
          .that(fileInfo.exists())
          .isEqualTo(childPathsExpectedToExist);
      if (fileInfo.exists()) {
        actualPaths.add(fileInfo.getPath());
        String[] uriComponents = pathToComponents.get(fileInfo.getPath());
        if (uriComponents != null) {
          // Only do fine-grained validation for the explicitly expected paths.
          validateFileInfoInternal(uriComponents[0], uriComponents[1], true, fileInfo);
        }
      }
    }

    if (listRoot) {
      assertThat(actualPaths).containsAllIn(expectedPaths);
    } else {
      assertThat(actualPaths).containsExactlyElementsIn(expectedPaths);
    }

    // Now re-verify using listFileNames instead of listFileInfo.
    FileInfo baseInfo = gcsfs.getFileInfo(path);
    List<URI> listedUris = gcsfs.listFileNames(baseInfo);

    if (!baseInfo.isDirectory() && !baseInfo.exists()) {
      // This is one case which differs between listFileInfo and listFileNames; listFileInfo will
      // throw an exception for non-existent paths, while listFileNames will *always* return the
      // unaltered path itself as long as it's not a directory. If it's a non-existent directory
      // path, it returns an empty list, as opposed to this case, where it's a list of size 1.
      expectedPaths.add(path);
    }

    if (listRoot) {
      // By nature of the globally-visible GCS root (gs://), as long as we share a project for
      // multiple testing purposes there's no way to know the exact expected contents to be listed,
      // because other people/tests may have their own buckets alongside those created by this test.
      // So, we just check that the expectedPaths are at least a subset of the listed ones.
      Set<URI> actualPathsSet = new HashSet<>(listedUris);
      for (URI expectedPath : expectedPaths) {
        assertWithMessage("expected: <%s> in: <%s>", expectedPath, actualPathsSet)
            .that(actualPathsSet)
            .contains(expectedPath);
      }
    } else {
      assertThat(expectedPaths).containsExactlyElementsIn(listedUris);
    }
  }

  // -----------------------------------------------------------------
  // Tests added by this class.
  // -----------------------------------------------------------------

  /**
   * Contains data needed for testing the delete() operation.
   */
  private static class DeleteData {

    // Description of test case.
    String description;

    // Bucket component of the path to delete.
    String bucketName;

    // Object component of the path to delete.
    String objectName;

    // Indicates whether it is a recursive delete.
    boolean recursive;

    // Expected outcome; can return true, return false, or return exception of a certain type.
    MethodOutcome expectedOutcome;

    // Objects expected to exist after the operation.
    List<String> objectsExpectedToExist;

    // Objects expected to be deleted after the operation.
    List<String> objectsExpectedToBeDeleted;

    /**
     * Constructs an instance of the DeleteData class.
     */
    DeleteData(
        String description,
        String bucketName, String objectName, boolean recursive,
        MethodOutcome expectedOutcome,
        List<String> objectsExpectedToExist,
        List<String> objectsExpectedToBeDeleted) {

      this.description = description;
      this.bucketName = bucketName;
      this.objectName = objectName;
      this.recursive = recursive;
      this.expectedOutcome = expectedOutcome;
      this.objectsExpectedToExist = objectsExpectedToExist;
      this.objectsExpectedToBeDeleted = objectsExpectedToBeDeleted;
    }
  }

  /**
   * Validates delete().
   */
  @Test
  public void testDelete()
      throws IOException {
    deleteHelper(new DeletionBehavior() {
      @Override
      public MethodOutcome nonEmptyDeleteOutcome() {
        // GCSFS throws IOException on non-recursive delete of non-empty dir.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }

      @Override
      public MethodOutcome nonExistentDeleteOutcome() {
        // GCSFS throws FileNotFoundException if deleting a non-existent file.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, FileNotFoundException.class);
      }
    });
  }

  /**
   * Tests listObjectNames() and getItemInfo().
   *
   * The data required for the 2 tests is expensive to create therefore
   * we combine the tests into one.
   */
  @Test
  public void testListObjectNamesAndGetItemInfo()
      throws IOException {

    // Objects created for this test.
    String[] objectNames = {
        "o1",
        "o2",
        "d0/",
        "d1/o11",
        "d1/o12",
        "d1/d10/",
        "d1/d11/o111",
        "d2/o21",
        "d2/o22",
    };

    String dirDoesNotExist = "does-not-exist/";
    String objDoesNotExist = "does-not-exist";

    // -------------------------------------------------------
    // Create test objects.
    String tempTestBucket = gcsiHelper.createUniqueBucket("list");
    gcsiHelper.createObjectsWithSubdirs(tempTestBucket, objectNames);

    // -------------------------------------------------------
    // Tests for getItemInfo().
    // -------------------------------------------------------

    // Verify that getItemInfo() returns correct info for each object.
    for (String objectName : objectNames) {
      validateGetItemInfo(tempTestBucket, objectName, true);
    }

    // Verify that getItemInfo() returns correct info for bucket.
    validateGetItemInfo(tempTestBucket, null, true);

    // Verify that getItemInfo() returns correct info for a non-existent object.
    validateGetItemInfo(tempTestBucket, dirDoesNotExist, false);

    // Verify that getItemInfo() returns correct info for a non-existent bucket.
    validateGetItemInfo(tempTestBucket, objDoesNotExist, false);

    // -------------------------------------------------------
    // Tests for listObjectNames().
    // -------------------------------------------------------

    // Verify that listObjectNames() returns correct names for each case below.

    // At root.
    validateListNamesAndInfo(tempTestBucket, null, true, "o1", "o2", "d0/", "d1/", "d2/");
    validateListNamesAndInfo(tempTestBucket, "", true, "o1", "o2", "d0/", "d1/", "d2/");

    // At d0.
    validateListNamesAndInfo(tempTestBucket, "d0/", true);

    // At o1.
    validateListNamesAndInfo(tempTestBucket, "o1", true, "o1");

    // TODO(user) : bug in GCS? fails only when running gcsfs tests?
    // validateListNamesAndInfo(bucketName, "d0", true, "d0/");

    // At d1.
    validateListNamesAndInfo(tempTestBucket, "d1/", true, "d1/o11", "d1/o12", "d1/d10/", "d1/d11/");

    // TODO(user) : bug in GCS? fails only when running gcsfs tests?
    // validateListNamesAndInfo(bucketName, "d1", true, "d1/");

    // At d1/d11.
    validateListNamesAndInfo(tempTestBucket, "d1/d11/", true, "d1/d11/o111");

    // TODO(user) : bug in GCS? fails only when running gcsfs tests?
    // validateListNamesAndInfo(bucketName, "d1/d11", true, "d1/d11/");

    // At d2.
    validateListNamesAndInfo(tempTestBucket, "d2/", true, "d2/o21", "d2/o22");

    // TODO(user) : bug in GCS? fails only when running gcsfs tests?
    // validateListNamesAndInfo(bucketName, "d2", true, "d2/");

    // At non-existent path.
    validateListNamesAndInfo(tempTestBucket, dirDoesNotExist, false);
    validateListNamesAndInfo(tempTestBucket, objDoesNotExist, false);
    validateListNamesAndInfo(objDoesNotExist, objDoesNotExist, false);

    // -------------------------------------------------------
    // Tests for listObjectNames().
    // -------------------------------------------------------
    validateListNamesAndInfo(
        null, null, true, sharedBucketName1, sharedBucketName2, tempTestBucket);
  }

  @Test @SuppressWarnings("EqualsIncompatibleType")
  public void testGoogleCloudStorageItemInfoNegativeEquality() {
    // Assert that .equals with an incorrect type returns false and does not throw.
    assertThat(!GoogleCloudStorageItemInfo.ROOT_INFO.equals("non-item-info")).isTrue();
  }

  /**
   * Validates simple write and read operations.
   */
  @Test
  public void testWriteAndReadObject()
      throws IOException {
    String bucketName = sharedBucketName1;
    String message = "Hello world!\n";

    // Write an object.
    int numBytesWritten = gcsiHelper.writeTextFile(
        bucketName, objectName, message);

    // Read the whole object.
    String message2 = gcsiHelper.readTextFile(
        bucketName, objectName, 0, numBytesWritten, true);

    // Verify we read what we wrote.
    assertThat(message2).isEqualTo(message);
  }

  /**
   * Validates partial reads.
   */
  @Test
  public void testReadPartialObject()
      throws IOException {
    String bucketName = sharedBucketName1;
    String message = "Hello world!\n";

    // Write an object.
    gcsiHelper.writeTextFile(bucketName, objectName, message);

    // Read the whole object in 2 parts.
    int offset = 6;  // chosen arbitrarily.
    String message1 = gcsiHelper.readTextFile(
        bucketName, objectName, 0, offset, false);
    String message2 = gcsiHelper.readTextFile(
        bucketName, objectName, offset, message.length() - offset, true);

    // Verify we read what we wrote.
    assertWithMessage("partial read mismatch")
        .that(message1)
        .isEqualTo(message.substring(0, offset));
    assertWithMessage("partial read mismatch").that(message2).isEqualTo(message.substring(offset));
  }

  /**
   * Validates that we cannot open a non-existent object.
   */
  @Test
  public void testOpenNonExistent()
      throws IOException {
    String bucketName = gcsiHelper.getUniqueBucketName("open-non-existent");
    assertThrows(
        FileNotFoundException.class,
        () -> gcsiHelper.readTextFile(bucketName, objectName, 0, 100, true));
  }

  /**
   * Validates delete().
   */
  public void deleteHelper(DeletionBehavior behavior)
      throws IOException {
    String bucketName = sharedBucketName1;

    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "d0/",
      "d1/f1",
      "d1/d0/",
      "d1/d11/f1",
    };

    // -------------------------------------------------------
    // Create test objects.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, objectNames);

    // The same set of objects are also created under a bucket that
    // we will delete as a part of the test.
    String tempBucket = gcsiHelper.createUniqueBucket("delete");
    gcsiHelper.createObjectsWithSubdirs(tempBucket, objectNames);

    // -------------------------------------------------------
    // Initialize test data.
    List<DeleteData> deleteData = new ArrayList<>();
    String doesNotExist = "does-not-exist";
    String dirDoesNotExist = "does-not-exist";

    // Delete an item that does not exist.
    deleteData.add(new DeleteData(
        "Delete an object that does not exist: file",
        bucketName, doesNotExist, false,
        behavior.nonExistentDeleteOutcome(),  // expected outcome
        null,  // expected to exist
        null));  // expected to be deleted
    deleteData.add(new DeleteData(
        "Delete an object that does not exist: dir",
        bucketName, dirDoesNotExist, false,
        behavior.nonExistentDeleteOutcome(),  // expected outcome
        null,  // expected to exist
        null));  // expected to be deleted
    deleteData.add(new DeleteData(
        "Delete a bucket that does not exist",
        doesNotExist, doesNotExist, false,
        behavior.nonExistentDeleteOutcome(),  // expected outcome
        null,  // expected to exist
        null));  // expected to be deleted

    // Delete an empty directory.
    deleteData.add(new DeleteData(
        "Delete an empty directory",
        bucketName, "d0/", true,
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        null,  // expected to exist
        Lists.newArrayList("d0/")));  // expected to be deleted

    // Delete a non-empty directory (recursive == false).
    deleteData.add(new DeleteData(
        "Delete a non-empty directory (recursive == false)",
        bucketName, "d1/", false,
        behavior.nonEmptyDeleteOutcome(),  // expected outcome
        Lists.newArrayList("d1/", "d1/f1", "d1/d0/", "d1/d11/f1"),  // expected to exist
        null));  // expected to be deleted

    // Delete a non-empty directory (recursive == true).
    deleteData.add(new DeleteData(
        "Delete a non-empty directory (recursive == true)",
        bucketName, "d1/", true,
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        null,  // expected to exist
        Lists.newArrayList("d1/", "d1/f1", "d1/d0/", "d1/d11/f1")));  // expected to be deleted

    // Delete a non-empty bucket (recursive == false).
    deleteData.add(new DeleteData(
        "Delete a non-empty bucket (recursive == false)",
        tempBucket, null, false,
        behavior.nonEmptyDeleteOutcome(),  // expected outcome
        Lists.newArrayList(
            // expected to exist
            "f1", "d0/", "d1/", "d1/f1", "d1/d0/", "d1/d11/f1"),
        null));  // expected to be deleted

    // Delete a non-empty bucket (recursive == true).
    deleteData.add(new DeleteData(
        "Delete a non-empty bucket (recursive == true)",
        tempBucket, null, true,
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        null,  // expected to exist
        Lists.newArrayList(
            // expected to be deleted
            "f1", "d0/", "d1/", "d1/f1", "d1/d0/", "d1/d11/f1")));

    // -------------------------------------------------------
    // Call delete() for each path and verify the expected behavior.
    for (DeleteData dd : deleteData) {

      // Verify that items that we expect to delete are present before the operation.
      assertPathsExist(dd.description, dd.bucketName, dd.objectsExpectedToBeDeleted, true);

      URI path = gcsiHelper.getPath(dd.bucketName, dd.objectName);
      try {

        // Perform the delete operation.
        boolean result = gcsiHelper.delete(path, dd.recursive);

        if (result) {
          assertWithMessage(
                  "Unexpected result for '%s' path: %s :: expected %s, actually returned true.",
                  path, dd.description, dd.expectedOutcome)
              .that(dd.expectedOutcome.getType())
              .isEqualTo(MethodOutcome.Type.RETURNS_TRUE);
        } else {
          assertWithMessage(
                  "Unexpected result for '%s' path: %s :: expected %s, actually returned false.",
                  path, dd.description, dd.expectedOutcome)
              .that(dd.expectedOutcome.getType())
              .isEqualTo(MethodOutcome.Type.RETURNS_FALSE);
        }
      } catch (Exception e) {
        assertWithMessage(
                "Unexpected result for '%s' path: %s :: expected %s, actually threw exception %s",
                path, dd.description, dd.expectedOutcome, Throwables.getStackTraceAsString(e))
            .that(dd.expectedOutcome.getType())
            .isEqualTo(MethodOutcome.Type.THROWS_EXCEPTION);
      }

      // Verify that items that we expect to exist are present.
      assertPathsExist(dd.description, dd.bucketName, dd.objectsExpectedToExist, true);

      // Verify that items that we expect to be deleted are not present.
      assertPathsExist(dd.description, dd.bucketName, dd.objectsExpectedToBeDeleted, false);
    }
  }

  /**
   * Call mkdir then create a file with the same name, not including the trailing slash for the
   * param to mkdir. The create should fail.
   */
  @Test
  public void testMkdirAndCreateFileOfSameName()
      throws IOException, URISyntaxException {
    String bucketName = sharedBucketName1;
    String uniqueDirName = "dir-" + UUID.randomUUID();
    gcsiHelper.mkdir(
        bucketName, uniqueDirName + GoogleCloudStorage.PATH_DELIMITER);
    IOException ioe =
        expectThrows(
            IOException.class,
            () -> gcsiHelper.writeTextFile(bucketName, uniqueDirName, "hello world"));
    assertWithMessage(
            String.format(
                "unexpected exception: %s\n%s",
                ioe.getMessage(), Throwables.getStackTraceAsString(ioe)))
        .that(ioe.getMessage().matches(".*(A directory with that name exists|Is a directory).*"))
        .isTrue();
    gcsiHelper.delete(bucketName, uniqueDirName);
  }

  /**
   * Validates mkdirs().
   */
  @Test
  public void testMkdirs()
      throws IOException, URISyntaxException {
    mkdirsHelper(new MkdirsBehavior() {
      @Override
      public MethodOutcome mkdirsRootOutcome() {
        return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
      }

      @Override
      public MethodOutcome fileAlreadyExistsOutcome() {
        return new MethodOutcome(MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }
    });
  }

  /**
   * Validates mkdirs().
   */
  public void mkdirsHelper(MkdirsBehavior behavior)
      throws IOException, URISyntaxException {
    String bucketName = sharedBucketName1;

    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "d0/",
      "d1/f11",
    };

    // -------------------------------------------------------
    // Create test objects.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, objectNames);

    // -------------------------------------------------------
    // Initialize test data.
    // key == directory path to pass to mkdirs()
    // val == Expected outcome
    Map<URI, MethodOutcome> dirData = new HashMap<>();

    // Verify that attempt to create root dir does not throw (no-op).
    dirData.put(GoogleCloudStorageFileSystem.GCS_ROOT, behavior.mkdirsRootOutcome());

    // Verify that no exception is thrown when directory already exists.
    dirData.put(gcsiHelper.getPath(bucketName, "d0/"),
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));
    dirData.put(gcsiHelper.getPath(bucketName, "d0"),
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Expect IOException if a file with the given name already exists.
    dirData.put(gcsiHelper.getPath(bucketName, "f1/"),
        behavior.fileAlreadyExistsOutcome());
    dirData.put(gcsiHelper.getPath(bucketName, "d1/f11/d3/"),
        behavior.fileAlreadyExistsOutcome());

    // Some intermediate directories exist (but not all).
    dirData.put(gcsiHelper.getPath(bucketName, "d1/d2/d3/"),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // No intermediate directories exist.
    dirData.put(gcsiHelper.getPath(bucketName, "dA/dB/dC/"),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Trying to create the same dirs again is a no-op.
    dirData.put(gcsiHelper.getPath(bucketName, "dA/dB/dC/"),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Make paths that include making a top-level directory (bucket).
    String uniqueBucketName = gcsiHelper.getUniqueBucketName("mkdir-1");
    dirData.put(gcsiHelper.getPath(uniqueBucketName, null),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Create the same bucket again, should be no-op.
    dirData.put(gcsiHelper.getPath(uniqueBucketName, null),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Make a path where the bucket is a non-existent parent directory.
    String uniqueBucketName2 = gcsiHelper.getUniqueBucketName("mkdir-2");
    dirData.put(gcsiHelper.getPath(uniqueBucketName2, "foo/bar"),
                new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE));

    // Call mkdirs() for each path and verify the expected behavior.
    for (URI path : dirData.keySet()) {
      MethodOutcome expectedOutcome = dirData.get(path);
      try {
        boolean result = gcsiHelper.mkdirs(path);
        if (result) {
          assertWithMessage(
                  String.format(
                      "Unexpected result for path: %s : expected %s, actually returned true.",
                      path, expectedOutcome.toString()))
              .that(expectedOutcome.getType())
              .isEqualTo(MethodOutcome.Type.RETURNS_TRUE);

          // Assert that all of the sub-dirs have been created.
          List<URI> subDirPaths = getSubDirPaths(path);
          for (URI subDirPath : subDirPaths) {
            assertWithMessage(
                    String.format(
                        "Sub-path %s of path %s not found or not a dir", subDirPath, path))
                .that(gcsiHelper.exists(subDirPath) && gcsiHelper.isDirectory(subDirPath))
                .isTrue();
          }
        } else {
          assertWithMessage(
                  String.format(
                      "Unexpected result for path: %s : expected %s, actually returned false.",
                      path, expectedOutcome.toString()))
              .that(expectedOutcome.getType())
              .isEqualTo(MethodOutcome.Type.RETURNS_FALSE);
        }
      } catch (Exception e) {
        assertWithMessage(
                String.format(
                    "Unexpected result for path: %s : expected %s, actually threw exception %s.",
                    path, expectedOutcome.toString(), Throwables.getStackTraceAsString(e)))
            .that(expectedOutcome.getType())
            .isEqualTo(MethodOutcome.Type.THROWS_EXCEPTION);
      }
    }
  }

  /**
   * Validates getFileInfos().
   */
  @Test
  public void testGetFileInfos()
      throws IOException, URISyntaxException {
    String bucketName = sharedBucketName1;
    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "d0/",
    };

    // -------------------------------------------------------
    // Create test objects.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, objectNames);

    List<URI> pathsToGet = new ArrayList<>();
    // Mix up the types of the paths to ensure the method will return the values in the same order
    // as their respective input parameters regardless of whether some are ROOT, directories, etc.
    pathsToGet.add(gcsiHelper.getPath(bucketName, "nonexistent"));
    pathsToGet.add(gcsiHelper.getPath(bucketName, "f1"));
    pathsToGet.add(gcsiHelper.getPath(null, null));
    pathsToGet.add(gcsiHelper.getPath(bucketName, "d0"));
    pathsToGet.add(gcsiHelper.getPath(bucketName, null));

    List<FileInfo> fileInfos = gcsfs.getFileInfos(pathsToGet);

    // First one doesn't exist.
    assertThat(fileInfos.get(0).exists()).isFalse();
    assertThat(fileInfos.get(0).getItemInfo().getResourceId())
        .isEqualTo(new StorageResourceId(bucketName, "nonexistent"));

    // Second one exists and is a StorageObject.
    assertThat(fileInfos.get(1).exists()).isTrue();
    assertThat(fileInfos.get(1).getItemInfo().getResourceId().isStorageObject()).isTrue();
    assertThat(fileInfos.get(1).getItemInfo().getResourceId())
        .isEqualTo(new StorageResourceId(bucketName, "f1"));

    // Third one exists and is root.
    assertThat(fileInfos.get(2).exists()).isTrue();
    assertThat(fileInfos.get(2).isGlobalRoot()).isTrue();

    // Fourth one exists, but had to be auto-converted into a directory path.
    assertThat(fileInfos.get(3).exists()).isTrue();
    assertThat(fileInfos.get(3).isDirectory()).isTrue();
    assertThat(fileInfos.get(3).getItemInfo().getResourceId().isStorageObject()).isTrue();
    assertThat(fileInfos.get(3).getItemInfo().getResourceId())
        .isEqualTo(new StorageResourceId(bucketName, "d0/"));

    // Fifth one is a bucket.
    assertThat(fileInfos.get(4).exists()).isTrue();
    assertThat(fileInfos.get(4).isDirectory()).isTrue();
    assertThat(fileInfos.get(4).getItemInfo().getResourceId().isBucket()).isTrue();
    assertThat(fileInfos.get(4).getItemInfo().getResourceId())
        .isEqualTo(new StorageResourceId(bucketName));
  }

  /**
   * Contains data needed for testing the rename() operation.
   */
  private static class RenameData {

    // Description of test case.
    String description;

    // Bucket component of the src path.
    String srcBucketName;

    // Object component of the src path.
    String srcObjectName;

    // Bucket component of the dst path.
    String dstBucketName;

    // Object component of the dst path.
    String dstObjectName;

    // Expected outcome; can return true, return false, or return exception of a certain type.
    MethodOutcome expectedOutcome;

    // Objects expected to exist in src bucket after the operation.
    List<String> objectsExpectedToExistSrc;

    // Objects expected to exist in dst bucket after the operation.
    List<String> objectsExpectedToExistDst;

    // Objects expected to be deleted after the operation.
    List<String> objectsExpectedToBeDeleted;

    /**
     * Constructs an instance of the RenameData class.
     */
    RenameData(String description,
        String srcBucketName, String srcObjectName,
        String dstBucketName, String dstObjectName,
        MethodOutcome expectedOutcome,
        List<String> objectsExpectedToExistSrc,
        List<String> objectsExpectedToExistDst,
        List<String> objectsExpectedToBeDeleted) {

      this.description = description;
      this.srcBucketName = srcBucketName;
      this.srcObjectName = srcObjectName;
      this.dstBucketName = dstBucketName;
      this.dstObjectName = dstObjectName;
      this.expectedOutcome = expectedOutcome;
      this.objectsExpectedToExistSrc = objectsExpectedToExistSrc;
      this.objectsExpectedToExistDst = objectsExpectedToExistDst;
      this.objectsExpectedToBeDeleted = objectsExpectedToBeDeleted;
    }
  }

  /**
   * Validates rename().
   */
  @Test
  public void testRename()
      throws IOException {
    renameHelper(new RenameBehavior() {
      @Override
      public MethodOutcome renameFileIntoRootOutcome() {
        // GCSFS throws IOException on rename into root.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }

      @Override
      public MethodOutcome renameRootOutcome() {
        // GCSFS throws IllegalArgumentException on rename of root.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IllegalArgumentException.class);
      }

      @Override
      public MethodOutcome nonExistentSourceOutcome() {
        // GCSFS throws FileNotFoundException on nonexistent src.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, FileNotFoundException.class);
      }

      @Override
      public MethodOutcome destinationFileExistsSrcIsFileOutcome() {
        // GCSFS throws IOException if dst already exists, is a file, and src is also a file.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }

      @Override
      public MethodOutcome destinationFileExistsSrcIsDirectoryOutcome() {
        // GCSFS throws IOException if dst already exists, is a file, and src is a directory.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
      }

      @Override
      public MethodOutcome nonExistentDestinationFileParentOutcome() {
        // GCSFS throws FileNotFoundException if a parent of file dst doesn't exist.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, FileNotFoundException.class);
      }

      @Override
      public MethodOutcome nonExistentDestinationDirectoryParentOutcome() {
        // GCSFS throws FileNotFoundException if a parent of directory dst doesn't exist.
        return new MethodOutcome(
            MethodOutcome.Type.THROWS_EXCEPTION, FileNotFoundException.class);
      }
    });
  }

  /**
   * Validates rename().
   */
  protected void renameHelper(RenameBehavior behavior)
      throws IOException {
    String bucketName = sharedBucketName1;
    String otherBucketName = sharedBucketName2;

    String uniqueDir = "dir-" + UUID.randomUUID() + GoogleCloudStorage.PATH_DELIMITER;
    String uniqueFile = uniqueDir + "f1";

    // Objects created for this test.
    String[] objectNames = {
      "f1",
      "f2",
      "d0/",
      "d0-a/",
      "d0-b/",
      "d1/f1",
      "d1/d0/",
      "d1/d11/f1",
      "d1-a/f1",
      "d1-b/f1",
      "d1-c/f1",
      "d1-d/f1",
      "d1-e/f1",
      "d1-f/f1",
      "d1-g/f1",
      "d1-h/f1",
      "d1-i/f1",
      uniqueFile,
      "td0-a/",
      "td0-b/",
      "n1-src/d1/f1",
      "n1-dst/",
      "n2-src/d0/",
      "n2-src/f1",
      "n2-src/d1/f1",
      "n2-src/d2/d21/d211/f1",
      "n2-dst/",
      "n2-dst/f1"
    };

    // Objects created in other bucket for this test.
    String[] otherObjectNames = {
      "td0/"
    };

    // -------------------------------------------------------
    // Create test objects.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.clearBucket(otherBucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, objectNames);
    gcsiHelper.createObjectsWithSubdirs(otherBucketName, otherObjectNames);

    // -------------------------------------------------------
    // Initialize test data.
    List<RenameData> renameData = new ArrayList<>();
    String doesNotExist = "does-not-exist";
    String dirDoesNotExist = "does-not-exist";

    // TODO(user) : add test case for dst under src (not allowed)

    // src == root.
    renameData.add(new RenameData(
        "src == root",
        null, null,
        otherBucketName, doesNotExist,
        behavior.renameRootOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted

    // src does not exist.
    renameData.add(new RenameData(
        "src does not exist: 1",
        bucketName, doesNotExist,
        otherBucketName, doesNotExist,
        behavior.nonExistentSourceOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted
    renameData.add(new RenameData(
        "src does not exist: 2",
        bucketName, dirDoesNotExist,
        otherBucketName, dirDoesNotExist,
        behavior.nonExistentSourceOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted
    renameData.add(new RenameData(
        "src does not exist: 3",
        doesNotExist, doesNotExist,
        otherBucketName, doesNotExist,
        behavior.nonExistentSourceOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted

    // dst is a file that already exists.
    if (behavior.destinationFileExistsSrcIsFileOutcome().getType()
        == MethodOutcome.Type.RETURNS_TRUE) {
      renameData.add(new RenameData(
          "dst is a file that already exists: 1",
          bucketName, "f1",
          bucketName, "f2",
          behavior.destinationFileExistsSrcIsFileOutcome(),  // expected outcome
          null,  // expected to exist in src
          Lists.newArrayList("f2"),  // expected to exist in dst
          Lists.newArrayList("f1")));  // expected to be deleted
    } else {
      renameData.add(new RenameData(
          "dst is a file that already exists: 1",
          bucketName, "f1",
          bucketName, "f2",
          behavior.destinationFileExistsSrcIsFileOutcome(),  // expected outcome
          Lists.newArrayList("f1"),  // expected to exist in src
          Lists.newArrayList("f2"),  // expected to exist in dst
          null));  // expected to be deleted
    }

    renameData.add(new RenameData(
        "dst is a file that already exists: 2",
        bucketName, "d0/",
        bucketName, "f2",
        behavior.destinationFileExistsSrcIsDirectoryOutcome(),  // expected outcome
        Lists.newArrayList("d0/"),  // expected to exist in src
        Lists.newArrayList("f2"),  // expected to exist in dst
        null));  // expected to be deleted

    // Parent of destination does not exist.
    renameData.add(new RenameData(
        "Parent of destination does not exist: 1",
        bucketName, "f1",
        bucketName, "does-not-exist/f1",
        behavior.nonExistentDestinationFileParentOutcome(),  // expected outcome
        null,  // expected to exist in src
        null,  // expected to exist in dst
        null));  // expected to be deleted

    if (behavior.nonExistentDestinationDirectoryParentOutcome().getType()
        == MethodOutcome.Type.RETURNS_TRUE) {
      renameData.add(new RenameData(
          "Parent of destination does not exist: 2",
          bucketName, "d0-b/",
          bucketName, "does-not-exist2/d0-b/",
          behavior.nonExistentDestinationDirectoryParentOutcome(),  // expected outcome
          null,  // expected to exist in src
          Lists.newArrayList("does-not-exist2/d0-b/"),  // expected to exist in dst
          Lists.newArrayList("d0-b/")));  // expected to be deleted
    } else {
      renameData.add(new RenameData(
          "Parent of destination does not exist: 2",
          bucketName, "d0-b/",
          bucketName, "does-not-exist2/d0-b/",
          behavior.nonExistentDestinationDirectoryParentOutcome(),  // expected outcome
          Lists.newArrayList("d0-b/"),  // expected to exist in src
          null,  // expected to exist in dst
          null));  // expected to be deleted
    }


    // This test case fails for LocalFileSystem; it clobbers the destination instead.
    // TODO(user): Make the MethodOutcome able to encompass high-level behaviors.
    renameData.add(new RenameData(
        "destination is a dir that exists and non-empty: 2",
        bucketName, "d1-h/",
        bucketName, "td0-a",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        Lists.newArrayList("td0-a/", "td0-a/d1-h/", "td0-a/d1-h/f1"),  // expected to exist in src
        null,  // expected to exist in dst
        Lists.newArrayList("d1-h/", "d1-h/f1")));  // expected to be deleted

    // Rename a dir: destination is a dir that does not exist
    renameData.add(new RenameData(
        "destination is a dir that does not exist",
        bucketName, "d1-b/",
        bucketName, "td0-x/",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        Lists.newArrayList("td0-x/", "td0-x/f1"),  // expected to exist in src
        null,  // expected to exist in dst
        Lists.newArrayList("d1-b/", "d1-b/f1")));  // expected to be deleted

    // Rename a dir: destination is a file that does not exist
    renameData.add(new RenameData(
        "destination is a file that does not exist",
        bucketName, "d1-c/",
        bucketName, "td0-a/df",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        Lists.newArrayList("td0-a/", "td0-a/df/", "td0-a/df/f1"),  // expected to exist in src
        null,  // expected to exist in dst
        Lists.newArrayList("d1-c/", "d1-c/f1")));  // expected to be deleted

    // Rename a file: destination is a file that does not exist
    renameData.add(new RenameData(
        "destination is a file that does not exist",
        bucketName, "d1-d/f1",
        bucketName, "td0-a/f1-x",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),  // expected outcome
        Lists.newArrayList("d1-d/", "td0-a/", "td0-a/f1-x"),  // expected to exist in src
        null,  // expected to exist in dst
        Lists.newArrayList("d1-d/f1")));  // expected to be deleted

    // Rename a file: destination is root.
    if (behavior.renameFileIntoRootOutcome().getType() == MethodOutcome.Type.RETURNS_TRUE) {
      // TODO(user): Refactor the way assertPathsExist so that it can check for existence in
      // root as well.
      renameData.add(new RenameData(
          "file : destination is root",
          bucketName, "d1-i/f1",
          null, null,
          behavior.renameFileIntoRootOutcome(),  // expected outcome
          Lists.newArrayList("d1-i/"),  // expected to exist in src
          null,  // expected to exist in dst
          Lists.newArrayList("d1-i/f1")));  // expected to be deleted
    } else {
      renameData.add(new RenameData(
          "file : destination is root",
          bucketName, "d1-i/f1",
          null, null,
          behavior.renameFileIntoRootOutcome(),  // expected outcome
          Lists.newArrayList("d1-i/", "d1-i/f1"),  // expected to exist in src
          null,  // expected to exist in dst
          null));  // expected to be deleted
    }


    // Rename a file: src is a directory with a multi-level sub-directory.
    renameData.add(new RenameData(
        "src is a directory with a multi-level subdirectory; dst is a directory which exists.",
        bucketName, "n1-src/",
        bucketName, "n1-dst/",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),
        Lists.newArrayList("n1-dst/", "n1-dst/n1-src/d1/", "n1-dst/n1-src/d1/f1"),
        null,
        Lists.newArrayList("n1-src/", "n1-src/d1/", "n1-src/d1/f1")));

    // Rename a file: src is a directory with a multi-level sub-directory.
    // Similar to the previous case but with more levels involved.
    renameData.add(new RenameData(
        "src is a directory with a multi-level subdirectory; dst is a directory which exists - 2",
        bucketName, "n2-src/",
        bucketName, "n2-dst/",
        new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE),
        Lists.newArrayList("n2-dst/", "n2-dst/f1",
            "n2-dst/n2-src/d0/", "n2-dst/n2-src/f1",
            "n2-dst/n2-src/d1/f1", "n2-dst/n2-src/d2/d21/d211/f1"),
        null,
        Lists.newArrayList("n2-src/", "n2-src/d0/", "n2-src/f1",
            "n2-src/d1/f1", "n2-src/d2/d21/d211/f1")));

    // -------------------------------------------------------
    // Call rename() for each path and verify the expected behavior.
    final ExecutorService threadPool = Executors.newCachedThreadPool();

    try {
      // First do a run-through to check existence of starting files.
      final List<Throwable> errorList = new ArrayList<>();
      final CountDownLatch checkStartCounter = new CountDownLatch(renameData.size());
      for (final RenameData rd : renameData) {
        @SuppressWarnings("unused") // go/futurereturn-lsc
        Future<?> possiblyIgnoredError =
            threadPool.submit(
                new Runnable() {
                  @Override
                  public void run() {
                    try {
                      // Verify that items that we expect to rename are present before the operation
                      assertPathsExist(
                          rd.description, rd.srcBucketName, rd.objectsExpectedToBeDeleted, true);
                    } catch (Throwable t) {
                      synchronized (errorList) {
                        errorList.add(t);
                      }
                    } finally {
                      checkStartCounter.countDown();
                    }
                  }
                });
      }
      try {
        checkStartCounter.await();
      } catch (InterruptedException ie) {
        throw new IOException("Interrupted while awaiting counter!", ie);
      }
      if (!errorList.isEmpty()) {
        AssertionError error = new AssertionError();
        for (Throwable t : errorList) {
          error.addSuppressed(t);
        }
        throw error;
      }

      // Do a loop to do all the renames.
      final CountDownLatch renameCounter = new CountDownLatch(renameData.size());
      for (final RenameData rd : renameData) {
        @SuppressWarnings("unused") // go/futurereturn-lsc
        Future<?> possiblyIgnoredError =
            threadPool.submit(
                new Runnable() {
                  @Override
                  public void run() {
                    try {
                      URI src = gcsiHelper.getPath(rd.srcBucketName, rd.srcObjectName);
                      URI dst = gcsiHelper.getPath(rd.dstBucketName, rd.dstObjectName);
                      boolean result = false;

                      String desc = src.toString() + " -> " + dst.toString();
                      try {
                        // Perform the rename operation.
                        result = gcsiHelper.rename(src, dst);

                        if (result) {
                          assertWithMessage(
                                  "Unexpected result for '%s': %s :: expected %s,"
                                      + " actually returned true.",
                                  desc, rd.description, rd.expectedOutcome)
                              .that(rd.expectedOutcome.getType())
                              .isEqualTo(MethodOutcome.Type.RETURNS_TRUE);
                        } else {
                          assertWithMessage(
                                  "Unexpected result for '%s': %s :: expected %s,"
                                      + " actually returned false.",
                                  desc, rd.description, rd.expectedOutcome)
                              .that(rd.expectedOutcome.getType())
                              .isEqualTo(MethodOutcome.Type.RETURNS_FALSE);
                        }
                      } catch (Exception e) {
                        assertWithMessage(
                                "Unexpected result for '%s': %s :: expected %s, actually threw %s.",
                                desc,
                                rd.description,
                                rd.expectedOutcome,
                                Throwables.getStackTraceAsString(e))
                            .that(rd.expectedOutcome.getType())
                            .isEqualTo(MethodOutcome.Type.THROWS_EXCEPTION);
                      }
                    } catch (Throwable t) {
                      synchronized (errorList) {
                        errorList.add(t);
                      }
                    } finally {
                      renameCounter.countDown();
                    }
                  }
                });
      }
      try {
        renameCounter.await();
      } catch (InterruptedException ie) {
        throw new IOException("Interrupted while awaiting counter!", ie);
      }
      if (!errorList.isEmpty()) {
        AssertionError error = new AssertionError();
        for (Throwable t : errorList) {
          error.addSuppressed(t);
        }
        throw error;
      }

      // Finally, check the existence of final destination files.
      final CountDownLatch checkDestCounter = new CountDownLatch(renameData.size());
      for (final RenameData rd : renameData) {
        @SuppressWarnings("unused") // go/futurereturn-lsc
        Future<?> possiblyIgnoredError =
            threadPool.submit(
                new Runnable() {
                  @Override
                  public void run() {
                    try {
                      URI src = gcsiHelper.getPath(rd.srcBucketName, rd.srcObjectName);

                      // Verify that items that we expect to exist are present.
                      assertPathsExist(
                          rd.description, rd.srcBucketName, rd.objectsExpectedToExistSrc, true);
                      String dstBucketName;
                      if ((rd.dstBucketName == null) && (rd.dstObjectName == null)) {
                        // If both bucket and object names are null that means the destination
                        // of the rename is root path. In that case, the leaf directory
                        // of the source path becomes the destination bucket.
                        String srcDirName = gcsiHelper.getItemName(src);
                        dstBucketName = srcDirName;
                      } else {
                        dstBucketName = rd.dstBucketName;
                      }
                      assertPathsExist(
                          rd.description, dstBucketName, rd.objectsExpectedToExistDst, true);

                      // Verify that items that we expect to be deleted are not present.
                      assertPathsExist(
                          rd.description, rd.srcBucketName, rd.objectsExpectedToBeDeleted, false);
                    } catch (Throwable t) {
                      synchronized (errorList) {
                        errorList.add(t);
                      }
                    } finally {
                      checkDestCounter.countDown();
                    }
                  }
                });
      }
      try {
        checkDestCounter.await();
      } catch (InterruptedException ie) {
        throw new IOException("Interrupted while awaiting counter!", ie);
      }
      if (!errorList.isEmpty()) {
        AssertionError error = new AssertionError();
        for (Throwable t : errorList) {
          error.addSuppressed(t);
        }
        throw error;
      }
    } finally {
      threadPool.shutdown();
      try {
        if (!threadPool.awaitTermination(10L, TimeUnit.SECONDS)) {
          System.err.println("Failed to awaitTermination! Forcing executor shutdown.");
          threadPool.shutdownNow();
        }
      } catch (InterruptedException ie) {
        throw new IOException("Interrupted while shutting down threadpool!", ie);
      }
    }
  }

  @Test
  public void testRenameWithContentChecking()
      throws IOException {
    String bucketName = sharedBucketName1;
    // TODO(user): Split out separate test cases, extract a suitable variant of RenameData to
    // follow same pattern of iterating over subcases.
    String[] fileNames = {
        "test-recursive/oldA/B/file2",
        "test-recursive/oldA/file1",
        "test-flat/oldA/aaa",
        "test-flat/oldA/b"
    };

    // Create the objects; their contents will be their own object names as an ASCII string.
    gcsiHelper.clearBucket(bucketName);
    gcsiHelper.createObjectsWithSubdirs(bucketName, fileNames);

    // Check original file existence.
    String testDescRecursive = "Rename of directory with file1 and subdirectory with file2";
    List<String> originalObjects = ImmutableList.of(
        "test-recursive/",
        "test-recursive/oldA/",
        "test-recursive/oldA/B/",
        "test-recursive/oldA/B/file2",
        "test-recursive/oldA/file1",
        "test-flat/oldA/aaa",
        "test-flat/oldA/b");
    assertPathsExist(testDescRecursive, bucketName, originalObjects, true);

    // Check original file content.
    for (String originalName : fileNames) {
      assertThat(gcsiHelper.readTextFile(bucketName, originalName)).isEqualTo(originalName);
    }

    // Do rename oldA -> newA in test-recursive.
    {
      URI src = gcsiHelper.getPath(bucketName, "test-recursive/oldA");
      URI dst = gcsiHelper.getPath(bucketName, "test-recursive/newA");
      assertThat(gcsiHelper.rename(src, dst)).isTrue();
    }

    // Do rename oldA -> newA in test-flat.
    {
      URI src = gcsiHelper.getPath(bucketName, "test-flat/oldA");
      URI dst = gcsiHelper.getPath(bucketName, "test-flat/newA");
      assertThat(gcsiHelper.rename(src, dst)).isTrue();
    }

    // Check resulting file existence.
    List<String> resultingObjects = ImmutableList.of(
        "test-recursive/",
        "test-recursive/newA/",
        "test-recursive/newA/B/",
        "test-recursive/newA/B/file2",
        "test-recursive/newA/file1",
        "test-flat/newA/aaa",
        "test-flat/newA/b");
    assertPathsExist(testDescRecursive, bucketName, resultingObjects, true);

    // Check resulting file content.
    for (String originalName : fileNames) {
      String resultingName = originalName.replaceFirst("oldA", "newA");
      assertThat(gcsiHelper.readTextFile(bucketName, resultingName)).isEqualTo(originalName);
    }

    // Things which mustn't exist anymore.
    List<String> deletedObjects = ImmutableList.of(
        "test-recursive/oldA/",
        "test-recursive/oldA/B/",
        "test-recursive/oldA/B/file2",
        "test-recursive/oldA/file1",
        "test-flat/oldA/aaa",
        "test-flat/oldA/b");
    assertPathsExist(testDescRecursive, bucketName, deletedObjects, false);
  }

  @Test
  public void testFileCreationSetsAttributes() throws IOException {
    CreateFileOptions createFileOptions =
        new CreateFileOptions(
            false /* overwrite existing */,
            ImmutableMap.of("key1", "value1".getBytes(StandardCharsets.UTF_8)));

    URI testFilePath = gcsiHelper.getPath(sharedBucketName1, "test-file-creation-attributes.txt");
    try (WritableByteChannel channel =
        gcsfs.create(testFilePath, createFileOptions)) {
      assertThat(channel).isNotNull();
    }

    FileInfo info = gcsfs.getFileInfo(testFilePath);

    assertThat(info.getAttributes()).hasSize(1);
    assertThat(info.getAttributes()).containsKey("key1");
    assertThat(info.getAttributes().get("key1"))
        .isEqualTo("value1".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testFileCreationUpdatesParentDirectoryModificationTimestamp()
      throws IOException, InterruptedException {
    URI directory =
        gcsiHelper.getPath(sharedBucketName1, "test-modification-timestamps/create-dir/");

    gcsfs.mkdirs(directory);

    FileInfo directoryInfo = gcsfs.getFileInfo(directory);

    assertThat(directoryInfo.isDirectory()).isTrue();
    assertThat(directoryInfo.exists()).isTrue();
    Thread.sleep(100);

    URI childFile = directory.resolve("file.txt");

    try (WritableByteChannel channel = gcsfs.create(childFile)) {
      assertThat(channel).isNotNull();
    }

    FileInfo newDirectoryInfo = gcsfs.getFileInfo(directory);

    assertWithMessage("Modification times should not be equal")
        .that(newDirectoryInfo.getModificationTime())
        .isNotEqualTo(directoryInfo.getModificationTime());

    // This is prone to flake. Creation time is set by GCS while modification time is set
    // client side. We'll only assert that A) creation time is different from modification time and
    // B) that they are within 10 minutes of each other.
    long timeDelta = directoryInfo.getCreationTime() - newDirectoryInfo.getModificationTime();
    assertThat(Math.abs(timeDelta)).isLessThan(TimeUnit.MINUTES.toMillis(10));
  }

  @Test
  public void testPredicateIsConsultedForModificationTimestamps()
      throws IOException, InterruptedException {
    URI directory =
        gcsiHelper.getPath(sharedBucketName1, "test-modification-predicates/mkdirs-dir/");
    URI directoryToUpdate = directory.resolve("subdirectory-1/");
    URI directoryToIncludeAlways = directory.resolve(INCLUDED_TIMESTAMP_SUBSTRING);
    URI directoryToExcludeAlways = directory.resolve(EXCLUDED_TIMESTAMP_SUBSTRING);

    gcsfs.mkdirs(directoryToUpdate);
    gcsfs.mkdirs(directoryToIncludeAlways);
    gcsfs.mkdirs(directoryToExcludeAlways);

    FileInfo directoryInfo = gcsfs.getFileInfo(directory);
    FileInfo directoryToUpdateInfo = gcsfs.getFileInfo(directoryToUpdate);
    FileInfo includeAlwaysInfo = gcsfs.getFileInfo(directoryToIncludeAlways);
    FileInfo excludeAlwaysInfo = gcsfs.getFileInfo(directoryToExcludeAlways);

    assertThat(directoryInfo.isDirectory() && directoryToUpdateInfo.isDirectory()).isTrue();
    assertThat(directoryToUpdateInfo.isDirectory() && directoryToUpdateInfo.exists()).isTrue();
    assertThat(includeAlwaysInfo.isDirectory() && includeAlwaysInfo.exists()).isTrue();
    assertThat(excludeAlwaysInfo.isDirectory() && excludeAlwaysInfo.exists()).isTrue();

    Thread.sleep(100);

    for (URI parentDirectory : new URI[]{directoryToExcludeAlways, directoryToIncludeAlways}) {
      URI sourceFile = parentDirectory.resolve("child-file");
      try (WritableByteChannel channel = gcsfs.create(sourceFile)) {
        assertThat(channel).isNotNull();
      }
    }

    FileInfo updatedIncludeAlways = gcsfs.getFileInfo(directoryToIncludeAlways);
    FileInfo updatedExcludeAlwaysInfo = gcsfs.getFileInfo(directoryToExcludeAlways);

    long updatedTimeDelta =
        includeAlwaysInfo.getCreationTime() - updatedIncludeAlways.getModificationTime();
    assertThat(Math.abs(updatedTimeDelta)).isLessThan(TimeUnit.MINUTES.toMillis(10));

    // Despite having a new file, modification time should not be updated.
    assertThat(excludeAlwaysInfo.getModificationTime())
        .isEqualTo(updatedExcludeAlwaysInfo.getModificationTime());
  }

  @Test
  public void testMkdirsUpdatesParentDirectoryModificationTimestamp()
      throws IOException, InterruptedException {
    URI directory =
        gcsiHelper.getPath(sharedBucketName1, "test-modification-timestamps/mkdirs-dir/");
    URI directoryToUpdate = directory.resolve("subdirectory-1/");

    gcsfs.mkdirs(directoryToUpdate);

    FileInfo directoryInfo = gcsfs.getFileInfo(directory);
    FileInfo directoryToUpdateInfo = gcsfs.getFileInfo(directoryToUpdate);

    assertThat(directoryInfo.isDirectory() && directoryToUpdateInfo.isDirectory()).isTrue();
    assertThat(directoryToUpdateInfo.isDirectory() && directoryToUpdateInfo.exists()).isTrue();

    Thread.sleep(100);

    URI childDirectory = directoryToUpdate.resolve("subdirectory-2/subdirectory-3/");

    gcsfs.mkdirs(childDirectory);

    FileInfo newDirectoryToUpdateInfo = gcsfs.getFileInfo(directoryToUpdate);

    assertWithMessage("Modification times should not be equal")
        .that(newDirectoryToUpdateInfo.getModificationTime())
        .isNotEqualTo(directoryToUpdateInfo.getModificationTime());

    // This is prone to flake. Creation time is set by GCS while modification time is set
    // client side. We'll only assert that A) creation time is different from modification time and
    // B) that they are within 10 minutes of each other.
    long timeDelta =
        directoryToUpdateInfo.getCreationTime() - newDirectoryToUpdateInfo.getModificationTime();
    assertThat(Math.abs(timeDelta)).isLessThan(TimeUnit.MINUTES.toMillis(10));

    // The root (/test-modification-timestamps/mkdirs-dir/) should *not* have had its timestamp
    // updated, only subdirectory-1 should have:
    FileInfo nonUpdatedDirectoryInfo = gcsfs.getFileInfo(directory);
    assertThat(nonUpdatedDirectoryInfo.getModificationTime())
        .isEqualTo(directoryInfo.getModificationTime());
  }

  @Test
  public void testDeleteUpdatesDirectoryModificationTimestamps()
      throws IOException, InterruptedException {
    URI directory =
        gcsiHelper.getPath(sharedBucketName1, "test-modification-timestamps/delete-dir/");

    gcsfs.mkdirs(directory);

    URI sourceFile = directory.resolve("child-file");
    // Create a test object in our source directory:
    try (WritableByteChannel channel = gcsfs.create(sourceFile)) {
      assertThat(channel).isNotNull();
    }

    FileInfo directoryInfo = gcsfs.getFileInfo(directory);
    FileInfo sourceFileInfo = gcsfs.getFileInfo(sourceFile);

    assertThat(directoryInfo.isDirectory()).isTrue();
    assertThat(directoryInfo.exists() && sourceFileInfo.exists()).isTrue();

    Thread.sleep(100);

    gcsfs.delete(sourceFile, false);

    FileInfo updatedDirectoryInfo = gcsfs.getFileInfo(directory);
    assertWithMessage("Modification times should not be equal")
        .that(updatedDirectoryInfo.getModificationTime())
        .isNotEqualTo(directoryInfo.getModificationTime());

    // This is prone to flake. Creation time is set by GCS while modification time is set
    // client side. We'll only assert that A) creation time is different from modification time and
    // B) that they are within 10 minutes of eachother.
    long timeDelta =
        directoryInfo.getCreationTime() - updatedDirectoryInfo.getModificationTime();
    assertThat(Math.abs(timeDelta)).isLessThan(TimeUnit.MINUTES.toMillis(10));
  }

  @Test
  public void testRenameUpdatesParentDirectoryModificationTimestamps()
      throws IOException, InterruptedException {
    URI directory =
        gcsiHelper.getPath(sharedBucketName1, "test-modification-timestamps/rename-dir/");
    URI sourceDirectory = directory.resolve("src-directory/");
    URI destinationDirectory = directory.resolve("destination-directory/");
    gcsfs.mkdirs(sourceDirectory);
    gcsfs.mkdirs(destinationDirectory);

    URI sourceFile = sourceDirectory.resolve("child-file");
    // Create a test object in our source directory:
    try (WritableByteChannel channel = gcsfs.create(sourceFile)) {
      assertThat(channel).isNotNull();
    }

    FileInfo directoryInfo = gcsfs.getFileInfo(directory);
    FileInfo sourceDirectoryInfo = gcsfs.getFileInfo(sourceDirectory);
    FileInfo destinationDirectoryInfo = gcsfs.getFileInfo(destinationDirectory);
    FileInfo sourceFileInfo = gcsfs.getFileInfo(sourceFile);

    assertThat(
            directoryInfo.isDirectory()
                && destinationDirectoryInfo.isDirectory()
                && sourceDirectoryInfo.isDirectory())
        .isTrue();
    assertThat(
            directoryInfo.exists()
                && destinationDirectoryInfo.exists()
                && sourceDirectoryInfo.exists()
                && sourceFileInfo.exists())
        .isTrue();

    Thread.sleep(100);

    gcsfs.rename(sourceFile, destinationDirectory);

    // The root (/test-modification-timestamps/rename-dir/) directory's time stamp shouldn't change:
    FileInfo updatedDirectoryInfo = gcsfs.getFileInfo(directory);
    assertWithMessage("Modification time should NOT have changed")
        .that(updatedDirectoryInfo.getModificationTime())
        .isEqualTo(directoryInfo.getModificationTime());

    // Timestamps for both source and destination *should* change:
    FileInfo updatedSourceDirectoryInfo = gcsfs.getFileInfo(sourceDirectory);
    FileInfo updatedDestinationDirectoryInfo = gcsfs.getFileInfo(destinationDirectory);
    // This is prone to flake. Creation time is set by GCS while modification time is set
    // client side. We'll only assert that A) creation time is different from modification time and
    // B) that they are within 10 minutes of eachother.
    long sourceTimeDelta =
        sourceDirectoryInfo.getCreationTime() - updatedSourceDirectoryInfo.getModificationTime();
    assertThat(Math.abs(sourceTimeDelta)).isLessThan(TimeUnit.MINUTES.toMillis(10));

    // This is prone to flake. Creation time is set by GCS while modification time is set
    // client side. We'll only assert that A) creation time is different from modification time and
    // B) that they are within 10 minutes of eachother.
    long destinationTimeDelta =
        destinationDirectoryInfo.getCreationTime()
            - updatedDestinationDirectoryInfo.getModificationTime();
    assertThat(Math.abs(destinationTimeDelta)).isLessThan(TimeUnit.MINUTES.toMillis(10));
  }

  @Test
  public void testComposeSuccess() throws IOException {
    String bucketName = sharedBucketName1;
    URI directory = gcsiHelper.getPath(bucketName, "test-compose/");
    URI object1 = directory.resolve("object1");
    URI object2 = directory.resolve("object2");
    URI destination = directory.resolve("destination");
    gcsfs.mkdirs(directory);

    // Create the source objects
    try (WritableByteChannel channel1 = gcsfs.create(object1)) {
      assertThat(channel1).isNotNull();
      channel1.write(ByteBuffer.wrap("content1".getBytes(UTF_8)));
    }
    try (WritableByteChannel channel2 = gcsfs.create(object2)) {
      assertThat(channel2).isNotNull();
      channel2.write(ByteBuffer.wrap("content2".getBytes(UTF_8)));
    }
    assertThat(gcsfs.exists(object1) && gcsfs.exists(object2)).isTrue();

    gcsfs.compose(
        ImmutableList.of(object1, object2), destination, CreateFileOptions.DEFAULT_CONTENT_TYPE);

    byte[] expectedOutput = "content1content2".getBytes(UTF_8);
    ByteBuffer actualOutput = ByteBuffer.allocate(expectedOutput.length);
    try (SeekableByteChannel destinationChannel =
            gcsiHelper.open(bucketName, "test-compose/destination")) {
      destinationChannel.read(actualOutput);
    }
    assertThat(actualOutput.array()).isEqualTo(expectedOutput);
  }

  /**
   * Gets a unique path of a non-existent file.
   */
  public static URI getTempFilePath() {
    return gcsiHelper.getPath(sharedBucketName1, "file-" + UUID.randomUUID());
  }

  /**
   * Returns intermediate sub-paths for the given path.
   * <p>
   * For example,
   * getSubDirPaths(gs://foo/bar/zoo) returns: (gs://foo/, gs://foo/bar/)
   *
   * @param path Path to get sub-paths of.
   * @return List of sub-directory paths.
   */
  private List<URI> getSubDirPaths(URI path) {
    StorageResourceId resourceId = gcsiHelper.validatePathAndGetId(path, true);

    List<String> subdirs = GoogleCloudStorageFileSystem.getSubDirs(resourceId.getObjectName());
    List<URI> subDirPaths = new ArrayList<>(subdirs.size());
    for (String subdir : subdirs) {
      subDirPaths.add(gcsiHelper.getPath(resourceId.getBucketName(), subdir));
    }

    return subDirPaths;
  }

  /**
   * If the given paths are expected to exist then asserts that they do,
   * otherwise asserts that they do not exist.
   */
  private void assertPathsExist(
      String testCaseDescription, String bucketName,
      List<String> objectNames, boolean expectedToExist)
      throws IOException {
    if (objectNames != null) {
      for (String object : objectNames) {
        URI path = gcsiHelper.getPath(bucketName, object);
        String msg = String.format("test-case: %s :: %s: %s",
            testCaseDescription,
            (expectedToExist
                ? "Path expected to exist but not found"
                : "Path expected to not exist but found"),
            path.toString());
        assertWithMessage(msg).that(gcsiHelper.exists(path)).isEqualTo(expectedToExist);
      }
    }
  }
}
