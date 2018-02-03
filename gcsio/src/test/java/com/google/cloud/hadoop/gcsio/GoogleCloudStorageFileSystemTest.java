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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * The unittest version of {@code GoogleCloudStorageFileSystemIntegrationTest}; the external
 * GoogleCloudStorage dependency is replaced by an in-memory version which mimics the same
 * bucket/object semantics.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemTest
    extends GoogleCloudStorageFileSystemIntegrationTest {

  @BeforeClass
  public static void beforeAllTests()
      throws IOException {
    // Disable logging.
    Logger.getRootLogger().setLevel(Level.OFF);

    if (gcsfs == null) {
      // TODO(user): Maybe switch to
      // new CacheSupplementedGoogleCloudStorage(new InMemoryGoogleCloudStorage()).
      gcsfs = new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage(),
          GoogleCloudStorageFileSystemOptions
              .newBuilder()
              .setShouldIncludeInTimestampUpdatesPredicate(INCLUDE_SUBSTRINGS_PREDICATE)
              .build());
      gcsfs.setUpdateTimestampsExecutor(MoreExecutors.newDirectExecutorService());
      gcs = gcsfs.getGcs();
      GoogleCloudStorageFileSystemIntegrationTest.postCreateInit();
    }
  }

  @AfterClass
  public static void afterAllTests() throws IOException {
    GoogleCloudStorageFileSystemIntegrationTest.afterAllTests();
  }

  /**
   * Helper to fill out some default valid options after which the caller may want to reset a few
   * invalid options for individual items for particular tests.
   */
  private static void setDefaultValidOptions(
      GoogleCloudStorageFileSystemOptions.Builder optionsBuilder) {
    optionsBuilder
        .setIsMetadataCacheEnabled(true)
        .setCacheMaxEntryAgeMillis(12345L)
        .setCacheMaxInfoAgeMillis(42L)
        .getCloudStorageOptionsBuilder()
            .setAppName("appName")
            .setProjectId("projectId")
            .getWriteChannelOptionsBuilder()
                .setFileSizeLimitedTo250Gb(GCS_FILE_SIZE_LIMIT_250GB_DEFAULT)
                .setUploadBufferSize(WRITE_BUFFERSIZE_DEFAULT);
  }

  /**
   * Validates constructor.
   */
  @Test
  public void testConstructor() throws IOException {
    GoogleCredential cred = new GoogleCredential();
    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
        GoogleCloudStorageFileSystemOptions.newBuilder();

    setDefaultValidOptions(optionsBuilder);

    // Verify that projectId == null or empty does not throw.
    optionsBuilder.getCloudStorageOptionsBuilder().setProjectId(null);
    new GoogleCloudStorageFileSystem(cred, optionsBuilder.build());

    optionsBuilder.getCloudStorageOptionsBuilder().setProjectId("");
    new GoogleCloudStorageFileSystem(cred, optionsBuilder.build());

    optionsBuilder.getCloudStorageOptionsBuilder().setProjectId("projectId");

    // Verify that appName == null or empty throws IllegalArgumentException.

    optionsBuilder.getCloudStorageOptionsBuilder().setAppName(null);
    try {
      new GoogleCloudStorageFileSystem(cred, optionsBuilder.build());
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    optionsBuilder.getCloudStorageOptionsBuilder().setAppName("");
    try {
      new GoogleCloudStorageFileSystem(cred, optionsBuilder.build());
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    optionsBuilder.getCloudStorageOptionsBuilder().setAppName("appName");

    // Verify that credential == null throws IllegalArgumentException.
    try {
      new GoogleCloudStorageFileSystem((Credential) null, optionsBuilder.build());
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    // Verify that fake projectId/appName and empty cred does not throw.
    setDefaultValidOptions(optionsBuilder);

    GoogleCloudStorageFileSystem tmpGcsFs =
        new GoogleCloudStorageFileSystem(cred, optionsBuilder.build());

    // White-box testing; check a few internal outcomes of our options.
    Assert.assertTrue(tmpGcsFs.getGcs() instanceof CacheSupplementedGoogleCloudStorage);
    CacheSupplementedGoogleCloudStorage cacheGcs =
        (CacheSupplementedGoogleCloudStorage) tmpGcsFs.getGcs();
    Assert.assertEquals(
        12345L, cacheGcs.getResourceCache().getMutableConfig().getMaxEntryAgeMillis());
    Assert.assertEquals(
        42L, cacheGcs.getResourceCache().getMutableConfig().getMaxInfoAgeMillis());
  }

  /**
   * Verify that pathComparator produces correct sorting order.
   */
  @Test
  public void testPathComparator()
      throws URISyntaxException {
    String[] paths = {
      "gs://aa",
      "gs://abcdefghij",
      "gs://aaa",
      "gs:/",
      "gs://aa/f",
      "gs://aaa/f",
      "gs://aa/bb/f",
      "gs://ab",
      "gs://aa/bb/",
      "gs://aa",
    };

    String[] expectedAfterSort = {
      "gs:/",
      "gs://aa",
      "gs://aa",
      "gs://ab",
      "gs://aaa",
      "gs://aa/f",
      "gs://aaa/f",
      "gs://aa/bb/",
      "gs://aa/bb/f",
      "gs://abcdefghij",
    };

    // Prepare URI lists from their string equivalents.
    List<URI> pathUris = new ArrayList<>();
    List<URI> expectedUris = new ArrayList<>();

    for (String path : paths) {
      pathUris.add(new URI(path));
    }

    for (String path : expectedAfterSort) {
      expectedUris.add(new URI(path));
    }

    // Sanity check for input data using "natural-ordering" sorting.
    List<URI> pathUrisNaturalSorted = new ArrayList<>(pathUris);
    Collections.sort(pathUrisNaturalSorted);
    List<URI> expectedUrisNaturalSorted = new ArrayList<>(expectedUris);
    Collections.sort(expectedUrisNaturalSorted);
    Assert.assertArrayEquals(expectedUrisNaturalSorted.toArray(), pathUrisNaturalSorted.toArray());

    // Sort the paths with the GCSFS-supplied pathComparator and verify.
    Collections.sort(pathUris, GoogleCloudStorageFileSystem.pathComparator);
    Assert.assertArrayEquals(expectedUris.toArray(), pathUris.toArray());
  }

  /** Verify that we cannot pass invalid path to GoogleCloudStorageFileSystem. */
  @Test
  public void testInvalidPath() throws IOException, URISyntaxException {
    String[] invalidPaths = {

      // Path with a scheme other than gs.
      "foo://bucket/object",

      // Path with empty object name.
      "gs://bucket/",
      "gs://bucket",

      // Path with consecutive / chars in the path component.
      "gs://bucket//obj",
      "gs://bucket/obj//foo/bar",
    };

    for (String invalidPath : invalidPaths) {
      try {
        gcsfs.getPathCodec().validatePathAndGetId(new URI(invalidPath), false);
        Assert.fail(String.format("Expected path to be invalid: %s", invalidPath));
      } catch (IllegalArgumentException expected) {
        // Expected.
      }
    }

    String[] validPaths = {
      "gs:/",
      "gs://bucket/obj",
      "gs://bucket/obj/",
      "gs://bucket/obj/bar",
    };

    for (String validPath : validPaths) {
      gcsfs.getPathCodec().validatePathAndGetId(new URI(validPath), false);
    }

    try {
      // To verify the behavior when the GCSFS is the one creating an invalid URI, we must call
      // through directly to GCSFS.getPath instead of GCSFS.validatePathAndGetId like above;
      // there's no way to create the invalid URI beforehand to pass through to GCSFS.
      String invalidBucketName = "bucket-name-has-invalid-char^";
      gcsfs.getPathCodec().getPath(invalidBucketName, null, true);
      Assert.fail(String.format("Expected getPath to be invalid: gs://%s", invalidBucketName));
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  /**
   * Verify getItemName().
   */
  @Test
  public void testGetItemName()
      throws URISyntaxException {
    // Trailing slashes are effectively stripped for returned bucket names, but not for object
    // names.
    String[] inputPaths = {
      "gs:/",
      "gs://my-bucket",
      "gs://my-bucket/",
      "gs://my-bucket/foo",
      "gs://my-bucket/foo/",
      "gs://my-bucket/foo/bar",
      "gs://my-bucket/foo/bar/",
    };

    String[] expectedNames = {
      null,
      "my-bucket",
      "my-bucket",
      "foo",
      "foo/",
      "bar",
      "bar/",
    };

    List<String> actualNames = new ArrayList<>();
    for (String inputPath : inputPaths) {
      actualNames.add(gcsfs.getItemName(new URI(inputPath)));
    }
    Assert.assertArrayEquals(expectedNames, actualNames.toArray(new String[0]));
  }

  /**
   * Verify getParentPath().
   */
  @Test
  public void testGetParentPathEdgeCases()
      throws URISyntaxException {
    URI[] inputPaths = {
      new URI("gs:/"),
      new URI("gs://my-bucket"),
      new URI("gs://my-bucket/"),
      new URI("gs://my-bucket/foo"),
      new URI("gs://my-bucket/foo/"),
      new URI("gs://my-bucket/foo/bar"),
      new URI("gs://my-bucket/foo/bar/"),
    };

    URI[] expectedPaths = {
      null,
      new URI("gs:/"),
      new URI("gs:/"),
      new URI("gs://my-bucket/"),
      new URI("gs://my-bucket/"),
      new URI("gs://my-bucket/foo/"),
      new URI("gs://my-bucket/foo/"),
    };

    List<URI> actualPaths = new ArrayList<>();
    for (URI inputPath : inputPaths) {
      actualPaths.add(gcsfs.getParentPath(inputPath));
    }
    Assert.assertArrayEquals(expectedPaths, actualPaths.toArray(new URI[0]));
  }

  /** Verify validateBucketName(). */
  @Test
  public void testValidateBucketName() {
    String[] invalidBucketNames = {

      // Empty or null.
      null,
      "",

      // With a '/' character in it.
      "foo/bar",
      "/bar",
    };

    for (String bucketName : invalidBucketNames) {
      try {
        GoogleCloudStorageFileSystem.validateBucketName(bucketName);
        Assert.fail(String.format("Expected bucket name to be invalid: %s", bucketName));
      } catch (IllegalArgumentException expected) {
        // Expected.
      }
    }

    String[] validBucketNames = {
      "foo",
      "foo/",
    };

    for (String bucketName : validBucketNames) {
      GoogleCloudStorageFileSystem.validateBucketName(bucketName);
    }
  }

  /** Verify validateObjectName(). */
  @Test
  public void testValidateObjectName() {
    String[] invalidObjectNames = {

      // Empty or null.
      null,
      "",

      // With consecutive '/' characters in it.
      "//",
      "///",
      "foo//bar",
      "foo/bar//",
      "//foo/bar",
      "foo////bar",

      // other cases
      "/",
    };

    for (String objectName : invalidObjectNames) {
      try {
        GoogleCloudStorageFileSystem.validateObjectName(objectName, false);
        Assert.fail(String.format("Expected object name to be invalid: %s", objectName));
      } catch (IllegalArgumentException expected) {
        // Expected.
      }
    }

    // Verify that an empty object name is allowed when explicitly allowed.
    GoogleCloudStorageFileSystem.validateObjectName(null, true);
    GoogleCloudStorageFileSystem.validateObjectName("", true);

    String[] validObjectNames = {
      "foo",
      "foo/bar",
      "foo/bar/",
    };

    for (String objectName : validObjectNames) {
      GoogleCloudStorageFileSystem.validateObjectName(objectName, false);
    }
  }

  /**
   * Verify misc cases for FileInfo.
   */
  @Test
  public void testFileInfo()
      throws IOException {
    Assert.assertEquals(
        GoogleCloudStorageFileSystem.GCS_ROOT,
        gcsfs.getFileInfo(GoogleCloudStorageFileSystem.GCS_ROOT).getPath());
    Assert.assertEquals(
        GoogleCloudStorageItemInfo.ROOT_INFO,
        gcsfs.getFileInfo(GoogleCloudStorageFileSystem.GCS_ROOT).getItemInfo());
  }

  /**
   * Verify misc cases for create/open.
   */
  @Test
  public void testMiscCreateAndOpen()
      throws URISyntaxException, IOException {
    URI dirPath = new URI("gs://foo/bar/");
    try {
      gcsfs.create(dirPath);
      Assert.fail(String.format("Writing to directory should not be allowed: %s", dirPath));
    } catch (IOException expected) {
      // Expected.
    }

    try {
      gcsfs.open(dirPath);
      Assert.fail(String.format("Reading from directory should not be allowed: %s", dirPath));
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  @Test
  public void testCreateNoParentDirectories()
      throws URISyntaxException, IOException {
    String bucketName = sharedBucketName1;
    gcsfs.create(
        new URI("gs://" + bucketName + "/no/parent/dirs/exist/a.txt"),
        new CreateFileOptions(
            false,  // overwriteExisting
            CreateFileOptions.DEFAULT_CONTENT_TYPE,
            CreateFileOptions.EMPTY_ATTRIBUTES,
            true,  // checkNoDirectoryConflict
            false,  // ensureParentDirectoriesExist
            StorageResourceId.UNKNOWN_GENERATION_ID))
      .close();
    Assert.assertTrue(
        gcsfs.getGcs().getItemInfo(new StorageResourceId(bucketName, "no/parent/dirs/exist/a.txt"))
            .exists());
    Assert.assertFalse(
        gcsfs.getGcs().getItemInfo(new StorageResourceId(bucketName, "no/parent/dirs/exist/"))
            .exists());
    Assert.assertFalse(
        gcsfs.getGcs().getItemInfo(new StorageResourceId(bucketName, "no/parent/dirs/"))
            .exists());
  }

  @Test
  public void testCreateAllowConflictWithExistingDirectory()
      throws URISyntaxException, IOException {
    String bucketName = sharedBucketName1;
    gcsfs.mkdirs(new URI("gs://" + bucketName + "/conflicting-dirname"));
    gcsfs.create(
        new URI("gs://" + bucketName + "/conflicting-dirname"),
        new CreateFileOptions(
            false,  // overwriteExisting
            CreateFileOptions.DEFAULT_CONTENT_TYPE,
            CreateFileOptions.EMPTY_ATTRIBUTES,
            false,  // checkNoDirectoryConflict
            true,  // ensureParentDirectoriesExist
            StorageResourceId.UNKNOWN_GENERATION_ID))
        .close();

    // This is a "shoot yourself in the foot" use case, but working as intended if
    // checkNoDirectoryConflict is disabled; object and directory have same basename.
    Assert.assertTrue(
        gcsfs.getGcs().getItemInfo(new StorageResourceId(bucketName, "conflicting-dirname"))
            .exists());
    Assert.assertTrue(
        gcsfs.getGcs().getItemInfo(new StorageResourceId(bucketName, "conflicting-dirname/"))
            .exists());
  }
}
