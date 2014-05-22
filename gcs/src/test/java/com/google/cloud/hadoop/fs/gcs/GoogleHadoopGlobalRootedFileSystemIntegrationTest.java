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

import com.google.cloud.hadoop.fs.gcs.HadoopFileSystemTestBase.WorkingDirData;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageIntegrationTest;
import com.google.cloud.hadoop.gcsio.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.testing.ExceptionUtil;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for GoogleHadoopFileSystemBase class.
 *
 * We reuse test code from GoogleCloudStorageIntegrationTest and
 * GoogleCloudStorageFileSystemIntegrationTest. In addition, there are
 * some tests that test behavior that is only visible at GHFS level.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopGlobalRootedFileSystemIntegrationTest
    extends GoogleHadoopFileSystemTestBase {
  /**
   * Performs initialization once before tests are run.
   */
  @BeforeClass
  public static void beforeAllTests()
      throws IOException {

    // Disable logging.
    Logger.getRootLogger().setLevel(Level.OFF);

    gcsit = new GoogleHadoopGlobalRootedFileSystemIntegrationTest();

    GoogleHadoopFileSystemBase testInstance = new GoogleHadoopGlobalRootedFileSystem();
    ghfs = testInstance;
    ghfsFileSystemDescriptor = testInstance;
    statistics = FileSystemStatistics.EXACT;
    URI initUri;
    try {
      initUri = new URI("gsg://bucket-should-be-ignored");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    ghfs.initialize(initUri, loadConfig());

    HadoopFileSystemTestBase.postCreateInit();
  }

  /**
   * Helper to load all the GHFS-specific config values from environment variables, such as those
   * needed for setting up the credentials of a real GoogleCloudStorage.
   */
  protected static Configuration loadConfig()
      throws IOException {
    // Supply client-id, client-secret and project-id to GHFS
    // through a Configuration object instance.
    // TODO(user) : add helper to get multiple env vars in one
    // call and produce a friendlier message if value(s) are missing.
    String clientId =
        System.getenv(GoogleCloudStorageIntegrationTest.GCS_TEST_CLIENT_ID);
    String clientSecret =
        System.getenv(GoogleCloudStorageIntegrationTest.GCS_TEST_CLIENT_SECRET);
    String projectId =
        System.getenv(GoogleCloudStorageIntegrationTest.GCS_TEST_PROJECT_ID);
    Assert.assertNotNull(clientId);
    Assert.assertNotNull(clientSecret);
    Assert.assertNotNull(projectId);
    Configuration config = new Configuration();
    config.setBoolean(
        GoogleHadoopFileSystemBase.ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY, false);
    config.set(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, projectId);
    config.set(GoogleHadoopFileSystemBase.GCS_CLIENT_ID_KEY, clientId);
    config.set(GoogleHadoopFileSystemBase.GCS_CLIENT_SECRET_KEY, clientSecret);
    String systemBucketName =
        GoogleCloudStorageIntegrationTest.getUniqueBucketName("-system-bucket");
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, systemBucketName);
    config.setBoolean(GoogleHadoopFileSystemBase.GCS_CREATE_SYSTEM_BUCKET_KEY, true);
    config.setBoolean(
        GoogleHadoopFileSystemBase.GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY, true);
    return config;
  }

  /**
   * Perform clean-up once after all tests are turn.
   */
  @AfterClass
  public static void afterAllTests()
      throws IOException {
    GoogleHadoopFileSystemTestBase.afterAllTests();
  }

  /**
   * Validates success path in checkPath().
   */
  @Test @Override
  public void testCheckPathSuccess()  {
    GoogleHadoopFileSystemBase myGhfs = (GoogleHadoopFileSystemBase) ghfs;
    List<String> validPaths = new ArrayList<>();
    validPaths.add("/");
    validPaths.add("/foo");
    validPaths.add("/foo/bar");
    validPaths.add("gsg:/");
    validPaths.add("gsg:/foo");
    validPaths.add("gsg:/foo/bar");
    validPaths.add("gsg://");
    validPaths.add("gsg://foo");
    validPaths.add("gsg://foo/bar");
    for (String validPath : validPaths) {
      Path path = new Path(validPath);
      myGhfs.checkPath(path);
    }
  }

  /**
   * Validates failure path in checkPath().
   */
  @Test @Override
  public void testCheckPathFailure()  {
    GoogleHadoopFileSystemBase myGhfs = (GoogleHadoopFileSystemBase) ghfs;
    List<String> invalidPaths = new ArrayList<>();
    invalidPaths.add("gs:/");
    invalidPaths.add("hdfs:/");
    invalidPaths.add("gs:/foo/bar");
    invalidPaths.add("hdfs:/foo/bar");
    invalidPaths.add("gs://");
    invalidPaths.add("hdfs://");
    invalidPaths.add("gs://foo/bar");
    invalidPaths.add("hdfs://foo/bar");
    for (String invalidPath : invalidPaths) {
      Path path = new Path(invalidPath);
      try {
        myGhfs.checkPath(path);
        String msg = String.format(
            "checkPath should have thrown IllegalArgumentException on path: %s", invalidPaths);
        Assert.fail(msg);
      } catch (IllegalArgumentException e) {
        // Expected
        Assert.assertTrue(e.getLocalizedMessage().startsWith("Wrong FS scheme:"));
      }
    }
  }

  /**
   * Validates success path in initialize().
   */
  @Test @Override
  public void testInitializeSuccess()
      throws IOException, URISyntaxException {
    GoogleHadoopFileSystemBase fs = null;

    // Reuse loadConfig() to initialize auth related settings.
    Configuration config = loadConfig();

    // Set up remaining settings to known test values.
    int bufferSize = 512;
    config.setInt(GoogleHadoopFileSystemBase.BUFFERSIZE_KEY, bufferSize);
    long blockSize = 1024;
    config.setLong(GoogleHadoopFileSystemBase.BLOCK_SIZE_KEY, blockSize);
    String systemBucketName =
        GoogleCloudStorageIntegrationTest.getUniqueBucketName("-system-bucket");
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, systemBucketName);

    URI initUri = (new Path("gsg://bucket-should-be-ignored")).toUri();
    try {
      fs = new GoogleHadoopGlobalRootedFileSystem();
      fs.initialize(initUri, config);
    } catch (IOException e) {
      Assert.fail("Unexpected exception");
    }

    // Verify that config settings were set correctly.
    Assert.assertEquals(bufferSize, fs.getBufferSizeOverride());
    Assert.assertEquals(blockSize, fs.getDefaultBlockSize());
    Assert.assertEquals(systemBucketName, fs.getSystemBucketName());
    Assert.assertEquals(initUri, fs.initUri);
  }

  /**
   * Validates failure paths in initialize().
   */
  @Test @Override
  public void testInitializeFailure()
      throws IOException, URISyntaxException {
    String existingBucket = bucketName;

    // Verify that we cannot initialize using URI with a wrong scheme.
    URI wrongScheme = new URI("http://foo/bar");
    ExceptionUtil.checkThrowsWithMessage(IllegalArgumentException.class,
        "URI scheme not supported",
        new GoogleHadoopGlobalRootedFileSystem(), "initialize", wrongScheme, new Configuration());

    // Verify that we can invoke GCE service account auth (though it will not succeed).
    Configuration config = new Configuration();
    URI gsUri = new URI("gsg://foobar/");
    String fakeProjectId = "123456";
    config.setBoolean(GoogleHadoopFileSystemBase.ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY, true);
    config.set(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, fakeProjectId);
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, existingBucket);
    ExceptionUtil.checkThrowsWithMessage(IOException.class,
        "Error getting access token from metadata server",
        new GoogleHadoopGlobalRootedFileSystem(), "initialize", gsUri, config);

    // Verify that incomplete config raises exception.
    config = new Configuration();
    config.setBoolean(GoogleHadoopFileSystemBase.ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY, false);
    config.setBoolean(
        HadoopCredentialConfiguration.BASE_KEY_PREFIX +
            HadoopCredentialConfiguration.ENABLE_NULL_CREDENTIAL_SUFFIX,
        true);
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, existingBucket);
    // project ID is not set.
    ExceptionUtil.checkThrowsWithMessage(IOException.class,
        GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY,
        new GoogleHadoopGlobalRootedFileSystem(), "initialize", gsUri, config);

    // Verify that running on non-GCE with ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY == true
    // raises exception.
    config = new Configuration();
    config.setBoolean(GoogleHadoopFileSystemBase.ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY, true);
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, existingBucket);
    config.set(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, fakeProjectId);
    ExceptionUtil.checkThrowsWithMessage(IOException.class,
        "Error getting access token from metadata server",
        new GoogleHadoopGlobalRootedFileSystem(), "initialize", gsUri, config);

    String fakeClientId = "fooclient";
    config = new Configuration();
    config.setBoolean(
        GoogleHadoopFileSystemBase.ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY, false);
    // Set project ID and client ID but no client secret.
    config.set(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, fakeProjectId);
    config.set(GoogleHadoopFileSystemBase.GCS_CLIENT_ID_KEY, fakeClientId);
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, existingBucket);
    ExceptionUtil.checkThrowsWithMessage(IllegalStateException.class,
        "No valid credential configuration discovered.",
        new GoogleHadoopGlobalRootedFileSystem(), "initialize", gsUri, config);

    // To test the parts of initialize which occur after GCSFS initialization in configure(), while
    // still being reusable by derived unittests (we can't call loadConfig in a test case which
    // is inherited by a derived test), we will use the constructor which already provides a (fake)
    // GCSFS and skip the portions of the config specific to GCSFS.
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());

    // Non-existent system bucket with GCS_CREATE_SYSTEM_BUCKET_KEY set to false.
    config = new Configuration();
    config.setBoolean(GoogleHadoopFileSystemBase.GCS_CREATE_SYSTEM_BUCKET_KEY, false);
    config.set(
        GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, "this-bucket-doesnt-exist");
    ExceptionUtil.checkThrowsWithMessage(FileNotFoundException.class,
        GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY,
        new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs), "initialize", gsUri, config);

    // System bucket which causes invalid URI.
    config = new Configuration();
    config.setBoolean(GoogleHadoopFileSystemBase.GCS_CREATE_SYSTEM_BUCKET_KEY, true);
    config.set(
        GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, "this-bucket-has-illegal-char^");
    ExceptionUtil.checkThrowsWithMessage(IllegalArgumentException.class, "Invalid bucket name",
        new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs), "initialize", gsUri, config);

    // System bucket which looks like a path (contains '/').
    config = new Configuration();
    config.setBoolean(GoogleHadoopFileSystemBase.GCS_CREATE_SYSTEM_BUCKET_KEY, true);
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, "bucket/with-subdir");
    ExceptionUtil.checkThrowsWithMessage(IllegalArgumentException.class, "must not contain '/'",
        new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs), "initialize", gsUri, config);
  }

  /**
   * Validates initialize() with configuration key fs.gs.working.dir set.
   */
  @Test @Override
  public void testInitializeWithWorkingDirectory()
      throws IOException, URISyntaxException {
    // We can just test by calling initialize multiple times (for each test condition) because
    // there is nothing in initialize() which must be run only once. If this changes, this test
    // method will need to resort to using a new GoogleHadoopGlobalRootedFileSystem() for each item
    // in the for-loop.
    List<WorkingDirData> wddList = setUpWorkingDirectoryTest();
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, bucketName);
    URI gsUri = new URI("gsg://foobar/");
    for (WorkingDirData wdd : wddList) {
      Path path = wdd.path;
      Path expectedWorkingDir = wdd.expectedPath;
      Path currentWorkingDir = ghfs.getWorkingDirectory();
      config.set(GoogleHadoopFileSystemBase.GCS_WORKING_DIRECTORY_KEY, path.toString());
      ghfs.initialize(gsUri, config);
      Path newWorkingDir = ghfs.getWorkingDirectory();
      if (expectedWorkingDir != null) {
        Assert.assertEquals(expectedWorkingDir, newWorkingDir);
      } else {
        Assert.assertEquals(currentWorkingDir, newWorkingDir);
      }
    }
    Assert.assertTrue(ghfs.getHomeDirectory().toString().startsWith("gsg:/" + bucketName));
  }

  /**
   * Validates success path in configureBuckets().
   */
  @Test @Override
  public void testConfigureBucketsSuccess() throws URISyntaxException, IOException {
    GoogleHadoopFileSystemBase fs = null;

    URI gsUri = new URI("gsg://foobar/");

    // To test configureBuckets which occurs after GCSFS initialization in configure(), while
    // still being reusable by derived unittests (we can't call loadConfig in a test case which
    // is inherited by a derived test), we will use the constructor which already provides a (fake)
    // GCSFS and skip the portions of the config specific to GCSFS.

    String systemBucketName =
        GoogleCloudStorageIntegrationTest.getUniqueBucketName("-system-bucket");

    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());

    try {
      fs = new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs);
      fs.configureBuckets(systemBucketName, true);
    } catch (IOException e) {
      Assert.fail("Unexpected exception");
    }

    // Verify that config settings were set correctly.
    Assert.assertEquals(systemBucketName, fs.getSystemBucketName());
  }

  /**
   * Validates failure paths in configureBuckets().
   * @throws IOException
   * @throws URISyntaxException
   */
  @Test @Override
  public void testConfigureBucketsFailure()
      throws IOException, URISyntaxException {
    // To test configureBuckets which occurs after GCSFS initialization in configure(), while
    // still being reusable by derived unittests (we can't call loadConfig in a test case which
    // is inherited by a derived test), we will use the constructor which already provides a (fake)
    // GCSFS and skip the portions of the config specific to GCSFS.
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());

    // Non-existent system bucket with GCS_CREATE_SYSTEM_BUCKET_KEY set to false.
    boolean createSystemBuckets = false;
    String systemBucketName = "this-bucket-doesnt-exist";
    ExceptionUtil.checkThrowsWithMessage(FileNotFoundException.class,
        GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY,
        new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs),
        "configureBuckets", systemBucketName, createSystemBuckets);

    // System bucket which causes invalid URI.
    createSystemBuckets = true;
    systemBucketName = "this-bucket-has-illegal-char^";
    ExceptionUtil.checkThrowsWithMessage(IllegalArgumentException.class, "Invalid bucket name",
        new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs),
        "configureBuckets", systemBucketName, createSystemBuckets);

    // System bucket which looks like a path (contains '/').
    createSystemBuckets = true;
    systemBucketName = "bucket/with-subdir";
    ExceptionUtil.checkThrowsWithMessage(IllegalArgumentException.class, "must not contain '/'",
        new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs),
        "configureBuckets", systemBucketName, createSystemBuckets);
  }

  /**
   * Tests failure mode of getHadoopPath().
   */
  @Test
  public void testGetHadoopPathFailure() {
    try {
      ((GoogleHadoopGlobalRootedFileSystem) ghfs).getHadoopPathFromResourceId(
          new StorageResourceId("buck^et", "object"));
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }
}
