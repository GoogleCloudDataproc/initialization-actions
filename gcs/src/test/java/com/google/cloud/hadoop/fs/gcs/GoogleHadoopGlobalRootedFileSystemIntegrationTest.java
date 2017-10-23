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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for GoogleHadoopFileSystemBase class.
 *
 * We reuse test code from GoogleCloudStorageIntegrationHelper and
 * GoogleCloudStorageFileSystemIntegrationTest. In addition, there are
 * some tests that test behavior that is only visible at GHFS level.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopGlobalRootedFileSystemIntegrationTest
    extends GoogleHadoopFileSystemTestBase {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Performs initialization once before tests are run.
   */
  @BeforeClass
  public static void beforeAllTests()
      throws IOException {

    // Disable logging.
    Logger.getRootLogger().setLevel(Level.OFF);

    GoogleHadoopFileSystemBase testInstance = new GoogleHadoopGlobalRootedFileSystem();
    ghfs = testInstance;
    ghfsFileSystemDescriptor = testInstance;
    URI initUri;
    try {
      initUri = new URI("gsg://bucket-should-be-ignored");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }

    // loadConfig needs ghfsHelper, which is normally created in
    // postCreateInit. Create one here for it to use.
    ghfsHelper = new HadoopFileSystemIntegrationHelper(
        ghfs, ghfsFileSystemDescriptor);
    ghfs.initialize(initUri, loadConfig());

    HadoopFileSystemTestBase.postCreateInit();
    ghfsHelper.setIgnoreStatistics(); // Multi-threaded code screws us up.
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
    String serviceAccount = TestConfiguration.getInstance().getServiceAccount();
    String privateKey = TestConfiguration.getInstance().getPrivateKeyFile();
    String projectId = TestConfiguration.getInstance().getProjectId();
    Assert.assertNotNull(serviceAccount);
    Assert.assertNotNull(privateKey);
    Assert.assertNotNull(projectId);
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, projectId);
    config.set(GoogleHadoopFileSystemBase.SERVICE_ACCOUNT_AUTH_EMAIL_KEY, serviceAccount);
    config.set(GoogleHadoopFileSystemBase.SERVICE_ACCOUNT_AUTH_KEYFILE_KEY, privateKey);
    String systemBucketName =
        ghfsHelper.getUniqueBucketName("-system-bucket");
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, systemBucketName);
    config.setBoolean(GoogleHadoopFileSystemBase.GCS_CREATE_SYSTEM_BUCKET_KEY, true);
    config.setBoolean(
        GoogleHadoopFileSystemBase.GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY, true);
    config.setBoolean(
        GoogleHadoopFileSystemBase.GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_KEY, false);
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
        ghfsHelper.getUniqueBucketName("-system-bucket");
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

  @Test
  public void testInitializeThrowsWhenWrongSchemeConfigured()
      throws URISyntaxException, IOException {
    // Verify that we cannot initialize using URI with a wrong scheme.
    URI wrongScheme = new URI("http://foo/bar");

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("URI scheme not supported");

    new GoogleHadoopGlobalRootedFileSystem().initialize(wrongScheme, new Configuration());
  }

  @Test
  public void testInitializeThrowsWhenCredentialsNotFound()
      throws URISyntaxException, IOException {
    String fakeClientId = "fooclient";
    String existingBucket = bucketName;

    Configuration config = new Configuration();
    URI gsUri = new URI("gsg://foobar/");
    String fakeProjectId = "123456";
    config = new Configuration();
    config.setBoolean(
        GoogleHadoopFileSystemBase.ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY, false);
    // Set project ID and client ID but no client secret.
    config.set(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, fakeProjectId);
    config.set(GoogleHadoopFileSystemBase.GCS_CLIENT_ID_KEY, fakeClientId);
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, existingBucket);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("No valid credential configuration discovered");

    new GoogleHadoopGlobalRootedFileSystem().initialize(gsUri, config);
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

    // To test configureBuckets which occurs after GCSFS initialization in configure(), while
    // still being reusable by derived unittests (we can't call loadConfig in a test case which
    // is inherited by a derived test), we will use the constructor which already provides a (fake)
    // GCSFS and skip the portions of the config specific to GCSFS.

    String systemBucketName =
        ghfsHelper.getUniqueBucketName("-system-bucket");

    GoogleCloudStorageOptions.Builder gcsOptionsBuilder =
        GoogleHadoopFileSystemTestHelper.defaultStorageOptionsBuilder();
    GoogleCloudStorageFileSystemOptions.Builder fsOptionsBuilder =
        GoogleCloudStorageFileSystemOptions.newBuilder()
        .setCloudStorageOptionsBuilder(gcsOptionsBuilder);
    GoogleCloudStorageFileSystem fakeGcsFs = new GoogleCloudStorageFileSystem(
        new InMemoryGoogleCloudStorage(gcsOptionsBuilder.build()),
        fsOptionsBuilder.build());

    try {
      fs = new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs);
      fs.configureBuckets(systemBucketName, true);
    } catch (IOException e) {
      Assert.fail("Unexpected exception");
    }

    // Verify that config settings were set correctly.
    Assert.assertEquals(systemBucketName, fs.getSystemBucketName());
  }


  @Test
  public void testConfigureBucketsThrowsWhenBucketNotFound() throws IOException {
    GoogleCloudStorageOptions.Builder gcsOptionsBuilder =
        GoogleHadoopFileSystemTestHelper.defaultStorageOptionsBuilder();
    GoogleCloudStorageFileSystemOptions.Builder fsOptionsBuilder =
        GoogleCloudStorageFileSystemOptions.newBuilder()
        .setCloudStorageOptionsBuilder(gcsOptionsBuilder);
    GoogleCloudStorageFileSystem fakeGcsFs = new GoogleCloudStorageFileSystem(
        new InMemoryGoogleCloudStorage(gcsOptionsBuilder.build()),
        fsOptionsBuilder.build());

    // Non-existent system bucket with GCS_CREATE_SYSTEM_BUCKET_KEY set to false.
    boolean createSystemBuckets = false;
    String systemBucketName = "this-bucket-doesnt-exist";
    expectedException.expect(FileNotFoundException.class);
    expectedException.expectMessage(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY);
    new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs)
        .configureBuckets(systemBucketName, createSystemBuckets);
  }

  @Test
  public void testConfigureBucketsThrowsWhenInvalidBucketName() throws IOException {
    GoogleCloudStorageOptions.Builder gcsOptionsBuilder =
        GoogleHadoopFileSystemTestHelper.defaultStorageOptionsBuilder();
    GoogleCloudStorageFileSystemOptions.Builder fsOptionsBuilder =
        GoogleCloudStorageFileSystemOptions.newBuilder()
        .setCloudStorageOptionsBuilder(gcsOptionsBuilder);
    GoogleCloudStorageFileSystem fakeGcsFs = new GoogleCloudStorageFileSystem(
        new InMemoryGoogleCloudStorage(gcsOptionsBuilder.build()),
        fsOptionsBuilder.build());

    boolean createSystemBuckets = true;
    String systemBucketName = "this-bucket-has-illegal-char^";
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid bucket name");
    new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs)
        .configureBuckets(systemBucketName, createSystemBuckets);
  }

  @Test
  public void testConfigureBucketsThrowsWhenSubdirSpecified() throws IOException {
    GoogleCloudStorageOptions.Builder gcsOptionsBuilder =
        GoogleHadoopFileSystemTestHelper.defaultStorageOptionsBuilder();
    GoogleCloudStorageFileSystemOptions.Builder fsOptionsBuilder =
        GoogleCloudStorageFileSystemOptions.newBuilder()
        .setCloudStorageOptionsBuilder(gcsOptionsBuilder);
    GoogleCloudStorageFileSystem fakeGcsFs = new GoogleCloudStorageFileSystem(
        new InMemoryGoogleCloudStorage(gcsOptionsBuilder.build()),
        fsOptionsBuilder.build());

    boolean createSystemBuckets = true;
    String systemBucketName = "bucket/with-subdir";
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("must not contain '/'");
    new GoogleHadoopGlobalRootedFileSystem(fakeGcsFs)
        .configureBuckets(systemBucketName, createSystemBuckets);
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

  /**
   * Disable test cases which are too expensive to always run in global-rooted mode.
   */
  @Override
  public void testListObjectNamesAndGetItemInfo() {
  }

  @Override
  public void provideCoverageForUnmodifiedMethods() {
  }
}
