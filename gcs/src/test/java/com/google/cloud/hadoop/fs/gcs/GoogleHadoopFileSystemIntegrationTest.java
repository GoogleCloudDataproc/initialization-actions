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
import com.google.cloud.hadoop.gcsio.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.testing.CredentialConfigurationUtil;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for GoogleHadoopFileSystem class.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemIntegrationTest
    extends GoogleHadoopFileSystemTestBase {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void beforeAllTests()
      throws IOException {
    GoogleHadoopFileSystem testInstance = new GoogleHadoopFileSystem();
    ghfs = testInstance;
    ghfsFileSystemDescriptor = testInstance;
    URI initUri;
    try {
      initUri = new URI("gs:/");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    ghfs.initialize(initUri, loadConfig());

    HadoopFileSystemTestBase.postCreateInit();
  }

  @Before
  public void clearFileSystemCache() throws IOException {
    FileSystem.closeAll();
  }

  @AfterClass
  public static void afterAllTests()
      throws IOException {
    GoogleHadoopFileSystemTestBase.afterAllTests();
  }

  // -----------------------------------------------------------------
  // Tests that exercise behavior defined in HdfsBehavior.
  // -----------------------------------------------------------------

  /**
   * Validates rename().
   */
  @Test @Override
  public void testRename()
      throws IOException {
    renameHelper(new HdfsBehavior() {
        /**
         * Returns the MethodOutcome of trying to rename an existing file into the root directory.
         */
        @Override
        public MethodOutcome renameFileIntoRootOutcome() {
          return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
        }
      });
  }

  /**
   * Validates success path in checkPath().
   */
  @Test @Override
  public void testCheckPathSuccess() {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    String rootBucket = myGhfs.getRootBucketName();
    List<String> validPaths = new ArrayList<>();
    validPaths.add("/");
    validPaths.add("/foo");
    validPaths.add("/foo/bar");
    validPaths.add("gs:/");
    validPaths.add("gs:/foo");
    validPaths.add("gs:/foo/bar");
    validPaths.add("gs://");
    validPaths.add("gs://" + rootBucket);
    validPaths.add("gs://" + rootBucket + "/bar");
    for (String validPath : validPaths) {
      Path path = new Path(validPath);
      myGhfs.checkPath(path);
    }
  }

  /**
   * Validates failure path in checkPath().
   */
  @Test @Override
  public void testCheckPathFailure() {
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;
    List<String> invalidSchemePaths = new ArrayList<>();
    String rootBucket = myGhfs.getRootBucketName();
    invalidSchemePaths.add("gsg:/");
    invalidSchemePaths.add("hdfs:/");
    invalidSchemePaths.add("gsg:/foo/bar");
    invalidSchemePaths.add("hdfs:/foo/bar");
    invalidSchemePaths.add("gsg://");
    invalidSchemePaths.add("hdfs://");
    invalidSchemePaths.add("gsg://" + rootBucket);
    invalidSchemePaths.add("gsg://" + rootBucket + "/bar");
    for (String invalidPath : invalidSchemePaths) {
      Path path = new Path(invalidPath);
      try {
        myGhfs.checkPath(path);
        String msg = String.format(
            "checkPath should have thrown IllegalArgumentException on path: %s", invalidPath);
        Assert.fail(msg);
      } catch (IllegalArgumentException e) {
        // Expected
        Assert.assertTrue(e.getLocalizedMessage().startsWith("Wrong FS scheme:"));
      }
    }
    
    List<String> invalidBucketPaths = new ArrayList<>();
    String notRootBucket = "not-" + rootBucket;
    invalidBucketPaths.add("gs://" + notRootBucket);
    invalidBucketPaths.add("gs://" + notRootBucket + "/bar");
    for (String invalidPath : invalidBucketPaths) {
      Path path = new Path(invalidPath);
      try {
        myGhfs.checkPath(path);
        String msg = String.format(
            "checkPath should have thrown IllegalArgumentException on path: %s", invalidPath);
        Assert.fail(msg);
      } catch (IllegalArgumentException e) {
        // Expected
        Assert.assertTrue(e.getLocalizedMessage().startsWith("Wrong bucket:"));
      }
    }
  }
  /**
   * Verify that default constructor does not throw.
   */
  @Test
  public void testDefaultConstructor() {
    new GoogleHadoopFileSystem();
  }

  /**
   * Verify that getHomeDirectory() returns expected value.
   */
  @Test
  public void testGetHomeDirectory() {
    URI homeDir = ghfs.getHomeDirectory().toUri();
    String scheme = homeDir.getScheme();
    String bucket = homeDir.getAuthority();
    String path = homeDir.getPath();
    Assert.assertEquals("Unexpected home directory scheme: " + scheme, "gs", scheme);
    Assert.assertEquals(
        "Unexpected home directory bucket: " + bucket,
        ((GoogleHadoopFileSystem) ghfs).getRootBucketName(), bucket);
    Assert.assertTrue("Unexpected home directory path: " + path, path.startsWith("/user/"));
  }

  /**
   * Test getHadoopPath() invalid args.
   */
  @Test
  public void testGetHadoopPathInvalidArgs()
      throws URISyntaxException {
    try {
      ((GoogleHadoopFileSystem) ghfs).getHadoopPath(new URI("gs://foobucket/bar"));
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().startsWith("Authority of URI"));
    }
  }

  /**
   * Validates success path in initialize().
   */
  @Test @Override
  public void testInitializeSuccess()
      throws IOException, URISyntaxException {
    GoogleHadoopFileSystem fs = null;

    // Reuse loadConfig() to initialize auth related settings.
    Configuration config = loadConfig();

    // Set up remaining settings to known test values.
    int bufferSize = 512;
    config.setInt(GoogleHadoopFileSystemBase.BUFFERSIZE_KEY, bufferSize);
    long blockSize = 1024;
    config.setLong(GoogleHadoopFileSystemBase.BLOCK_SIZE_KEY, blockSize);
    String systemBucketName =
        ghfsHelper.getUniqueBucketName("-system-bucket");
    String rootBucketName =
        ghfsHelper.getUniqueBucketName("-root-bucket");
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, systemBucketName);

    URI initUri = (new Path("gs://" + rootBucketName)).toUri();
    try {
      fs = new GoogleHadoopFileSystem();
      fs.initialize(initUri, config);
    } catch (IOException e) {
      Assert.fail("Unexpected exception");
    }

    // Verify that config settings were set correctly.
    Assert.assertEquals(bufferSize, fs.getBufferSizeOverride());
    Assert.assertEquals(blockSize, fs.getDefaultBlockSize());
    Assert.assertEquals(systemBucketName, fs.getSystemBucketName());
    Assert.assertEquals(initUri, fs.initUri);
    Assert.assertEquals(rootBucketName, fs.getRootBucketName());

    initUri = (new Path("gs:/foo")).toUri();
    try {
      fs = new GoogleHadoopFileSystem();
      fs.initialize(initUri, config);
    } catch (IOException e) {
      Assert.fail("Unexpected exception");
    }

    // Verify that config settings were set correctly.
    Assert.assertEquals(bufferSize, fs.getBufferSizeOverride());
    Assert.assertEquals(blockSize, fs.getDefaultBlockSize());
    Assert.assertEquals(systemBucketName, fs.getSystemBucketName());
    Assert.assertEquals(initUri, fs.initUri);
    Assert.assertEquals(systemBucketName, fs.getRootBucketName());
  }

  @Test
  public void testInitializeThrowsWhenNoProjectIdConfigured()
      throws URISyntaxException, IOException {
    // Verify that incomplete config raises exception.
    String existingBucket = bucketName;

    Configuration config = new Configuration();
    URI gsUri = new URI("gs://foobar/");
    config.setBoolean(GoogleHadoopFileSystemBase.ENABLE_GCE_SERVICE_ACCOUNT_AUTH_KEY, false);
    config.setBoolean(
        HadoopCredentialConfiguration.BASE_KEY_PREFIX
            + HadoopCredentialConfiguration.ENABLE_NULL_CREDENTIAL_SUFFIX,
        true);
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, existingBucket);
    // project ID is not set.

    expectedException.expect(IOException.class);
    expectedException.expectMessage(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY);

    new GoogleHadoopFileSystem().initialize(gsUri, config);
  }

  @Test
  public void testInitializeThrowsWhenWrongSchemeConfigured()
      throws URISyntaxException, IOException {
    // Verify that we cannot initialize using URI with a wrong scheme.
    URI wrongScheme = new URI("http://foo/bar");

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("URI scheme not supported");

    new GoogleHadoopFileSystem().initialize(wrongScheme, new Configuration());
  }

  @Test
  public void testInitializeThrowsWhenCredentialsNotFound()
      throws URISyntaxException, IOException {
    String fakeClientId = "fooclient";
    String existingBucket = bucketName;

    Configuration config = new Configuration();
    URI gsUri = new URI("gs://foobar/");
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

    new GoogleHadoopFileSystem().initialize(gsUri, config);
  }

  /**
   * Validates initialize() with configuration key fs.gs.working.dir set.
   */
  @Test @Override
  public void testInitializeWithWorkingDirectory()
      throws IOException, URISyntaxException {
    // We can just test by calling initialize multiple times (for each test condition) because
    // there is nothing in initialize() which must be run only once. If this changes, this test
    // method will need to resort to using a new GoogleHadoopFileSystem() for each item
    // in the for-loop.
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;

    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, bucketName);
    ghfs.initialize(myGhfs.initUri, config);

    // setUpWorkingDirectoryTest() depends on getFileSystemRoot(), which in turn depends on
    // having initialized with the desired systemBucket. If we tried to call this before
    // ghfs.initialize on the preceding line, the test may or may not succeed depending on
    // whether the last test case happened to set systemBucket to bucketName already.
    List<WorkingDirData> wddList = setUpWorkingDirectoryTest();
    String rootBucketName = myGhfs.getRootBucketName();
    for (WorkingDirData wdd : wddList) {
      Path path = wdd.path;
      Path expectedWorkingDir = wdd.expectedPath;
      Path currentWorkingDir = ghfs.getWorkingDirectory();
      config.set(GoogleHadoopFileSystemBase.GCS_WORKING_DIRECTORY_KEY, path.toString());
      ghfs.initialize(myGhfs.initUri, config);
      Path newWorkingDir = ghfs.getWorkingDirectory();
      if (expectedWorkingDir != null) {
        Assert.assertEquals(expectedWorkingDir, newWorkingDir);
      } else {
        Assert.assertEquals(currentWorkingDir, newWorkingDir);
      }
    }
    Assert.assertTrue(ghfs.getHomeDirectory().toString().startsWith("gs://" + rootBucketName));
  }

  /**
   * Validates success path in configureBuckets().
   * @throws URISyntaxException 
   * @throws IOException 
   */
  @Test @Override
  public void testConfigureBucketsSuccess() throws URISyntaxException, IOException {
    GoogleHadoopFileSystem fs = null;
    
    String systemBucketName =
        ghfsHelper.getUniqueBucketName("-system-bucket");
    String rootBucketName =
        ghfsHelper.getUniqueBucketName("-root-bucket");

    URI initUri = (new Path(rootBucketName)).toUri();
    
    // To test configureBuckets which occurs after GCSFS initialization in configure(), while
    // still being reusable by derived unittests (we can't call loadConfig in a test case which
    // is inherited by a derived test), we will use the constructor which already provides a (fake)
    // GCSFS and skip the portions of the config specific to GCSFS.

    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());
    
    try {
      fs = new GoogleHadoopFileSystem(fakeGcsFs);
      fs.initUri = initUri;
      fs.configureBuckets(systemBucketName, true);
    } catch (IOException e) {
      Assert.fail("Unexpected exception");
    }

    // Verify that config settings were set correctly.
    Assert.assertEquals(systemBucketName, fs.getSystemBucketName());
    Assert.assertEquals(initUri, fs.initUri);

    initUri = (new Path("gs:/foo")).toUri();
    try {
      fs = new GoogleHadoopFileSystem(fakeGcsFs);
      fs.initUri = initUri;
      fs.configureBuckets(systemBucketName, true);
    } catch (IOException e) {
      Assert.fail("Unexpected exception");
    }

    // Verify that config settings were set correctly.
    Assert.assertEquals(systemBucketName, fs.getSystemBucketName());
    Assert.assertEquals(initUri, fs.initUri);
    Assert.assertEquals(systemBucketName, fs.getRootBucketName());
  }

  @Test
  public void testConfigureBucketsThrowsWhenBucketNotFound() throws IOException {
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());

    // Non-existent system bucket with GCS_CREATE_SYSTEM_BUCKET_KEY set to false.
    boolean createSystemBuckets = false;
    String systemBucketName = "this-bucket-doesnt-exist";
    expectedException.expect(FileNotFoundException.class);
    expectedException.expectMessage(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY);
    new GoogleHadoopFileSystem(fakeGcsFs).configureBuckets(systemBucketName, createSystemBuckets);
  }

  @Test
  public void testConfigureBucketsThrowsWhenInvalidBucketName() throws IOException {
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());

    boolean createSystemBuckets = true;
    String systemBucketName = "this-bucket-has-illegal-char^";
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid bucket name");
    new GoogleHadoopFileSystem(fakeGcsFs).configureBuckets(systemBucketName, createSystemBuckets);
  }

  @Test
  public void testConfigureBucketsThrowsWhenSubdirSpecified() throws IOException {
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());

    boolean createSystemBuckets = true;
    String systemBucketName = "bucket/with-subdir";
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("must not contain '/'");
    new GoogleHadoopFileSystem(fakeGcsFs).configureBuckets(systemBucketName, createSystemBuckets);
  }


  private Configuration getConfigurationWtihImplementation() throws IOException {
    Configuration conf = loadConfig();
    CredentialConfigurationUtil.addTestConfigurationSettings(conf);
    conf.set("fs.gs.impl", GoogleHadoopFileSystem.class.getCanonicalName());
    return conf;
  }

  @Test
  public void testFileSystemIsRemovedFromCacheOnClose() throws IOException, URISyntaxException {
    Configuration conf = getConfigurationWtihImplementation();

    URI fsUri = new URI(String.format("gs://%s/", bucketName));

    FileSystem fs1 = FileSystem.get(fsUri, conf);
    FileSystem fs2 = FileSystem.get(fsUri, conf);

    Assert.assertSame(fs1, fs2);

    fs1.close();

    FileSystem fs3 = FileSystem.get(fsUri, conf);
    Assert.assertNotSame(fs1, fs3);

    fs3.close();
  }

  @Test
  public void testIOExceptionIsThrowAfterClose() throws IOException, URISyntaxException {
    Configuration conf = getConfigurationWtihImplementation();

    URI fsUri = new URI(String.format("gs://%s/", bucketName));

    FileSystem fs1 = FileSystem.get(fsUri, conf);
    FileSystem fs2 = FileSystem.get(fsUri, conf);

    junit.framework.Assert.assertSame(fs1, fs2);

    fs1.close();

    expectedException.expect(IOException.class);

    fs2.exists(new Path("/SomePath/That/Doesnt/Matter"));
  }

  public void createFile(Path filePath, byte[] data) throws IOException {
    try (FSDataOutputStream output = ghfs.create(filePath)) {
      output.write(data);
    }
  }

  @Test
  public void testGlobStatus() throws IOException {
    ghfs.mkdirs(new Path("/directory1"));
    ghfs.mkdirs(new Path("/directory1/subdirectory1"));
    ghfs.mkdirs(new Path("/directory1/subdirectory2"));

    byte[] data = new byte[10];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    createFile(new Path("/directory1/subdirectory1/file1"), data);
    createFile(new Path("/directory1/subdirectory1/file2"), data);
    createFile(new Path("/directory1/subdirectory2/file1"), data);
    createFile(new Path("/directory1/subdirectory2/file2"), data);

    FileStatus[] rootDirectories = ghfs.globStatus(new Path("/d*"));
    Assert.assertEquals(1, rootDirectories.length);
    Assert.assertEquals("directory1", rootDirectories[0].getPath().getName());

    FileStatus[] subDirectories = ghfs.globStatus(new Path("/directory1/s*"));
    Assert.assertEquals(2, subDirectories.length);

    FileStatus[] subDirectory1Files = ghfs.globStatus(new Path("/directory1/subdirectory1/*"));
    Assert.assertEquals(2, subDirectory1Files.length);
    Assert.assertEquals("file1", subDirectory1Files[0].getPath().getName());
    Assert.assertEquals("file2", subDirectory1Files[1].getPath().getName());

    FileStatus[] subDirectory2Files = ghfs.globStatus(new Path("/directory1/subdirectory2/f*"));
    Assert.assertEquals(2, subDirectory2Files.length);
    Assert.assertEquals("file1", subDirectory2Files[0].getPath().getName());
    Assert.assertEquals("file2", subDirectory2Files[1].getPath().getName());
  }
}
