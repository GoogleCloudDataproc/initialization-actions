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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.GcsFileChecksumType;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for GoogleHadoopFileSystem class. */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemIntegrationTest extends GoogleHadoopFileSystemTestBase {

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(GoogleHadoopFileSystemIntegrationTest.class) {
        @Override
        public void before() throws Throwable {
          GoogleHadoopFileSystem testInstance = new GoogleHadoopFileSystem();
          ghfs = testInstance;
          ghfsFileSystemDescriptor = testInstance;

          // loadConfig needs ghfsHelper, which is normally created in
          // postCreateInit. Create one here for it to use.
          ghfsHelper = new HadoopFileSystemIntegrationHelper(ghfs, ghfsFileSystemDescriptor);

          URI initUri = new URI("gs://" + ghfsHelper.getUniqueBucketName("init"));
          ghfs.initialize(initUri, loadConfig());

          if (GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE.get(
              ghfs.getConf(), ghfs.getConf()::getBoolean)) {
            testInstance.getGcsFs();
          }

          HadoopFileSystemTestBase.postCreateInit();
        }

        @Override
        public void after() {
          GoogleHadoopFileSystemTestBase.storageResource.after();
        }
      };

  @Before
  public void clearFileSystemCache() throws IOException {
    FileSystem.closeAll();
  }

  // -----------------------------------------------------------------
  // Tests that exercise behavior defined in HdfsBehavior.
  // -----------------------------------------------------------------

  /** Validates rename(). */
  @Test
  @Override
  public void testRename() throws IOException {
    renameHelper(
        new HdfsBehavior() {
          /**
           * Returns the MethodOutcome of trying to rename an existing file into the root directory.
           */
          @Override
          public MethodOutcome renameFileIntoRootOutcome() {
            return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
          }
        });
  }

  @Test
  public void testInitializePath_success() throws Exception {
    List<String> validPaths = Arrays.asList("gs://foo", "gs://foo/bar");
    for (String path : validPaths) {
      try (GoogleHadoopFileSystem testGhfs = createInMemoryGoogleHadoopFileSystem()) {
        testGhfs.initialize(new URI(path), new Configuration());
      }
    }
  }

  @Test
  public void testInitializePath_failure_notSupportedSchema() throws Exception {
    List<String> invalidPaths =
        Arrays.asList("http://foo", "gsg://foo", "hdfs:/", "hdfs:/foo", "hdfs://foo");
    for (String path : invalidPaths) {
      URI uri = new URI(path);
      try (GoogleHadoopFileSystem testGhfs = createInMemoryGoogleHadoopFileSystem()) {
        IllegalArgumentException e =
            assertThrows(
                "Path '" + path + "' should be invalid",
                IllegalArgumentException.class,
                () -> testGhfs.initialize(uri, new Configuration()));
        assertThat(e).hasMessageThat().startsWith("URI scheme not supported:");
      }
    }
  }

  @Test
  public void testInitializePath_failure_bucketNotSpecified() throws Exception {
    List<String> invalidPaths = Arrays.asList("gs:/", "gs:/foo", "gs:/foo/bar", "gs:///");
    for (String path : invalidPaths) {
      URI uri = new URI(path);
      try (GoogleHadoopFileSystem testGhfs = createInMemoryGoogleHadoopFileSystem()) {
        IllegalArgumentException e =
            assertThrows(
                "Path '" + path + "' should be invalid",
                IllegalArgumentException.class,
                () -> testGhfs.initialize(uri, new Configuration()));
        assertThat(e).hasMessageThat().startsWith("No bucket specified in GCS URI:");
      }
    }
  }

  @Test
  @Override
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

  /** Validates failure path in checkPath(). */
  @Test
  @Override
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
      IllegalArgumentException e =
          assertThrows(
              "Path '" + path + "' should be invalid",
              IllegalArgumentException.class,
              () -> myGhfs.checkPath(path));
      assertThat(e.getLocalizedMessage()).startsWith("Wrong FS scheme:");
    }

    List<String> invalidBucketPaths = new ArrayList<>();
    String notRootBucket = "not-" + rootBucket;
    invalidBucketPaths.add("gs://" + notRootBucket);
    invalidBucketPaths.add("gs://" + notRootBucket + "/bar");
    for (String invalidPath : invalidBucketPaths) {
      Path path = new Path(invalidPath);
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> myGhfs.checkPath(path));
      assertThat(e.getLocalizedMessage()).startsWith("Wrong bucket:");
    }
  }
  /** Verify that default constructor does not throw. */
  @Test
  public void testDefaultConstructor() {
    new GoogleHadoopFileSystem();
  }

  /** Verify that getHomeDirectory() returns expected value. */
  @Test
  public void testGetHomeDirectory() {
    URI homeDir = ghfs.getHomeDirectory().toUri();
    String scheme = homeDir.getScheme();
    String bucket = homeDir.getAuthority();
    String path = homeDir.getPath();
    assertWithMessage("Unexpected home directory scheme: " + scheme).that(scheme).isEqualTo("gs");
    assertWithMessage("Unexpected home directory bucket: " + bucket)
        .that(bucket)
        .isEqualTo(((GoogleHadoopFileSystem) ghfs).getRootBucketName());
    assertWithMessage("Unexpected home directory path: " + path)
        .that(path.startsWith("/user/"))
        .isTrue();
  }

  /** Test getHadoopPath() invalid args. */
  @Test
  public void testGetHadoopPathInvalidArgs() throws URISyntaxException {
    IllegalArgumentException expected =
        assertThrows(
            IllegalArgumentException.class,
            () -> ((GoogleHadoopFileSystem) ghfs).getHadoopPath(new URI("gs://foobucket/bar")));
    assertThat(expected).hasMessageThat().startsWith("Authority of URI");
  }

  /** Validates that we correctly build our Options object from a Hadoop config. */
  @Test
  public void testBuildOptionsFromConfig() throws IOException {
    Configuration config = loadConfig("projectId", "serviceAccount", "priveKeyFile");

    GoogleCloudStorageFileSystemOptions.Builder optionsBuilder =
        GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);
    GoogleCloudStorageFileSystemOptions options = optionsBuilder.build();
    GoogleCloudStorageOptions gcsOptions = options.getCloudStorageOptions();

    assertThat(gcsOptions.isAutoRepairImplicitDirectoriesEnabled()).isTrue();
    assertThat(gcsOptions.isInferImplicitDirectoriesEnabled()).isFalse();

    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey(), false);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE.getKey(), true);

    optionsBuilder = GoogleHadoopFileSystemConfiguration.getGcsFsOptionsBuilder(config);
    options = optionsBuilder.build();

    gcsOptions = options.getCloudStorageOptions();
    assertThat(gcsOptions.isAutoRepairImplicitDirectoriesEnabled()).isFalse();
    assertThat(gcsOptions.isInferImplicitDirectoriesEnabled()).isTrue();
  }

  /** Validates success path in initialize(). */
  @Test
  @Override
  public void testInitializeSuccess() throws IOException {
    // Reuse loadConfig() to initialize auth related settings.
    Configuration config = loadConfig();

    // Set up remaining settings to known test values.
    int bufferSize = 512;
    config.setInt(
        GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_BUFFER_SIZE.getKey(), bufferSize);
    long blockSize = 1024;
    config.setLong(GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getKey(), blockSize);
    String rootBucketName = ghfsHelper.getUniqueBucketName("initialize-root");

    URI initUri = new Path("gs://" + rootBucketName).toUri();
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
    fs.initialize(initUri, config);
    GoogleCloudStorageOptions cloudStorageOptions =
        fs.getGcsFs().getOptions().getCloudStorageOptions();

    // Verify that config settings were set correctly.
    assertThat(cloudStorageOptions.getReadChannelOptions().getBufferSize()).isEqualTo(bufferSize);
    assertThat(fs.getDefaultBlockSize()).isEqualTo(blockSize);
    assertThat(fs.initUri).isEqualTo(initUri);
    assertThat(fs.getRootBucketName()).isEqualTo(rootBucketName);
  }

  @Test
  public void testInitializeSucceedsWhenNoProjectIdConfigured()
      throws URISyntaxException, IOException {
    Configuration config = loadConfig();
    // Unset Project ID
    config.unset(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey());

    URI gsUri = new Path("gs://foo").toUri();
    new GoogleHadoopFileSystem().initialize(gsUri, config);
  }

  @Test
  public void testInitializeThrowsWhenWrongSchemeConfigured()
      throws URISyntaxException, IOException {
    // Verify that we cannot initialize using URI with a wrong scheme.
    URI wrongScheme = new URI("http://foo/bar");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> new GoogleHadoopFileSystem().initialize(wrongScheme, new Configuration()));
    assertThat(thrown).hasMessageThat().contains("URI scheme not supported");
  }

  @Test
  public void testInitializeThrowsWhenCredentialsNotFound() throws URISyntaxException, IOException {
    String fakeClientId = "fooclient";
    URI gsUri = new URI("gs://foobar/");
    String fakeProjectId = "123456";
    Configuration config = new Configuration();
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_ENABLE.getKey(), false);
    // Set project ID and client ID but no client secret.
    config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), fakeProjectId);
    config.set(GoogleHadoopFileSystemConfiguration.AUTH_CLIENT_ID.getKey(), fakeClientId);

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    IllegalStateException thrown;
    if (GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE.get(
        config, config::getBoolean)) {
      ghfs.initialize(gsUri, config);
      thrown = assertThrows(IllegalStateException.class, ghfs::getGcsFs);
    } else {
      thrown = assertThrows(IllegalStateException.class, () -> ghfs.initialize(gsUri, config));
    }

    assertThat(thrown).hasMessageThat().contains("No valid credential configuration discovered");
  }

  /** Validates initialize() with configuration key fs.gs.working.dir set. */
  @Test
  @Override
  public void testInitializeWithWorkingDirectory() throws IOException, URISyntaxException {
    // We can just test by calling initialize multiple times (for each test condition) because
    // there is nothing in initialize() which must be run only once. If this changes, this test
    // method will need to resort to using a new GoogleHadoopFileSystem() for each item
    // in the for-loop.
    GoogleHadoopFileSystem myGhfs = (GoogleHadoopFileSystem) ghfs;

    Configuration config = loadConfig();
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
      config.set(
          GoogleHadoopFileSystemConfiguration.GCS_WORKING_DIRECTORY.getKey(), path.toString());
      ghfs.initialize(myGhfs.initUri, config);
      Path newWorkingDir = ghfs.getWorkingDirectory();
      if (expectedWorkingDir != null) {
        assertThat(newWorkingDir).isEqualTo(expectedWorkingDir);
      } else {
        assertThat(newWorkingDir).isEqualTo(currentWorkingDir);
      }
    }
    assertThat(ghfs.getHomeDirectory().toString()).startsWith("gs://" + rootBucketName);
  }

  /** Validates success path in configureBuckets(). */
  @Test
  @Override
  public void testConfigureBucketsSuccess() throws IOException {
    String rootBucketName = "gs://" + ghfsHelper.getUniqueBucketName("configure-root");

    URI initUri = new Path(rootBucketName).toUri();

    // To test configureBuckets which occurs after GCSFS initialization in configure(), while
    // still being reusable by derived unittests (we can't call loadConfig in a test case which
    // is inherited by a derived test), we will use the constructor which already provides a (fake)
    // GCSFS and skip the portions of the config specific to GCSFS.

    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());

    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initUri = initUri;
    fs.configureBuckets(fakeGcsFs);

    // Verify that config settings were set correctly.
    assertThat(fs.initUri).isEqualTo(initUri);

    initUri = new Path("gs://" + ghfsHelper.sharedBucketName1 + "/foo").toUri();
    fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initUri = initUri;
    fs.configureBuckets(fakeGcsFs);

    // Verify that config settings were set correctly.
    assertThat(fs.initUri).isEqualTo(initUri);

    assertThat(fs.getRootBucketName()).isEqualTo(initUri.getAuthority());
  }

  /** Validates success path when there is a root bucket but no system bucket is specified. */
  @Test
  @Override
  public void testConfigureBucketsWithRootBucketButNoSystemBucket() throws IOException {
    String rootBucketName = ghfsHelper.getUniqueBucketName("configure-root");
    URI initUri = new Path("gs://" + rootBucketName).toUri();
    GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());
    GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initUri = initUri;
    fs.configureBuckets(fakeGcsFs);

    // Verify that config settings were set correctly.
    assertThat(fs.initUri).isEqualTo(initUri);
  }

  /** Validates that exception thrown if no root bucket is specified. */
  @Test
  @Override
  public void testConfigureBucketsWithNeitherRootBucketNorSystemBucket() throws IOException {
    URI initUri = new Path("gs://").toUri();
    final GoogleCloudStorageFileSystem fakeGcsFs =
        new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());
    final GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem(fakeGcsFs);
    fs.initUri = initUri;

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> fs.configureBuckets(fakeGcsFs));

    assertThat(thrown).hasMessageThat().isEqualTo("No bucket specified in GCS URI: gs:/");
  }

  private Configuration getConfigurationWtihImplementation() throws IOException {
    Configuration conf = loadConfig();
    conf.set("fs.gs.impl", GoogleHadoopFileSystem.class.getCanonicalName());
    return conf;
  }

  @Test
  public void testFileSystemIsRemovedFromCacheOnClose() throws IOException, URISyntaxException {
    Configuration conf = getConfigurationWtihImplementation();

    URI fsUri = new URI(String.format("gs://%s/", sharedBucketName1));

    FileSystem fs1 = FileSystem.get(fsUri, conf);
    FileSystem fs2 = FileSystem.get(fsUri, conf);

    assertThat(fs2).isSameInstanceAs(fs1);

    fs1.close();

    FileSystem fs3 = FileSystem.get(fsUri, conf);
    assertThat(fs3).isNotSameInstanceAs(fs1);

    fs3.close();
  }

  @Test
  public void testIOExceptionIsThrowAfterClose() throws IOException, URISyntaxException {
    Configuration conf = getConfigurationWtihImplementation();

    URI fsUri = new URI(String.format("gs://%s/", sharedBucketName1));

    FileSystem fs1 = FileSystem.get(fsUri, conf);
    FileSystem fs2 = FileSystem.get(fsUri, conf);

    assertThat(fs2).isSameInstanceAs(fs1);

    fs1.close();

    assertThrows(IOException.class, () -> fs2.exists(new Path("/SomePath/That/Doesnt/Matter")));
  }

  public void createFile(Path filePath, byte[] data) throws IOException {
    try (FSDataOutputStream output = ghfs.create(filePath)) {
      output.write(data);
    }
  }

  @Test
  public void testGlobStatus() throws IOException {
    Path testRoot = new Path("/directory1/");
    ghfs.mkdirs(testRoot);
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
    assertThat(rootDirectories).hasLength(1);
    assertThat(rootDirectories[0].getPath().getName()).isEqualTo("directory1");

    FileStatus[] subDirectories = ghfs.globStatus(new Path("/directory1/s*"));
    assertThat(subDirectories).hasLength(2);

    FileStatus[] subDirectory1Files = ghfs.globStatus(new Path("/directory1/subdirectory1/*"));
    assertThat(subDirectory1Files).hasLength(2);
    assertThat(subDirectory1Files[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory1Files[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files = ghfs.globStatus(new Path("/directory1/subdirectory2/f*"));
    assertThat(subDirectory2Files).hasLength(2);
    assertThat(subDirectory2Files[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files2 = ghfs.globStatus(new Path("/directory1/subdirectory2/file?"));
    assertThat(subDirectory2Files2).hasLength(2);
    assertThat(subDirectory2Files2[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files2[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files3 =
        ghfs.globStatus(new Path("/directory1/subdirectory2/file[0-9]"));
    assertThat(subDirectory2Files3).hasLength(2);
    assertThat(subDirectory2Files3[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files3[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files4 =
        ghfs.globStatus(new Path("/directory1/subdirectory2/file[^1]"));
    assertThat(subDirectory2Files4).hasLength(1);
    assertThat(subDirectory2Files4[0].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files5 =
        ghfs.globStatus(new Path("/directory1/subdirectory2/file{1,2}"));
    assertThat(subDirectory2Files5).hasLength(2);
    assertThat(subDirectory2Files5[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files5[1].getPath().getName()).isEqualTo("file2");

    ghfs.delete(testRoot, true);
  }

  /** Tests getFileStatus() with non-default permissions. */
  @Test
  public void testConfigurablePermissions() throws IOException {
    String testPermissions = "777";
    Configuration conf = getConfigurationWtihImplementation();
    conf.set(GoogleHadoopFileSystemConfiguration.PERMISSIONS_TO_REPORT.getKey(), testPermissions);
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.getUri(), conf);
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "foo", 1, /* overwrite= */ true);

    FileStatus status = myGhfs.getFileStatus(filePath);
    assertThat(status.getPermission()).isEqualTo(new FsPermission(testPermissions));

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  /**
   * Test getFileStatus() uses the user reported by UGI
   *
   * @throws IOException
   */
  @Test
  public void testFileStatusUser() throws IOException, InterruptedException {
    String ugiUser = UUID.randomUUID().toString();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(ugiUser);
    Configuration conf = getConfigurationWtihImplementation();
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.getUri(), conf);
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "foo", 1, /* overwrite= */ true);

    FileStatus status =
        ugi.doAs((PrivilegedExceptionAction<FileStatus>) () -> myGhfs.getFileStatus(filePath));

    assertThat(status.getOwner()).isEqualTo(ugiUser);
    assertThat(status.getGroup()).isEqualTo(ugiUser);

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  /** Validates rename() dst as null. */
  @Test
  public void rename_dstAsNull_throwException() {
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    Path directory = new Path(String.format("gs://%s/testRename/", myGhfs.getRootBucketName()));
    Throwable exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.rename(directory, null));
    assertThat(exception).hasMessageThat().contains("dst must not be null");
  }

  /** Validates rename() src as null. */
  @Test
  public void rename_srcAsNull_throwException() {
    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    Path directory = new Path(String.format("gs://%s/testRename/", myGhfs.getRootBucketName()));
    Throwable exception =
        assertThrows(IllegalArgumentException.class, () -> myGhfs.rename(null, directory));
    assertThat(exception).hasMessageThat().contains("src must not be null");
  }

  @Test
  public void testCrc32cFileChecksum() throws Exception {
    testFileChecksum(
        GcsFileChecksumType.CRC32C,
        content -> Ints.toByteArray(Hashing.crc32c().hashString(content, UTF_8).asInt()));
  }

  @Test
  public void testMd5FileChecksum() throws Exception {
    testFileChecksum(
        GcsFileChecksumType.MD5, content -> Hashing.md5().hashString(content, UTF_8).asBytes());
  }

  private void testFileChecksum(
      GcsFileChecksumType checksumType, Function<String, byte[]> checksumFn) throws Exception {
    Configuration config = getConfigurationWtihImplementation();
    config.set("fs.gs.checksum.type", checksumType.name());

    GoogleHadoopFileSystem myGhfs = new GoogleHadoopFileSystem();
    myGhfs.initialize(ghfs.getUri(), config);

    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    String fileContent = "foo-testFileChecksum-" + checksumType;
    ghfsHelper.writeFile(filePath, fileContent, 1, /* overwrite= */ true);

    FileChecksum fileChecksum = myGhfs.getFileChecksum(filePath);

    assertThat(fileChecksum.getAlgorithmName()).isEqualTo(checksumType.getAlgorithmName());
    assertThat(fileChecksum.getLength()).isEqualTo(checksumType.getByteLength());
    assertThat(fileChecksum.getBytes()).isEqualTo(checksumFn.apply(fileContent));

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }
}
