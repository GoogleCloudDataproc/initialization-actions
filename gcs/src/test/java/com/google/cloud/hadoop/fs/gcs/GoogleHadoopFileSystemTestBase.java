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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.TimestampUpdatePredicate;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.testing.TestingAccessTokenProvider;
import com.google.common.base.Joiner;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Abstract base class for test suites targeting variants of GoogleHadoopFileSystem via the Hadoop
 * FileSystem interface. Includes general HadoopFileSystemTestBase cases plus some behavior only
 * visible at the GHFS level.
 */
public abstract class GoogleHadoopFileSystemTestBase extends HadoopFileSystemTestBase {

  private static final Joiner COMMA_JOINER = Joiner.on(',');

  /**
   * Helper to load all the GHFS-specific config values from environment variables, such as those
   * needed for setting up the credentials of a real GoogleCloudStorage.
   */
  protected static Configuration loadConfig() {
    TestConfiguration testConfiguration = TestConfiguration.getInstance();

    String projectId = testConfiguration.getProjectId();
    String privateKeyFile = testConfiguration.getPrivateKeyFile();
    String serviceAccount = testConfiguration.getServiceAccount();

    return loadConfig(projectId, serviceAccount, privateKeyFile);
  }

  /**
   * Helper to load GHFS-specific config values other than those from
   * the environment.
   */
  protected static Configuration loadConfig(
      String projectId, String serviceAccount, String privateKeyFile) {
    assertWithMessage("Expected value for env var %s", TestConfiguration.GCS_TEST_PROJECT_ID)
        .that(projectId)
        .isNotNull();
    assertWithMessage("Expected value for env var %s", TestConfiguration.GCS_TEST_SERVICE_ACCOUNT)
        .that(serviceAccount)
        .isNotNull();
    assertWithMessage("Expected value for env var %s", TestConfiguration.GCS_TEST_PRIVATE_KEYFILE)
        .that(privateKeyFile)
        .isNotNull();
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), projectId);
    config.set(
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_EMAIL.getKey(), serviceAccount);
    config.set(
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_KEY_FILE.getKey(), privateKeyFile);
    String systemBucketName = ghfsHelper.getUniqueBucketName("system");
    config.set(GoogleHadoopFileSystemConfiguration.GCS_SYSTEM_BUCKET.getKey(), systemBucketName);
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_CREATE_SYSTEM_BUCKET.getKey(), true);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey(), true);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE.getKey(), false);
    // Allow buckets to be deleted in test cleanup:
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCE_BUCKET_DELETE_ENABLE.getKey(), true);
    return config;
  }

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(GoogleHadoopFileSystemTestBase.class) {
        /** Perform clean-up once after all tests are turn. */
        @Override
        public void after() {
          if (ghfs != null) {
            // For GHFS tests, print the counter values to stdout.
            // We cannot use ghfs.logCounters() because we disable logging for tests.
            String countersStr = ((GoogleHadoopFileSystemBase) ghfs).countersToString();
            System.out.println(countersStr);
          }
          HadoopFileSystemTestBase.storageResource.after();
        }
      };

  // -----------------------------------------------------------------------------------------
  // Tests that vary according to the GHFS variant, but which we want to make sure get tested.
  // -----------------------------------------------------------------------------------------

  @Test
  public abstract void testCheckPathSuccess();

  @Test
  public abstract void testCheckPathFailure();

  @Test
  public abstract void testInitializeSuccess()
      throws IOException, URISyntaxException;

  @Test
  public abstract void testInitializeWithWorkingDirectory()
      throws IOException, URISyntaxException;

  @Test
  public abstract void testConfigureBucketsSuccess()
      throws URISyntaxException, IOException;

  @Test
  public abstract void testConfigureBucketsWithRootBucketButNoSystemBucket() throws IOException;

  @Test
  public abstract void testConfigureBucketsWithNeitherRootBucketNorSystemBucket()
      throws IOException;

  // -----------------------------------------------------------------------------------------
  // Tests that aren't supported by all configurations of GHFS.
  // -----------------------------------------------------------------------------------------

  @Test @Override
  public void testHsync() throws Exception {
    // hsync() is not supported in the default setup.
  }

  /** Tests getGcsPath(). */
  @Test
  public void testGetGcsPath() throws URISyntaxException {
    GoogleHadoopFileSystemBase myghfs = (GoogleHadoopFileSystemBase) ghfs;

    URI gcsPath = new URI("gs://" + myghfs.getSystemBucketName() + "/dir/obj");
    assertThat(myghfs.getGcsPath(new Path(gcsPath))).isEqualTo(gcsPath);

    assertThat(myghfs.getGcsPath(new Path("/buck^et", "object")))
        .isEqualTo(new URI("gs://" + myghfs.getSystemBucketName() + "/buck%5Eet/object"));
  }

  /**
   * Verifies that test config can be accessed through the FS instance.
   */
  @Test
  public void testConfig() {
    GoogleHadoopFileSystemBase myghfs = (GoogleHadoopFileSystemBase) ghfs;
    GoogleCloudStorageOptions cloudStorageOptions =
        myghfs.getGcsFs().getOptions().getCloudStorageOptions();

    assertThat(cloudStorageOptions.getReadChannelOptions().getBufferSize())
        .isEqualTo(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_BUFFER_SIZE.getDefault());
    assertThat(myghfs.getDefaultBlockSize())
        .isEqualTo(GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getDefault());
    assertThat(myghfs.getSystemBucketName()).isNotEmpty();
  }

  /**
   * Tests getCanonicalServiceName().
   */
  @Test
  public void testGetCanonicalServiceName() {
    assertThat(ghfs.getCanonicalServiceName()).isNull();
  }

  /** Test implicit directories. */
  @Test
  public void testImplicitDirectory() throws IOException {
    String bucketName = sharedBucketName1;
    GoogleHadoopFileSystemBase myghfs = (GoogleHadoopFileSystemBase) ghfs;
    GoogleCloudStorageFileSystem gcsfs = myghfs.getGcsFs();
    URI seedUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path parentPath = ghfsHelper.castAsHadoopPath(seedUri);
    URI parentUri = myghfs.getGcsPath(parentPath);

    // A subdir path that looks like gs://<bucket>/<generated-tempdir>/foo-subdir where
    // neither the subdir nor gs://<bucket>/<generated-tempdir> exist yet.
    Path subdirPath = new Path(parentPath, "foo-subdir");
    URI subdirUri = myghfs.getGcsPath(subdirPath);

    Path leafPath = new Path(subdirPath, "bar-subdir");
    URI leafUri = myghfs.getGcsPath(leafPath);
    gcsfs.mkdir(leafUri);

    boolean inferredDirExists =
        gcsfs.getOptions().getCloudStorageOptions().isInferImplicitDirectoriesEnabled();

    assertDirectory(gcsfs, leafUri, /* exists= */ true);
    assertDirectory(gcsfs, subdirUri, inferredDirExists);
    assertDirectory(gcsfs, parentUri, inferredDirExists);

    ghfsHelper.clearBucket(bucketName);
  }

  /**
   * Test directory repair at deletion
   *
   * @throws IOException
   */
  @Test
  public void testRepairDirectory() throws IOException {
    String bucketName = sharedBucketName1;
    GoogleHadoopFileSystemBase myghfs = (GoogleHadoopFileSystemBase) ghfs;
    GoogleCloudStorageFileSystem gcsfs = myghfs.getGcsFs();
    GoogleCloudStorage gcs = gcsfs.getGcs();
    URI seedUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path dirPath = ghfsHelper.castAsHadoopPath(seedUri);
    URI dirUri = myghfs.getGcsPath(dirPath);

    // A subdir path that looks like gs://<bucket>/<generated-tempdir>/foo-subdir where
    // neither the subdir nor gs://<bucket>/<generated-tempdir> exist yet.
    Path emptyObject = new Path(dirPath, "empty-object");
    URI objUri = myghfs.getGcsPath(emptyObject);
    StorageResourceId resource = gcsfs.getPathCodec().validatePathAndGetId(objUri, false);
    gcs.createEmptyObject(resource);

    boolean inferImplicitDirectories =
        gcsfs.getOptions().getCloudStorageOptions().isInferImplicitDirectoriesEnabled();

    assertWithMessage(
            "Expected to %s: %s", inferImplicitDirectories ? "exist" : "not exist", dirUri)
        .that(gcsfs.exists(dirUri))
        .isEqualTo(inferImplicitDirectories);

    gcsfs.delete(objUri, false);
    // Implicit directory created after deletion of the sole object in the directory
    assertWithMessage("Expected to exist: %s", dirUri).that(gcsfs.exists(dirUri)).isTrue();
    ghfsHelper.clearBucket(bucketName);

    // test implicit dir repair after a subdir vs. an object has been deleted (recursively)
    if (inferImplicitDirectories) {
      // only if directory inferring is enabled, the directory without the implicit
      // directory entry can be deleted without the FileNotFoundException
      Path subDir = new Path(dirPath, "subdir");
      emptyObject = new Path(subDir, "empty-object");
      objUri = myghfs.getGcsPath(emptyObject);
      resource = gcsfs.getPathCodec().validatePathAndGetId(objUri, false);
      gcs.createEmptyObject(resource);
      URI subdirUri = myghfs.getGcsPath(subDir);
      assertWithMessage("Expected to exist: %s", dirUri).that(gcsfs.exists(dirUri)).isTrue();
      assertWithMessage("Expected to exist: %s", subdirUri).that(gcsfs.exists(subdirUri)).isTrue();
      gcsfs.delete(subdirUri, true);
      // Implicit directory created after deletion of the sole object in the directory
      assertWithMessage("Expected to exist: %s", dirUri).that(gcsfs.exists(dirUri)).isTrue();
      ghfsHelper.clearBucket(bucketName);
    }
  }

  private static void assertDirectory(GoogleCloudStorageFileSystem gcsfs, URI path, boolean exists)
      throws IOException {
    assertWithMessage("Expected to %s: %s", exists ? "exist" : "not exist", path)
        .that(gcsfs.exists(path))
        .isEqualTo(exists);
    assertWithMessage("Expected to be a directory: %s", path)
        .that(gcsfs.getFileInfo(path).isDirectory())
        .isTrue();
  }

  /** Validates makeQualified() when working directory is not root. */
  @Test
  public void testMakeQualifiedNotRoot() {
    GoogleHadoopFileSystemBase myGhfs = (GoogleHadoopFileSystemBase) ghfs;
    Path fsRootPath = myGhfs.getFileSystemRoot();
    URI fsRootUri = fsRootPath.toUri();
    String fsRoot = fsRootPath.toString();
    String workingParent = fsRoot + "working/";
    String workingDir = workingParent + "dir";
    myGhfs.setWorkingDirectory(new Path(workingDir));
    Map<String, String > qualifiedPaths = new HashMap<>();
    qualifiedPaths.put("/", fsRoot);
    qualifiedPaths.put("/foo", fsRoot + "foo");
    qualifiedPaths.put("/foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(".", workingDir);
    qualifiedPaths.put("foo", workingDir + "/foo");
    qualifiedPaths.put("foo/bar", workingDir + "/foo/bar");
    qualifiedPaths.put(fsRoot, fsRoot);
    qualifiedPaths.put(fsRoot + "foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("/foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("/foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("foo/../foo", workingDir + "/foo");
    qualifiedPaths.put("foo/bar/../../foo/bar", workingDir + "/foo/bar");
    qualifiedPaths.put(fsRoot + "foo/../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("..", workingParent);
    qualifiedPaths.put("../..", fsRoot);
    qualifiedPaths.put("../foo", workingParent + "/foo");
    qualifiedPaths.put("../foo/bar", workingParent + "/foo/bar");
    qualifiedPaths.put("../foo/../foo", workingParent + "/foo");
    qualifiedPaths.put("../foo/bar/../../foo/bar", workingParent + "/foo/bar");
    qualifiedPaths.put(workingDir + "/../foo/../foo", workingParent + "/foo");
    qualifiedPaths.put(workingDir + "/../foo/bar/../../foo/bar", workingParent + "/foo/bar");
    qualifiedPaths.put(fsRoot + "..foo/bar", fsRoot + "..foo/bar");
    qualifiedPaths.put("..foo/bar", workingDir + "/..foo/bar");

    // GHFS specific behavior where root is it's own parent.
    qualifiedPaths.put("/..", fsRoot);
    qualifiedPaths.put("/../../..", fsRoot);
    qualifiedPaths.put("/../foo/", fsRoot + "foo");
    qualifiedPaths.put("/../../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("../../..", fsRoot);
    qualifiedPaths.put(fsRoot + "..", fsRoot);
    qualifiedPaths.put(fsRoot + "../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("../../../foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("../../../foo/bar/../../foo/bar", fsRoot + "foo/bar");

    // Skip for authority-less gsg paths.
    if (fsRootUri.getAuthority() != null) {
      // When the path to qualify is of the form gs://somebucket, we want to qualify
      // it as gs://someBucket/
      qualifiedPaths.put(
          fsRoot.substring(0, fsRoot.length() - 1),
          fsRoot);
    }

    for (String unqualifiedString : qualifiedPaths.keySet()) {
      Path unqualifiedPath = new Path(unqualifiedString);
      Path qualifiedPath = new Path(qualifiedPaths.get(unqualifiedString));
      assertThat(qualifiedPath).isEqualTo(myGhfs.makeQualified(unqualifiedPath));
    }
  }

  /**
   * Validates makeQualified() when working directory is root.
   */
  @Test
  public void testMakeQualifiedRoot()  {
    GoogleHadoopFileSystemBase myGhfs = (GoogleHadoopFileSystemBase) ghfs;
    myGhfs.setWorkingDirectory(myGhfs.getFileSystemRoot());
    Path fsRootPath = myGhfs.getFileSystemRoot();
    URI fsRootUri = fsRootPath.toUri();
    String fsRoot = fsRootPath.toString();
    Map<String, String > qualifiedPaths = new HashMap<>();
    qualifiedPaths.put("/", fsRoot);
    qualifiedPaths.put("/foo", fsRoot + "foo");
    qualifiedPaths.put("/foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(".", fsRoot);
    qualifiedPaths.put("foo", fsRoot + "foo");
    qualifiedPaths.put("foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot, fsRoot);
    qualifiedPaths.put(fsRoot + "foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("/foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("/foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "foo/../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "..foo/bar", fsRoot + "..foo/bar");
    qualifiedPaths.put("..foo/bar", fsRoot + "..foo/bar");

    // GHFS specific behavior where root is it's own parent.
    qualifiedPaths.put("/..", fsRoot);
    qualifiedPaths.put("/../../..", fsRoot);
    qualifiedPaths.put("/../foo/", fsRoot + "foo");
    qualifiedPaths.put("/../../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("..", fsRoot);
    qualifiedPaths.put("../..", fsRoot);
    qualifiedPaths.put("../foo", fsRoot + "foo");
    qualifiedPaths.put("../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "..", fsRoot);
    qualifiedPaths.put(fsRoot + "../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("../../../foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put("../foo/../foo", fsRoot + "foo");
    qualifiedPaths.put("../foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "../foo/../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "../foo/bar/../../foo/bar", fsRoot + "foo/bar");
    qualifiedPaths.put(fsRoot + "foo/../../../../foo", fsRoot + "foo");
    qualifiedPaths.put(fsRoot + "foo/bar/../../../../../foo/bar", fsRoot + "foo/bar");

    // Skip for authority-less gsg paths.
    if (fsRootUri.getAuthority() != null) {
      // When the path to qualify is of the form gs://somebucket, we want to qualify
      // it as gs://someBucket/
      qualifiedPaths.put(
          fsRoot.substring(0, fsRoot.length() - 1),
          fsRoot);
    }

    for (String unqualifiedString : qualifiedPaths.keySet()) {
      Path unqualifiedPath = new Path(unqualifiedString);
      Path qualifiedPath = new Path(qualifiedPaths.get(unqualifiedString));
      assertThat(qualifiedPath).isEqualTo(myGhfs.makeQualified(unqualifiedPath));
    }
  }

  /**
   * We override certain methods in FileSystem simply to provide debug tracing. (Search for
   * "Overridden functions for debug tracing" in GoogleHadoopFileSystemBase.java). We do not add or
   * update any functionality for such methods. The following tests simply exercise that path to
   * ensure coverage. Consequently, they do not really test any functionality.
   *
   * <p>Having coverage for these methods lets us easily determine the amount of coverage that is
   * missing in the rest of the code.
   */
  @Test
  public void provideCoverageForUnmodifiedMethods() throws IOException {
    // -------------------------------------------------------
    // Create test data.

    // Temporary file in GHFS.
    URI tempFileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path tempFilePath = ghfsHelper.castAsHadoopPath(tempFileUri);
    Path tempDirPath = tempFilePath.getParent();
    String text = "Hello World!";
    ghfsHelper.writeFile(tempFilePath, text, 1, /* overwrite= */ false);

    // Another temporary file in GHFS.
    URI tempFileUri2 = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path tempFilePath2 = ghfsHelper.castAsHadoopPath(tempFileUri2);

    // Temporary file in local FS.
    File localTempFile = File.createTempFile("ghfs-test-", null);
    Path localTempFilePath = new Path(localTempFile.getPath());
    Path localTempDirPath = localTempFilePath.getParent();

    // -------------------------------------------------------
    // Call methods to provide coverage for. Note that we do not attempt to
    // test their functionality as we are not testing Hadoop engine here.
    try {
      ghfs.deleteOnExit(tempFilePath);
      ghfs.getContentSummary(tempFilePath);
      ghfs.getDelegationToken("foo");
      ghfs.copyFromLocalFile(false, true, localTempFilePath, tempDirPath);
      ghfs.copyFromLocalFile(false, true, new Path[] { localTempFilePath }, tempDirPath);
      localTempFile.delete();
      ghfs.copyToLocalFile(true, tempFilePath, localTempDirPath);
      File localCopiedFile = new File(localTempDirPath.toString(), tempFilePath.getName());
      localCopiedFile.delete();
      Path localOutputPath = ghfs.startLocalOutput(tempFilePath2, localTempFilePath);
      try (Writer writer = Files.newBufferedWriter(Paths.get(localOutputPath.toString()), UTF_8)) {
        writer.write(text);
      }
      ghfs.completeLocalOutput(tempFilePath2, localOutputPath);
      ghfs.getUsed();
      ghfs.setVerifyChecksum(false);
      ghfs.getFileChecksum(tempFilePath2);
      ghfs.setPermission(tempFilePath2, FsPermission.getDefault());
      try {
        ghfs.setOwner(tempFilePath2, "foo-user", "foo-group");
      } catch (IOException ioe) {
        // Some filesystems (like the LocalFileSystem) are strict about existence of owners.
        // TODO(user): Abstract out the behaviors around owners/permissions and properly test
        //  the different behaviors between different filesystems.
      }
      ghfs.setTimes(tempFilePath2, 0, 0);
    } finally {
      // We do not need to separately delete the temp files created in GHFS because
      // we delete all test buckets recursively at the end of the tests.
      if (localTempFile.exists()) {
        localTempFile.delete();
      }
    }
  }

  @Test
  public void testIncludedParentPathPredicates() throws URISyntaxException {
    Configuration configuration = new Configuration();
    // 1 Disable all updates and then try to include all paths
    configuration.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_ENABLE.getKey(), false);
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getKey(), "/");
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES.getKey(), "");

    TimestampUpdatePredicate predicate =
        GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate.create(configuration);

    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("/foobar")))
        .isFalse();
    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("")))
        .isFalse();

    // 2 Enable updates, set include to everything and exclude to everything
    configuration.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_ENABLE.getKey(), true);
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getKey(), "/");
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES.getKey(), "/");

    predicate = GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate
        .create(configuration);

    assertWithMessage("Should be included")
        .that(predicate.shouldUpdateTimestamp(new URI("/foobar")))
        .isTrue();
    assertWithMessage("Should be included")
        .that(predicate.shouldUpdateTimestamp(new URI("")))
        .isTrue();

    // 3 Enable specific paths, exclude everything:
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getKey(),
        "/foobar,/baz");
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES.getKey(), "/");

    predicate = GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate
        .create(configuration);

    assertWithMessage("Should be included")
        .that(predicate.shouldUpdateTimestamp(new URI("asdf/foobar")))
        .isTrue();
    assertWithMessage("Should be included")
        .that(predicate.shouldUpdateTimestamp(new URI("asdf/baz")))
        .isTrue();
    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("/anythingElse")))
        .isFalse();
    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("/")))
        .isFalse();

    // 4 set to defaults, set job history paths
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getKey(),
        COMMA_JOINER.join(
            GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getDefault()));
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES.getKey(),
        COMMA_JOINER.join(
            GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES.getDefault()));
    configuration.set(
        GoogleHadoopFileSystemConfiguration.MR_JOB_HISTORY_DONE_DIR_KEY, "/tmp/hadoop-yarn/done");
    configuration.set(
        GoogleHadoopFileSystemConfiguration.MR_JOB_HISTORY_INTERMEDIATE_DONE_DIR_KEY,
        "/tmp/hadoop-yarn/staging/done");

    predicate =
        GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate.create(configuration);

    assertThat(
            configuration.get(
                GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getKey()))
        .isEqualTo("/tmp/hadoop-yarn/staging/done,/tmp/hadoop-yarn/done");

    assertWithMessage("Should be included")
        .that(predicate.shouldUpdateTimestamp(new URI("gs://bucket/tmp/hadoop-yarn/staging/done/")))
        .isTrue();
    assertWithMessage("Should be included")
        .that(predicate.shouldUpdateTimestamp(new URI("gs://bucket/tmp/hadoop-yarn/done/")))
        .isTrue();
    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("asdf/baz")))
        .isFalse();
    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("/anythingElse")))
        .isFalse();
    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("/")))
        .isFalse();

    // 5 set to defaults, set job history paths with gs:// scheme
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getKey(),
        COMMA_JOINER.join(
            GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getDefault()));
    configuration.set(
        GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES.getKey(),
        COMMA_JOINER.join(
            GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES.getDefault()));
    configuration.set(
        GoogleHadoopFileSystemConfiguration.MR_JOB_HISTORY_DONE_DIR_KEY,
        "gs://foo-bucket/tmp/hadoop-yarn/done");
    configuration.set(
        GoogleHadoopFileSystemConfiguration.MR_JOB_HISTORY_INTERMEDIATE_DONE_DIR_KEY,
        "gs://foo-bucket/tmp/hadoop-yarn/staging/done");

    predicate =
        GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate.create(configuration);

    assertThat(
            configuration.get(
                GoogleHadoopFileSystemConfiguration.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES.getKey()))
        .isEqualTo(
            "gs://foo-bucket/tmp/hadoop-yarn/staging/done,gs://foo-bucket/tmp/hadoop-yarn/done");

    assertWithMessage("Should be included")
        .that(
            predicate.shouldUpdateTimestamp(
                new URI("gs://foo-bucket/tmp/hadoop-yarn/staging/done/")))
        .isTrue();
    assertWithMessage("Should be included")
        .that(predicate.shouldUpdateTimestamp(new URI("gs://foo-bucket/tmp/hadoop-yarn/done/")))
        .isTrue();
    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("asdf/baz")))
        .isFalse();
    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("/anythingElse")))
        .isFalse();
    assertWithMessage("Should be ignored")
        .that(predicate.shouldUpdateTimestamp(new URI("/")))
        .isFalse();
  }

  @Test
  public void testInvalidCredentialFromAccessTokenProvider() throws Exception {
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemConfiguration.GCS_SYSTEM_BUCKET.getKey(), sharedBucketName1);
    config.set("fs.gs.auth.access.token.provider.impl", TestingAccessTokenProvider.class.getName());
    URI gsUri = new URI("gs://foobar/");

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();

    IOException thrown;
    if (GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE.get(
        config, config::getBoolean)) {
      ghfs.initialize(gsUri, config);
      thrown = (IOException) assertThrows(RuntimeException.class, ghfs::getGcsFs).getCause();
    } else {
      thrown = assertThrows(IOException.class, () -> ghfs.initialize(gsUri, config));
    }

    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Invalid Credentials");
  }
}
