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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions.TimestampUpdatePredicate;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.testing.TestingAccessTokenProvider;
import com.google.cloud.hadoop.util.HadoopVersionInfo;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Abstract base class for test suites targeting variants of GoogleHadoopFileSystem via the Hadoop
 * FileSystem interface. Includes general HadoopFileSystemTestBase cases plus some behavior only
 * visible at the GHFS level.
 */
public abstract class GoogleHadoopFileSystemTestBase extends HadoopFileSystemTestBase {

  /**
   * Helper to load all the GHFS-specific config values from environment variables, such as those
   * needed for setting up the credentials of a real GoogleCloudStorage.
   */
  protected static Configuration loadConfig()
      throws IOException {
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
    Assert.assertNotNull(
        "Expected value for env var " + TestConfiguration.GCS_TEST_PROJECT_ID, projectId);
    Assert.assertNotNull(
        "Expected value for env var " + TestConfiguration.GCS_TEST_SERVICE_ACCOUNT, serviceAccount);
    Assert.assertNotNull(
        "Expected value for env var " + TestConfiguration.GCS_TEST_PRIVATE_KEYFILE, privateKeyFile);
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, projectId);
    config.set(GoogleHadoopFileSystemBase.SERVICE_ACCOUNT_AUTH_EMAIL_KEY, serviceAccount);
    config.set(GoogleHadoopFileSystemBase.SERVICE_ACCOUNT_AUTH_KEYFILE_KEY, privateKeyFile);
    String systemBucketName = ghfsHelper.getUniqueBucketName("system");
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, systemBucketName);
    config.setBoolean(GoogleHadoopFileSystemBase.GCS_CREATE_SYSTEM_BUCKET_KEY, true);
    config.setBoolean(
        GoogleHadoopFileSystemBase.GCS_ENABLE_REPAIR_IMPLICIT_DIRECTORIES_KEY, true);
    config.setBoolean(
        GoogleHadoopFileSystemBase.GCS_ENABLE_INFER_IMPLICIT_DIRECTORIES_KEY, false);
    // Allow buckets to be deleted in test cleanup:
    config.setBoolean(
        GoogleHadoopFileSystemBase.GCE_BUCKET_DELETE_ENABLE_KEY, true);
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

  // -----------------------------------------------------------------------------------------
  // Tests that aren't supported by all configurations of GHFS.
  // -----------------------------------------------------------------------------------------

  @Test @Override
  public void testHsync() throws Exception {
    // hsync() is not supported in the default setup.
  }

  /**
   * Tests getGcsPath().
   */
  @Test
  public void testGetGcsPath()
      throws URISyntaxException {
    GoogleHadoopFileSystemBase myghfs = (GoogleHadoopFileSystemBase) ghfs;
    URI gcsPath = new URI("gs://" + myghfs.getSystemBucketName() + "/dir/obj");
    URI convertedPath = myghfs.getGcsPath(new Path(gcsPath));
    Assert.assertEquals(gcsPath, convertedPath);

    assertThrows(
        IllegalArgumentException.class, () -> myghfs.getGcsPath(new Path("/buck^et", "object")));
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
        .isEqualTo(GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT);
    assertThat(myghfs.getDefaultBlockSize())
        .isEqualTo(GoogleHadoopFileSystemBase.BLOCK_SIZE_DEFAULT);
    assertThat(myghfs.getSystemBucketName()).isNotEmpty();
  }

  /**
   * Tests getCanonicalServiceName().
   */
  @Test
  public void testGetCanonicalServiceName() {
    Assert.assertNull(ghfs.getCanonicalServiceName());
  }

  /**
   * Makes listStatus and globStatus perform repairs by first creating an object directly without
   * creating its parent directory object.
   */
  @Test
  public void testRepairImplicitDirectory()
      throws IOException, URISyntaxException {
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

    boolean inferImplicitDirectories = gcsfs.getOptions()
        .getCloudStorageOptions().isInferImplicitDirectoriesEnabled();

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    if (inferImplicitDirectories) {
      Assert.assertTrue(
          "Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));
      Assert.assertTrue(
          "Expected to exist: " + parentUri, gcsfs.exists(parentUri));
    } else {
      Assert.assertFalse(
          "Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
      Assert.assertFalse(
          "Expected to !exist: " + parentUri, gcsfs.exists(parentUri));
    }

    myghfs.listStatus(parentPath);

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    Assert.assertTrue("Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));
    Assert.assertTrue("Expected to exist: " + parentUri, gcsfs.exists(parentUri));

    ghfsHelper.clearBucket(bucketName);

    // Reset for globStatus.
    gcsfs.mkdir(leafUri);

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    if (inferImplicitDirectories) {
      Assert.assertTrue(
          "Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));
      Assert.assertTrue(
          "Expected to exist: " + parentUri, gcsfs.exists(parentUri));
    } else {
      Assert.assertFalse(
          "Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
      Assert.assertFalse(
          "Expected to !exist: " + parentUri, gcsfs.exists(parentUri));
    }

    myghfs.globStatus(parentPath);

    // Globbing the single directory only repairs that top-level directory; it is *not* the same
    // as listStatus.
    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    if (inferImplicitDirectories) {
      Assert.assertTrue(
          "Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));
    } else {
      Assert.assertFalse(
          "Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
    }
    Assert.assertTrue("Expected to exist: " + parentUri, gcsfs.exists(parentUri));

    ghfsHelper.clearBucket(bucketName);

    // Reset for globStatus(path/*)
    gcsfs.mkdir(leafUri);

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    if (inferImplicitDirectories) {
      Assert.assertTrue(
          "Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));
      Assert.assertTrue(
          "Expected to exist: " + parentUri, gcsfs.exists(parentUri));
    } else {
      Assert.assertFalse(
          "Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
      Assert.assertFalse(
          "Expected to !exist: " + parentUri, gcsfs.exists(parentUri));
    }

    // When globbing children, the parent will only be repaired if flat-globbing is not enabled.
    Path globChildrenPath = new Path(parentPath.toString() + "/*");
    myghfs.globStatus(globChildrenPath);
    boolean expectParentRepair = !myghfs.shouldUseFlatGlob(globChildrenPath);

    // This will internally call listStatus, so will have the same behavior of repairing both
    // levels of subdirectories.
    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));

    HadoopVersionInfo versionInfo = HadoopVersionInfo.getInstance();

    if (versionInfo.isLessThan(2, 0) || versionInfo.isGreaterThan(2, 3)) {
      Assert.assertTrue("Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));

      if (expectParentRepair || inferImplicitDirectories) {
        Assert.assertTrue("Expected to exist: " + parentUri, gcsfs.exists(parentUri));
      } else {
        Assert.assertFalse(
            "Expected not to exist due to flat globbing: " + parentUri, gcsfs.exists(parentUri));
      }
    }

    ghfsHelper.clearBucket(bucketName);

    // Reset for globStatus(path*)
    gcsfs.mkdir(leafUri);

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    if (inferImplicitDirectories) {
      Assert.assertTrue(
          "Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));
      Assert.assertTrue(
          "Expected to exist: " + parentUri, gcsfs.exists(parentUri));
    } else {
      Assert.assertFalse(
          "Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
      Assert.assertFalse(
          "Expected to !exist: " + parentUri, gcsfs.exists(parentUri));
    }

    // Globbing with a wildcard in the parentUri itself also only repairs one level, but for
    // a different reason than globbing with no wildcard. Globbing with no wildcard requires
    // catching 'null' in globStatus, whereas having the wildcard causes the repair to happen
    // when listing parentOf(parentUri).
    myghfs.globStatus(new Path(parentPath.toString() + "*"));

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    if (inferImplicitDirectories) {
      Assert.assertTrue(
          "Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));
    } else {
      Assert.assertFalse(
          "Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
    }
    if (versionInfo.isLessThan(2, 0) || versionInfo.isGreaterThan(2, 3)) {
      Assert.assertTrue("Expected to exist: " + parentUri, gcsfs.exists(parentUri));
    }
    ghfsHelper.clearBucket(bucketName);
  }

 /**
   * Validates makeQualified() when working directory is not root.
   */
  @Test
  public void testMakeQualifiedNotRoot()  {
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
      Assert.assertEquals(qualifiedPath, myGhfs.makeQualified(unqualifiedPath));
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
    qualifiedPaths.put("../foo/../foo", fsRoot + "foo");
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
      Assert.assertEquals(qualifiedPath, myGhfs.makeQualified(unqualifiedPath));
    }
  }

  /**
   * We override certain methods in FileSystem simply to provide debug tracing. (Search for
   * "Overridden functions for debug tracing" in GoogleHadoopFileSystemBase.java).
   * We do not add or update any functionality for such methods. The following
   * tests simply exercise that path to ensure coverage. Consequently, they do not
   * really test any functionality.
   *
   * Having coverage for these methods lets us easily determine the amount of
   * coverage that is missing in the rest of the code.
   */
  @Test
  public void provideCoverageForUnmodifiedMethods()
      throws IOException {
    // -------------------------------------------------------
    // Create test data.

    // Temporary file in GHFS.
    URI tempFileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path tempFilePath = ghfsHelper.castAsHadoopPath(tempFileUri);
    Path tempDirPath = tempFilePath.getParent();
    String text = "Hello World!";
    ghfsHelper.writeFile(tempFilePath, text, 1, false);

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
      FileWriter writer = new FileWriter(localOutputPath.toString());
      writer.write(text);
      writer.close();
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
        // the different behaviors between different filesystems.
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
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_KEY, false);
    configuration.set(GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY, "/");
    configuration.set(GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY, "");

    TimestampUpdatePredicate predicate =
        GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate.create(configuration);

    Assert.assertFalse("Should be ignored", predicate.shouldUpdateTimestamp(new URI("/foobar")));
    Assert.assertFalse("Should be ignored", predicate.shouldUpdateTimestamp(new URI("")));

    // 2 Enable updates, set include to everything and exclude to everything
    configuration.setBoolean(
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_ENABLE_KEY, true);
    configuration.set(GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY, "/");
    configuration.set(GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY, "/");

    predicate = GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate
        .create(configuration);

    Assert.assertTrue("Should be included", predicate.shouldUpdateTimestamp(new URI("/foobar")));
    Assert.assertTrue("Should be included", predicate.shouldUpdateTimestamp(new URI("")));

    // 3 Enable specific paths, exclude everything:
    configuration.set(
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY, "/foobar,/baz");
    configuration.set(
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY, "/");

    predicate = GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate
        .create(configuration);

    Assert.assertTrue(
        "Should be included", predicate.shouldUpdateTimestamp(new URI("asdf/foobar")));
    Assert.assertTrue(
        "Should be included",  predicate.shouldUpdateTimestamp(new URI("asdf/baz")));
    Assert.assertFalse(
        "Should be ignored",  predicate.shouldUpdateTimestamp(new URI("/anythingElse")));
    Assert.assertFalse(
        "Should be ignored",  predicate.shouldUpdateTimestamp(new URI("/")));

    // 4 set to defaults, set job history paths
    configuration.set(
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY,
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_DEFAULT);
    configuration.set(
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY,
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_DEFAULT);
    configuration.set(
        GoogleHadoopFileSystemBase.MR_JOB_HISTORY_DONE_DIR_KEY,
        "/tmp/hadoop-yarn/done");
    configuration.set(
        GoogleHadoopFileSystemBase.MR_JOB_HISTORY_INTERMEDIATE_DONE_DIR_KEY,
        "/tmp/hadoop-yarn/staging/done");

    predicate =
        GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate.create(configuration);

    Assert.assertEquals(
        "/tmp/hadoop-yarn/staging/done,/tmp/hadoop-yarn/done",
        configuration.get(GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY));

    Assert.assertTrue(
        "Should be included",
        predicate.shouldUpdateTimestamp(new URI("gs://bucket/tmp/hadoop-yarn/staging/done/")));
    Assert.assertTrue(
        "Should be included",
        predicate.shouldUpdateTimestamp(new URI("gs://bucket/tmp/hadoop-yarn/done/")));
    Assert.assertFalse(
        "Should be ignored",
        predicate.shouldUpdateTimestamp(new URI("asdf/baz")));
    Assert.assertFalse(
        "Should be ignored",
        predicate.shouldUpdateTimestamp(new URI("/anythingElse")));
    Assert.assertFalse(
        "Should be ignored",
        predicate.shouldUpdateTimestamp(new URI("/")));

    // 5 set to defaults, set job history paths with gs:// scheme
    configuration.set(
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY,
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_DEFAULT);
    configuration.set(
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_KEY,
        GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_EXCLUDES_DEFAULT);
    configuration.set(
        GoogleHadoopFileSystemBase.MR_JOB_HISTORY_DONE_DIR_KEY,
        "gs://foo-bucket/tmp/hadoop-yarn/done");
    configuration.set(
        GoogleHadoopFileSystemBase.MR_JOB_HISTORY_INTERMEDIATE_DONE_DIR_KEY,
        "gs://foo-bucket/tmp/hadoop-yarn/staging/done");

    predicate =
        GoogleHadoopFileSystemBase.ParentTimestampUpdateIncludePredicate.create(configuration);

    Assert.assertEquals(
        "gs://foo-bucket/tmp/hadoop-yarn/staging/done,gs://foo-bucket/tmp/hadoop-yarn/done",
        configuration.get(GoogleHadoopFileSystemBase.GCS_PARENT_TIMESTAMP_UPDATE_INCLUDES_KEY));

    Assert.assertTrue(
        "Should be included",
        predicate.shouldUpdateTimestamp(new URI("gs://foo-bucket/tmp/hadoop-yarn/staging/done/")));
    Assert.assertTrue(
        "Should be included",
        predicate.shouldUpdateTimestamp(new URI("gs://foo-bucket/tmp/hadoop-yarn/done/")));
    Assert.assertFalse(
        "Should be ignored",
        predicate.shouldUpdateTimestamp(new URI("asdf/baz")));
    Assert.assertFalse(
        "Should be ignored",
        predicate.shouldUpdateTimestamp(new URI("/anythingElse")));
    Assert.assertFalse(
        "Should be ignored",
        predicate.shouldUpdateTimestamp(new URI("/")));
  }

  @Test
  public void testInvalidCredentialFromAccessTokenProvider()
      throws URISyntaxException, IOException {
    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, sharedBucketName1);
    config.set("fs.gs.auth.access.token.provider.impl", TestingAccessTokenProvider.class.getName());
    URI gsUri = new URI("gs://foobar/");

    IOException thrown =
        assertThrows(
            IOException.class, () -> new GoogleHadoopFileSystem().initialize(gsUri, config));
    assertThat(thrown).hasCauseThat().hasMessageThat().contains("Invalid Credentials");
  }
}
