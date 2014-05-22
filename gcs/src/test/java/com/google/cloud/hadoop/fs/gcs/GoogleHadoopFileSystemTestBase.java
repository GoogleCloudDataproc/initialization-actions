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
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageIntegrationTest;
import com.google.cloud.hadoop.util.HadoopVersionInfo;
import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Abstract base class for test suites targeting variants of GoogleHadoopFileSystem via the
 * Hadoop FileSystem interface. Includes general HadoopFileSystemTestBase cases plus some
 * behavior only visible at the GHFS level.
 */
@RunWith(JUnit4.class)
public abstract class GoogleHadoopFileSystemTestBase
    extends HadoopFileSystemTestBase {

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
    if (ghfs != null) {
      // For GHFS tests, print the counter values to stdout.
      // We cannot use ghfs.logCounters() because we disable logging for tests.
      String countersStr = ((GoogleHadoopFileSystemBase) ghfs).countersToString();
      System.out.println(countersStr);
    }
    HadoopFileSystemTestBase.afterAllTests();
  }

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
  public abstract void testInitializeFailure()
      throws IOException, URISyntaxException;

  @Test
  public abstract void testInitializeWithWorkingDirectory()
      throws IOException, URISyntaxException;

  @Test
  public abstract void testConfigureBucketsSuccess()
      throws URISyntaxException, IOException;

  @Test
  public abstract void testConfigureBucketsFailure()
      throws IOException, URISyntaxException;


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

    try {
      myghfs.getGcsPath(new Path("/buck^et", "object"));
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  /**
   * Verifies that test config can be accessed through the FS instance.
   */
  @Test
  public void testConfig() {
    GoogleHadoopFileSystemBase myghfs = (GoogleHadoopFileSystemBase) ghfs;
    Assert.assertEquals(
        GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT, myghfs.getBufferSizeOverride());
    Assert.assertEquals(
        GoogleHadoopFileSystemBase.BLOCK_SIZE_DEFAULT, myghfs.getDefaultBlockSize());
    Assert.assertTrue(!Strings.isNullOrEmpty(myghfs.getSystemBucketName()));
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
    GoogleHadoopFileSystemBase myghfs = (GoogleHadoopFileSystemBase) ghfs;
    GoogleCloudStorageFileSystem gcsfs = myghfs.getGcsFs();
    URI seedUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path parentPath = castAsHadoopPath(seedUri);
    URI parentUri = myghfs.getGcsPath(parentPath);

    // A subdir path that looks like gs://<bucket>/<generated-tempdir>/foo-subdir where
    // neither the subdir nor gs://<bucket>/<generated-tempdir> exist yet.
    Path subdirPath = new Path(parentPath, "foo-subdir");
    URI subdirUri = myghfs.getGcsPath(subdirPath);

    Path leafPath = new Path(subdirPath, "bar-subdir");
    URI leafUri = myghfs.getGcsPath(leafPath);
    gcsfs.mkdir(leafUri);

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    Assert.assertFalse("Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
    Assert.assertFalse("Expected to !exist: " + parentUri, gcsfs.exists(parentUri));

    myghfs.listStatus(parentPath);

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    Assert.assertTrue("Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));
    Assert.assertTrue("Expected to exist: " + parentUri, gcsfs.exists(parentUri));

    clearBucket(bucketName);

    // Reset for globStatus.
    gcsfs.mkdir(leafUri);

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    Assert.assertFalse("Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
    Assert.assertFalse("Expected to !exist: " + parentUri, gcsfs.exists(parentUri));

    myghfs.globStatus(parentPath);

    // Globbing the single directory only repairs that top-level directory; it is *not* the same
    // as listStatus.
    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    Assert.assertFalse("Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
    Assert.assertTrue("Expected to exist: " + parentUri, gcsfs.exists(parentUri));

    clearBucket(bucketName);

    // Reset for globStatus(path/*)
    gcsfs.mkdir(leafUri);

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    Assert.assertFalse("Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
    Assert.assertFalse("Expected to !exist: " + parentUri, gcsfs.exists(parentUri));

    // When globbing children, the parent will only be repaired if flat-globbing is not enabled.
    Path globChildrenPath = new Path(parentPath.toString() + "/*");
    myghfs.globStatus(globChildrenPath);
    boolean expectParentRepair = !myghfs.shouldUseFlatGlob(globChildrenPath);

    // This will internally call listStatus, so will have the same behavior of repairing both
    // levels of subdirectories.
    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));

    HadoopVersionInfo versionInfo = new HadoopVersionInfo();

    if (versionInfo.isLessThan(2, 0) || versionInfo.isGreaterThan(2, 3)) {
      Assert.assertTrue("Expected to exist: " + subdirUri, gcsfs.exists(subdirUri));

      if (expectParentRepair) {
        Assert.assertTrue("Expected to exist: " + parentUri, gcsfs.exists(parentUri));
      } else {
        Assert.assertFalse(
            "Expected not to exist due to flat globbing: " + parentUri, gcsfs.exists(parentUri));
      }
    }

    clearBucket(bucketName);

    // Reset for globStatus(path*)
    gcsfs.mkdir(leafUri);

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    Assert.assertFalse("Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
    Assert.assertFalse("Expected to !exist: " + parentUri, gcsfs.exists(parentUri));

    // Globbing with a wildcard in the parentUri itself also only repairs one level, but for
    // a different reason than globbing with no wildcard. Globbing with no wildcard requires
    // catching 'null' in globStatus, whereas having the wildcard causes the repair to happen
    // when listing parentOf(parentUri).
    myghfs.globStatus(new Path(parentPath.toString() + "*"));

    Assert.assertTrue("Expected to exist: " + leafUri, gcsfs.exists(leafUri));
    Assert.assertFalse("Expected to !exist: " + subdirUri, gcsfs.exists(subdirUri));
    if (versionInfo.isLessThan(2, 0) || versionInfo.isGreaterThan(2, 3)) {
      Assert.assertTrue("Expected to exist: " + parentUri, gcsfs.exists(parentUri));
    }
    clearBucket(bucketName);
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
    Path tempFilePath = castAsHadoopPath(tempFileUri);
    Path tempDirPath = tempFilePath.getParent();
    String text = "Hello World!";
    writeFile(tempFilePath, text, 1, false);

    // Another temporary file in GHFS.
    URI tempFileUri2 = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path tempFilePath2 = castAsHadoopPath(tempFileUri2);

    // Temporary file in local FS.
    File localTempFile = File.createTempFile("ghfs-test-", null);
    Path localTempFilePath = new Path(localTempFile.getPath());
    Path localTempDirPath = localTempFilePath.getParent();

    // -------------------------------------------------------
    // Call methods to provide coverage for. Note that we do not attempt to
    // test their functionality as we are not testing Hadoop engine here.
    try {
      ghfs.globStatus(tempFilePath);
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
      ghfs.makeQualified(localTempFilePath);
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


}
