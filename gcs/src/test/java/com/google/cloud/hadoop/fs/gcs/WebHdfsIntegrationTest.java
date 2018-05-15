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
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for HDFS.
 *
 * <p>This class allows running all tests in HadoopFileSystemTestBase
 * against HDFS through WebHDFS protocol. This allows us to determine if HDFS behavior is different
 * from GHFS behavior and if so, fix GHFS to match HDFS behavior.
 *
 * <p>We enable it by mapping paths used by GHFS tests to HDFS paths.
 *
 * <p>This class overrides the initial setup of the FileSystem under test to inject an actual HDFS
 * implementation, as well as injecting a version of FileSystemDescriptor which properly describes
 * the behavior of HDFS. The FileSystemDescriptor thus reroutes all the test methods through the
 * proper HDFS instance using webhdfs:/ paths.
 */
@RunWith(JUnit4.class)
public class WebHdfsIntegrationTest extends HadoopFileSystemTestBase {

  // Environment variable from which to get HDFS access info.
  public static final String WEBHDFS_ROOT = "WEBHDFS_ROOT";

  // HDFS path (passed to the test through environment var).
  static String hdfsRoot;

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(WebHdfsIntegrationTest.class) {
        /** Performs initialization once before tests are run. */
        @Override
        public void before() throws Throwable {
          // Get info about the HDFS instance against which we run tests.
          hdfsRoot = System.getenv(WEBHDFS_ROOT);
          assertThat(hdfsRoot).isNotNull();

          // Create a FileSystem instance to access the given HDFS.
          URI hdfsUri = new URI(hdfsRoot);
          Configuration config = new Configuration();
          config.set("fs.default.name", hdfsRoot);
          ghfs = FileSystem.get(hdfsUri, config);
          ghfsFileSystemDescriptor =
              new FileSystemDescriptor() {
                @Override
                public Path getFileSystemRoot() {
                  return new Path(hdfsRoot);
                }

                @Override
                public String getScheme() {
                  return getFileSystemRoot().toUri().getScheme();
                }

                @Deprecated
                @Override
                public String getHadoopScheme() {
                  return getScheme();
                }
              };

          postCreateInit();
          ghfsHelper.setIgnoreStatistics();
        }

        /** Perform clean-up once after all tests are turn. */
        @Override
        public void after() {
          HadoopFileSystemTestBase.storageResource.after();
        }
      };

  /**
   * Perform initialization after creating test instances.
   */
  public static void postCreateInit()
      throws IOException {
    HadoopFileSystemTestBase.postCreateInit();
  }

  // -----------------------------------------------------------------
  // Tests that exercise behavior defined in HdfsBehavior.
  // -----------------------------------------------------------------

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
    renameHelper(new HdfsBehavior() {
        @Override
        public MethodOutcome renameRootOutcome() {
          // Unlike HDFS which returns false, WebHdfs throws when trying to rename root.
          return new MethodOutcome(
              MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
        }
      });
  }

  // -----------------------------------------------------------------

  /**
   * Validates append().
   */
  @Test @Override
  public void testAppend()
      throws IOException {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);
    // For now, verify that append does not throw. We are not interested in
    // verifying that append() actually appends correctly. We will do that
    // once GHFS also starts supporting appends.
    ghfs.append(hadoopPath, GoogleHadoopFileSystemBase.BUFFERSIZE_DEFAULT, null);
  }

  /**
   * Validates getDefaultReplication().
   */
  @Test @Override
  public void testGetDefaultReplication()
      throws IOException {
    assertWithMessage("Expected default replication factor >= 1")
        .that(ghfs.getDefaultReplication() >= 1)
        .isTrue();
  }

  /**
   * Validates that we cannot open a non-existent object.
   * Note: WebHDFS throws IOException (Internal Server Error (error code=500))
   */
  @Test @Override
  public void testOpenNonExistent()
      throws IOException {
    String bucketName = ghfsHelper.getUniqueBucketName("open-non-existent");
    IOException e =
        assertThrows(
            IOException.class, () -> ghfsHelper.readTextFile(bucketName, objectName, 0, 100, true));
    assertThat(e).hasMessageThat().contains("Internal Server Error (error code=500)");
  }

  /**
   * Validates partial reads.
   *
   * Note:
   * WebHDFS implementation has a bug that does not handle partial reads correctly.
   * TODO(user): do not ignore this test once the bug is fixed.
   */
  @Test @Override
  public void testReadPartialObject()
      throws IOException {
  }

  /**
   * Validates functionality related to getting/setting current position.
   *
   * Note:
   * WebHDFS implementation has a bug that does not handle partial reads correctly.
   * testFilePosition test contains code that performs partial reads.
   * TODO(user): do not ignore this test once the bug is fixed.
   */
  @Test @Override
  public void testFilePosition()
      throws IOException {
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because they do not make sense
  // in the context of this layer.
  // -----------------------------------------------------------------
}
