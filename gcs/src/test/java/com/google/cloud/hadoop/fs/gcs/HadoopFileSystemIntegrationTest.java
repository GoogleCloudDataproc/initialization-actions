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

import com.google.common.base.Strings;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for HDFS.
 *
 * This class allows running all tests in HadoopFileSystemTestBase against
 * HDFS. This allows us to determine if HDFS behavior is different from GHFS behavior and if so, fix
 * GHFS to match HDFS behavior.
 *
 * We enable it by mapping paths used by GHFS tests to HDFS paths.
 * GHFS tests construct test paths using the following 2 methods:
 * -- combine bucketName and objectName to form GCS path.
 * -- directly use GCS path (in some cases).
 *
 * This class overrides the initial setup of the FileSystem under test to inject an actual
 * HDFS implementation, as well as injecting a version of FileSystemDescriptor which properly
 * describes the behavior of HDFS. The FileSystemDescriptor thus reroutes all the test methods
 * through the proper HDFS instance using hdfs:/ paths.
 */
@RunWith(JUnit4.class)
public class HadoopFileSystemIntegrationTest
    extends HadoopFileSystemTestBase {

  // Environment variable from which to get HDFS access info.
  public static final String HDFS_ROOT = "HDFS_ROOT";

  // HDFS path (passed to the test through environment var).
  static String hdfsRoot;

  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(HadoopFileSystemIntegrationTest.class) {
        /** Performs initialization once before tests are run. */
        @Override
        public void before() throws Throwable {
          // Get info about the HDFS instance against which we run tests.
          hdfsRoot = System.getenv(HDFS_ROOT);

          if (Strings.isNullOrEmpty(hdfsRoot)) {
            hdfsRoot = "file://" + folder.newFolder("hdfs_root").getAbsolutePath();
          }

          // Create a FileSystem instance to access the given HDFS.
          URI hdfsUri = null;
          try {
            hdfsUri = new URI(hdfsRoot);
          } catch (URISyntaxException e) {
            throw new AssertionError("Invalid HDFS path: " + hdfsRoot, e);
          }
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
          ghfsHelper.setIgnoreStatistics(); // Multi-threaded code screws us up.
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
    renameHelper(new HdfsBehavior());
  }
}
