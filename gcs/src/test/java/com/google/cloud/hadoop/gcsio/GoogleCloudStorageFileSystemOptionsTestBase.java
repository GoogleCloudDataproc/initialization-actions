/**
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the * License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A base class with tests for GoogleCloudStorageFileSystem with
 * various combinations of options.
 */
public abstract class GoogleCloudStorageFileSystemOptionsTestBase {

  // The test bucket name.
  private String testBucketName = "bucket1";

  // The test files we create.
  private String[] inputFiles = {
    "gs://" + testBucketName + "/a/b/c.txt",
    "gs://" + testBucketName + "/a/b/d.txt",
    "gs://" + testBucketName + "/e/f"
  };

  // The directories for all of the inputFiles.
  private String[] impliedDirs = {
    "gs://" + testBucketName + "/a/b",
    "gs://" + testBucketName + "/e"
  };

  // Each test creates a gcsfs, we keep it here so we can pick it up
  // in tearDown and use it to clean up.
  private GoogleCloudStorageFileSystem gcsfs;

  @BeforeClass
  public static void beforeAllTests()
      throws IOException {
    // Disable logging.
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  @After
  public void tearDown() throws IOException, URISyntaxException {
    // Clean up our test files.
    gcsfs.delete(new URI("gs://" + testBucketName), true);
  }

  /**
   * Generate the GCSFS to be used for testing.
   */
  public abstract GoogleCloudStorageFileSystem
      createGcsfsWithAutoRepairWithInferDirectories(
          boolean autoRepair, boolean inferDirectories)
      throws IOException;

  /**
   * Test auto-repair of directories.
   */
  @Test
  public void testAutoRepairEnabled() throws IOException, URISyntaxException {
    // We need different GCSFS options for our test.
    gcsfs = createGcsfsWithAutoRepairWithInferDirectories(true, false);

    createTestFiles(gcsfs);
    testAndPossiblyRepairDirs(gcsfs);

    // The directory objects should now exist.
    for (String dir : impliedDirs) {
      FileInfo dirInfo = gcsfs.getFileInfo(new URI(dir));
      Assert.assertTrue(
          "Directory should exist after repair.",
          dirInfo.exists());
      Assert.assertTrue(
          "Creation time on repaired directory should be non-zero.",
          dirInfo.getCreationTime() > 0);
    }
  }

  /**
   * Ensure directory auto-repair can be disabled.
   */
  @Test
  public void testAutoRepairDisabled() throws IOException, URISyntaxException {
    // We need different GCSFS options for our test.
    gcsfs = createGcsfsWithAutoRepairWithInferDirectories(false, false);

    createTestFiles(gcsfs);
    // Since we set autoRepair=false, no repair should happen.
    testAndPossiblyRepairDirs(gcsfs);

    // The directory objects should still not exist.
    for (String dir : impliedDirs) {
      FileInfo dirInfo = gcsfs.getFileInfo(new URI(dir));
      Assert.assertFalse(
          "Directory should not exist after (non-)repair.",
          dirInfo.exists());
    }
  }

  /**
   * With no auto-repair but with inferred directories,
   * the directories should appear to be there.
   */
  @Test
  public void testInferredDirectories() throws IOException, URISyntaxException {
    // We need different GCSFS options for our test.
    gcsfs = createGcsfsWithAutoRepairWithInferDirectories(false, true);
    createTestFiles(gcsfs);

    // The directory objects should exist (as inferred directories).
    for (String dir : impliedDirs) {
      FileInfo dirInfo = gcsfs.getFileInfo(new URI(dir));
      Assert.assertTrue(
          "Directory should exist (inferred)",
          dirInfo.exists());
      Assert.assertEquals(
          "Creation time on inferred directory should be zero.",
          0, dirInfo.getCreationTime());
    }
  }

  private void createTestFiles(GoogleCloudStorageFileSystem gcsfs)
      throws IOException, URISyntaxException {
    GoogleCloudStorage gcs = gcsfs.getGcs();
    createBucket(gcs, testBucketName);
    for (String inputFile : inputFiles) {
      createEmptyFile(gcs, inputFile);
    }

    // Make sure the files we just created exist
    for (String inputFile : inputFiles) {
      FileInfo fileInfo = gcsfs.getFileInfo(new URI(inputFile));
      Assert.assertTrue(fileInfo.exists());
    }
  }

  private void testAndPossiblyRepairDirs(
      GoogleCloudStorageFileSystem gcsfs)
      throws IOException, URISyntaxException {
    // We created our objects directly in GCS, so the implied directories
    // should not exist.
    for (String dir : impliedDirs) {
      FileInfo dirInfo = gcsfs.getFileInfo(new URI(dir));
      Assert.assertFalse(
          "Implied directory should not exist.",
          dirInfo.exists());
    }

    // List each directory so that auto-repair kicks in.
    for (String inputFile : inputFiles) {
      URI parentPathUri =
          GoogleCloudStorageFileSystem.getParentPath(new URI(inputFile));
      gcsfs.repairPossibleImplicitDirectory(parentPathUri);
    }
  }

  private void createBucket(
      GoogleCloudStorage gcs, String bucketName)
      throws IOException {
    gcs.create(bucketName);
  }

  private void createEmptyFile(
      GoogleCloudStorage gcs, String path)
      throws IOException, URISyntaxException {
    StorageResourceId id =
        GoogleCloudStorageFileSystem.validatePathAndGetId(new URI(path), false);
    gcs.createEmptyObject(id);
  }
}
