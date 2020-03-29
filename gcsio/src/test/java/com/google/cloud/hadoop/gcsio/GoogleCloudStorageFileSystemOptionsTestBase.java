/*
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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.common.collect.ObjectArrays;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A base class with tests for GoogleCloudStorageFileSystem with
 * various combinations of options.
 */
public abstract class GoogleCloudStorageFileSystemOptionsTestBase {

  // The test bucket name.
  private String testBucketName = "bucket1";

  // The test files we create.
  private String[] inputFiles = {
    "gs://" + testBucketName + "/a/b/f1.txt",
    "gs://" + testBucketName + "/a/c/f2.txt",
    "gs://" + testBucketName + "/e/f"
  };

  // The implied directories that directly contain input files.
  private String[] impliedDirs0 = {
    "gs://" + testBucketName + "/a/b",
    "gs://" + testBucketName + "/a/c",
    "gs://" + testBucketName + "/e"
  };

  private String impliedDirA = "gs://" + testBucketName + "/a";

  // The implied directories that only contain implied directories.
  private String[] impliedDirs1 = {
    impliedDirA
  };

  // All implied directories
  private String[] impliedDirs =
      ObjectArrays.concat(impliedDirs0, impliedDirs1, String.class);

  // Each test creates a gcsfs, we keep it here so we can pick it up
  // in tearDown and use it to clean up.
  private GoogleCloudStorageFileSystem gcsfs;

  @BeforeClass
  public static void beforeAllTests() throws IOException {
    // Disable logging.
    // Normally you would need to keep a strong reference to any logger used for
    // configuration, but the "root" logger is always present.
    Logger.getLogger("").setLevel(Level.OFF);
  }

  @After
  public void tearDown() throws IOException, URISyntaxException {
    if (gcsfs != null) {
      // Clean up our test files.
      gcsfs.delete(new URI("gs://" + testBucketName), true);
    }
  }

  /** Generate the GCSFS to be used for testing. */
  public abstract GoogleCloudStorageFileSystem createGcsfsWithInferDirectories(
      boolean inferDirectories) throws IOException;

  /** Ensure directory implicit directory is absent. */
  @Test
  public void testImplicitDirectory() throws IOException, URISyntaxException {
    // We need different GCSFS options for our test.
    gcsfs = createGcsfsWithInferDirectories(false);

    createTestFiles(gcsfs);
    testImpliedDirs(gcsfs);

    // The directory objects should still not exist.
    for (String dir : impliedDirs) {
      FileInfo dirInfo = gcsfs.getFileInfo(new URI(dir));
      assertWithMessage("Directory " + dir + " should not exist.").that(dirInfo.exists()).isFalse();
    }
  }

  /** With inferred directories, the directories should appear to be there. */
  @Test
  public void testInferredDirectories() throws IOException, URISyntaxException {
    // We need different GCSFS options for our test.
    gcsfs = createGcsfsWithInferDirectories(true);
    createTestFiles(gcsfs);

    // The directory objects should exist (as inferred directories).
    for (String dir : impliedDirs) {
      FileInfo dirInfo = gcsfs.getFileInfo(new URI(dir));
      assertWithMessage("Directory " + dir + " should exist (inferred)")
          .that(dirInfo.exists())
          .isTrue();
      assertWithMessage("Creation time on inferred directory " + dir + " should be zero.")
          .that(dirInfo.getCreationTime())
          .isEqualTo(0);
    }

    String dir = impliedDirA;
    List<FileInfo> subInfo = gcsfs.listFileInfo(new URI(dir));
    assertWithMessage("Implied directory " + dir + " should have 2 children")
        .that(subInfo.size())
        .isEqualTo(2);
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
      assertThat(fileInfo.exists()).isTrue();
    }
  }

  private void testImpliedDirs(GoogleCloudStorageFileSystem gcsfs)
      throws IOException, URISyntaxException {
    // We created our objects directly in GCS, so the implied directories
    // should not exist.
    for (String dir : impliedDirs) {
      FileInfo dirInfo = gcsfs.getFileInfo(new URI(dir));
      assertWithMessage("Implied directory " + dir + " should not exist.")
          .that(dirInfo.exists())
          .isFalse();
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
    StorageResourceId id = StorageResourceId.fromUriPath(new URI(path), false);
    gcs.createEmptyObject(id);
  }
}
