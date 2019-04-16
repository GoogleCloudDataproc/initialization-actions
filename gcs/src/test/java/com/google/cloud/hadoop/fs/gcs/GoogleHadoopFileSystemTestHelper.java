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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * GoogleHadoopFileSystemTestHelper contains helper methods and factory methods for setting up the
 * test instances used for various unit and integration tests.
 */
public class GoogleHadoopFileSystemTestHelper {

  public static final String IN_MEMORY_TEST_BUCKET = "gs://fake-in-memory-test-bucket";

  /**
   * Creates an instance of a bucket-rooted GoogleHadoopFileSystemBase using an in-memory underlying
   * store.
   */
  public static GoogleHadoopFileSystem createInMemoryGoogleHadoopFileSystem() throws IOException {
    GoogleCloudStorageOptions.Builder gcsOptionsBuilder =
        defaultStorageOptionsBuilder();
    GoogleCloudStorageFileSystemOptions.Builder fsOptionsBuilder =
        GoogleCloudStorageFileSystemOptions.newBuilder()
        .setCloudStorageOptionsBuilder(gcsOptionsBuilder);
    GoogleCloudStorageFileSystem memoryGcsFs = new GoogleCloudStorageFileSystem(
        new InMemoryGoogleCloudStorage(gcsOptionsBuilder.build()),
        fsOptionsBuilder.build());
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem(memoryGcsFs);
    initializeInMemoryFileSystem(ghfs, IN_MEMORY_TEST_BUCKET);
    return ghfs;
  }

  /**
   * Get the options we want to use for our GCS instances.
   */
  public static GoogleCloudStorageOptions.Builder
      defaultStorageOptionsBuilder() {
    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleCloudStorageOptions.newBuilder();
    return optionsBuilder;
  }

  /**
   * Helper for plumbing through an initUri and creating the proper Configuration object.
   * Calls FileSystem.initialize on {@code ghfs}.
   */
  private static void initializeInMemoryFileSystem(FileSystem ghfs, String initUriString)
      throws IOException {
    URI initUri;
    try {
      initUri = new URI(initUriString);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    Configuration config = new Configuration();
    ghfs.initialize(initUri, config);
    // Create test bucket
    ghfs.mkdirs(new Path(initUriString));
  }
}
