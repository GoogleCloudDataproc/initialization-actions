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
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;

/**
 * Helper class to create an in-memory GHFS from an in-memory GcsFs instance for testing only.
 */
public class InMemoryGoogleHadoopFileSystem
    extends GoogleHadoopFileSystem {
  // The in-memory GcsFs instance to be used.
  private static GoogleCloudStorageFileSystem inMemoryGcsFs = createUnderlyingStorage();

  /**
   * Default constructor. Should only be called after createUnderlyingStorage has been called.
   *
   * @throws IOException on IO Error.
   */
  public InMemoryGoogleHadoopFileSystem()
      throws IOException {
    super(inMemoryGcsFs);
    initialize();
  }

  /**
   * Sets the GcsFs to be used. This should be called before an instance is constructed in order to
   * create a GCS storage over which to run the in-memory GHFS.
   */
  private static synchronized GoogleCloudStorageFileSystem createUnderlyingStorage() {
    if (inMemoryGcsFs != null) {
      return inMemoryGcsFs;
    }
    try {
      return new GoogleCloudStorageFileSystem(new InMemoryGoogleCloudStorage());
    } catch (IOException e) {
      throw new RuntimeException("Could not initialize in-memory Google Cloud Storage.", e);
    }
  }

  /**
   * Returns a sample Hadoop job configuration to be used for testing.
   *
   * @return a sample Hadoop Configuration for in-memory GHFS.
   * @throws IOException on IO Error.
   */
  public static Configuration getSampleConfiguration()
      throws IOException {
    // Set the Hadoop job configuration.
    Configuration config = new Configuration();
    config.set("fs.gs.impl", InMemoryGoogleHadoopFileSystem.class.getName());
    String systemBucketName = "test-system-bucket";
    config.set(GoogleHadoopFileSystemConfiguration.GCS_SYSTEM_BUCKET.getKey(), systemBucketName);
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCS_CREATE_SYSTEM_BUCKET.getKey(), true);
    return config;
  }

  /**
   * Initializes this in-memory GHFS for testing.
   *
   * @throws IOException on IO Error.
   */
  private void initialize()
      throws IOException {
    // Initialize the GoogleHadoopFileSystemBase.
    URI initUri;
    try {
      initUri = new URI("gs://test_bucket/");
    } catch (URISyntaxException e) {
      this.close();
      throw new IllegalArgumentException(e);
    }
    this.initialize(initUri, getSampleConfiguration());
  }
}
