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

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.LaggedGoogleCloudStorage.ListVisibilityCalculator;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collection;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * A base class with tests for GoogleCloudStorageFileSystem with
 * various combinations of options.
 */
@RunWith(Parameterized.class)
public class GoogleCloudStorageFileSystemOptionsUnitTest
    extends GoogleCloudStorageFileSystemOptionsTestBase {

  /* In order to run this test class multiple times with different
   * underlying GCS implementations, we define this interface to create
   * a GCS instance, then create a set of inner classes that implement
   * this interface, each of which creates a different kind of GCS.
   */
  static interface GcsCreator {
    public GoogleCloudStorage createGcs(GoogleCloudStorageOptions options);
  }

  static class InMemoryGcsCreator implements GcsCreator {
    public GoogleCloudStorage createGcs(GoogleCloudStorageOptions options) {
      return new InMemoryGoogleCloudStorage(options);
    }
  }

  static class ZeroLaggedGcsCreator implements GcsCreator {
    public GoogleCloudStorage createGcs(GoogleCloudStorageOptions options) {
      return new LaggedGoogleCloudStorage(
          new InMemoryGoogleCloudStorage(options),
          Clock.SYSTEM,
          ListVisibilityCalculator.IMMEDIATELY_VISIBLE);
    }
  }

  private GcsCreator gcsCreator;

  public GoogleCloudStorageFileSystemOptionsUnitTest(GcsCreator gcsCreator) {
    this.gcsCreator = gcsCreator;
  }

  @Parameters
  public static Collection<Object[]> getConstructorArguments() {
    return ImmutableList.of(
        new Object[] {new InMemoryGcsCreator()}, new Object[] {new ZeroLaggedGcsCreator()});
  }

  @BeforeClass
  public static void beforeAllTests()
      throws IOException {
    GoogleCloudStorageFileSystemOptionsTestBase.beforeAllTests();
  }

  /** Generate the GCSFS to be used for testing. */
  @Override
  public GoogleCloudStorageFileSystem createGcsfsWithInferDirectories(boolean inferDirectories)
      throws IOException {
    // Use the GcsOptions builder from the GcsFsOptions builder
    // so that we can get to the GcsOptions from the GcsFsOptions
    // in order to ensure we have the right value for
    // isInferImplicitDirectoriesEnabled in gcsfs.
    GoogleCloudStorageFileSystemOptions.Builder fsOptionsBuilder =
        GoogleCloudStorageFileSystemOptions.builder();
    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.builder()
            .setInferImplicitDirectoriesEnabled(inferDirectories)
            .build();
    GoogleCloudStorage gcs = this.gcsCreator.createGcs(gcsOptions);
    return new GoogleCloudStorageFileSystem(gcs, fsOptionsBuilder.build());
  }

  @Test
  public void testGcsFsInheritsGcsOptions() throws IOException {
    GoogleCloudStorageOptions gcsOptions =
        GoogleCloudStorageOptions.builder()
            .setProjectId("foo-project")
            .setAppName("foo-app")
            .build();
    GoogleCloudStorage gcs = this.gcsCreator.createGcs(gcsOptions);
    GoogleCloudStorageFileSystem gcsfs = new GoogleCloudStorageFileSystem(gcs);
    assertThat(gcsfs.getOptions().getCloudStorageOptions().getProjectId()).isEqualTo("foo-project");
    assertThat(gcsfs.getOptions().getCloudStorageOptions().getAppName()).isEqualTo("foo-app");
  }
}
