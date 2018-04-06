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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.LaggedGoogleCloudStorage.ListVisibilityCalculator;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.Arrays;
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
  public static Collection<Object[]> getConstructorArguments()
      throws IOException {
    return Arrays.asList(new Object[][]{
        {new InMemoryGcsCreator()},
        {new ZeroLaggedGcsCreator()},
        // {new CachedLaggedGcsCreator()},
        // TODO(user): The above test fails when we run :UnitTests,
        // but succeeds when we run :UnitTests with a filter set to
        // this class name, so there must be some kind of interaction
        // between this test and other tests in :UnitTests.
        // Need to track this down.

        // We don't test with a file-backed cache, because it creates
        // its cache files on disk, which requires
        // that parent directories be created, so the test fails when
        // autoRepair=false and we expect there not to be any
        // intermediate directories.
    });
  }

  @BeforeClass
  public static void beforeAllTests()
      throws IOException {
    GoogleCloudStorageFileSystemOptionsTestBase.beforeAllTests();
  }

  /**
   * Generate the GCSFS to be used for testing.
   */
  @Override
  public GoogleCloudStorageFileSystem
      createGcsfsWithAutoRepairWithInferDirectories(
      boolean autoRepairEnabled, boolean inferDirectories)
      throws IOException {
    // Use the GcsOptions builder from the GcsFsOptions builder
    // so that we can get to the GcsOptions from the GcsFsOptions
    // in order to ensure we have the right value for
    // isInferImplicitDirectoriesEnabled in gcsfs.
    GoogleCloudStorageFileSystemOptions.Builder fsOptionsBuilder =
        GoogleCloudStorageFileSystemOptions.newBuilder();
            //.setShouldIncludeInTimestampUpdatesPredicate(
                //INCLUDE_SUBSTRINGS_PREDICATE)
    GoogleCloudStorageOptions.Builder gcsOptionsBuilder =
        fsOptionsBuilder.getCloudStorageOptionsBuilder();
    GoogleCloudStorageOptions gcsOptions = gcsOptionsBuilder
        .setAutoRepairImplicitDirectoriesEnabled(autoRepairEnabled)
        .setInferImplicitDirectoriesEnabled(inferDirectories)
        .build();
    GoogleCloudStorage gcs = this.gcsCreator.createGcs(gcsOptions);
    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem(gcs, fsOptionsBuilder.build());
    gcsfs.setUpdateTimestampsExecutor(MoreExecutors.newDirectExecutorService());
    return gcsfs;
  }

  @Test
  public void testLazyEvaluationOfGoogleCloudStorageOptionsBuilder() {
    GoogleCloudStorageOptions.Builder innerBuilder = GoogleCloudStorageOptions.newBuilder()
        .setProjectId("foo-project");
    GoogleCloudStorageFileSystemOptions.Builder builder =
        GoogleCloudStorageFileSystemOptions.newBuilder()
            .setCloudStorageOptionsBuilder(innerBuilder);
    innerBuilder.setProjectId("bar-project");
    assertThat(builder.build().getCloudStorageOptions().getProjectId()).isEqualTo("bar-project");
  }

  @Test
  public void testOverrideInnerBuilderWithImmutableOptions() {
    GoogleCloudStorageOptions.Builder innerBuilder = GoogleCloudStorageOptions.newBuilder()
        .setProjectId("foo-project");
    GoogleCloudStorageFileSystemOptions.Builder builder =
        GoogleCloudStorageFileSystemOptions.newBuilder()
            .setCloudStorageOptionsBuilder(innerBuilder)
            .setImmutableCloudStorageOptions(GoogleCloudStorageOptions.newBuilder()
                .setProjectId("bar-project")
                .build());
    assertThat(builder.build().getCloudStorageOptions().getProjectId()).isEqualTo("bar-project");
  }

  @Test
  public void testOverrideImmutableOptionsWithInnerBuilder() {
    GoogleCloudStorageOptions.Builder innerBuilder = GoogleCloudStorageOptions.newBuilder()
        .setProjectId("foo-project");
    GoogleCloudStorageFileSystemOptions.Builder builder =
        GoogleCloudStorageFileSystemOptions.newBuilder()
            .setImmutableCloudStorageOptions(GoogleCloudStorageOptions.newBuilder()
                .setProjectId("bar-project")
                .build())
            .setCloudStorageOptionsBuilder(innerBuilder);
    assertThat(builder.build().getCloudStorageOptions().getProjectId()).isEqualTo("foo-project");
  }

  @Test
  public void testUnsetImmutableOptionsBuilderRevertsToDefaults() {
    GoogleCloudStorageOptions.Builder innerBuilder = GoogleCloudStorageOptions.newBuilder()
        .setProjectId("foo-project");
    GoogleCloudStorageFileSystemOptions.Builder builder =
        GoogleCloudStorageFileSystemOptions.newBuilder()
            .setImmutableCloudStorageOptions(GoogleCloudStorageOptions.newBuilder()
                .setProjectId("bar-project")
                .build())
            .setCloudStorageOptionsBuilder(innerBuilder)
            .setImmutableCloudStorageOptions(null);
    assertThat(builder.build().getCloudStorageOptions().getProjectId()).isNull();
  }

  @Test
  public void testGcsFsInheritsGcsOptions() throws IOException {
    GoogleCloudStorageOptions gcsOptions = GoogleCloudStorageOptions.newBuilder()
        .setProjectId("foo-project")
        .setAppName("foo-app")
        .build();
    GoogleCloudStorage gcs = this.gcsCreator.createGcs(gcsOptions);
    GoogleCloudStorageFileSystem gcsfs = new GoogleCloudStorageFileSystem(gcs);
    assertThat(gcsfs.getOptions().getCloudStorageOptions().getProjectId()).isEqualTo("foo-project");
    assertThat(gcsfs.getOptions().getCloudStorageOptions().getAppName()).isEqualTo("foo-app");
  }
}
