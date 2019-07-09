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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.AUTHENTICATION_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE;
import static com.google.cloud.hadoop.util.EntriesCredentialConfiguration.JSON_KEYFILE_SUFFIX;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.util.testing.TestingAccessTokenProvider;
import com.google.common.flogger.LoggerConfig;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unittests for {@link GoogleHadoopFileSystem} class. */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemTest extends GoogleHadoopFileSystemIntegrationTest {

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(GoogleHadoopFileSystemTest.class) {
        @Override
        public void before() throws Throwable {
          // Disable logging.
          LoggerConfig.getConfig("").setLevel(Level.OFF);

          ghfs = GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem();
          ghfsFileSystemDescriptor = (FileSystemDescriptor) ghfs;

          GoogleHadoopFileSystemIntegrationTest.postCreateInit();
        }

        @Override
        public void after() {
          GoogleHadoopFileSystemIntegrationTest.storageResource.after();
        }
      };

  @Test
  public void testVersionString() {
    assertThat(GoogleHadoopFileSystemBase.VERSION).isNotNull();
    assertThat(
            GoogleHadoopFileSystemBase.UNKNOWN_VERSION.equals(GoogleHadoopFileSystemBase.VERSION))
        .isFalse();
  }

  @Test
  public void lazyInitialization_succeeds_withInvalidCredentialsConfiguration() throws Exception {
    new GoogleHadoopFileSystem();
    Configuration lazyConf = new Configuration();
    lazyConf.set(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), "true");
    lazyConf.set(AUTHENTICATION_PREFIX + JSON_KEYFILE_SUFFIX, "non-existent.json");
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();

    lazyFs.initialize(new URI("gs://test-non-existent/"), lazyConf);
    lazyFs.close();
  }

  @Test
  public void lazyInitialization_deleteCall_fails_withInvalidCredentialsConfiguration()
      throws Exception {
    new GoogleHadoopFileSystem();
    Configuration lazyConf = new Configuration();
    lazyConf.set(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), "true");
    lazyConf.set(AUTHENTICATION_PREFIX + JSON_KEYFILE_SUFFIX, "non-existent.json");
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();

    lazyFs.initialize(new URI("gs://test-non-existent"), lazyConf);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> lazyFs.delete(new Path("gs://test-non-existent/dir"), false));

    assertThat(exception).hasMessageThat().isEqualTo("Failed to create GCS FS");
    assertThat(exception).hasCauseThat().isInstanceOf(FileNotFoundException.class);
    assertThat(exception)
        .hasCauseThat()
        .hasMessageThat()
        .isEqualTo("non-existent.json (No such file or directory)");

    lazyFs.close();
  }

  @Test
  public void eagerInitialization_fails_withInvalidCredentialsConfiguration() {
    new GoogleHadoopFileSystem();
    Configuration eagerConf = new Configuration();
    eagerConf.set(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), "false");
    eagerConf.set(AUTHENTICATION_PREFIX + JSON_KEYFILE_SUFFIX, "non-existent.json");
    FileSystem eagerFs = new GoogleHadoopFileSystem();

    FileNotFoundException exception =
        assertThrows(
            FileNotFoundException.class,
            () -> eagerFs.initialize(new URI("gs://test-non-existent"), eagerConf));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("non-existent.json (No such file or directory)");
  }

  // -----------------------------------------------------------------
  // Tests that exercise behavior defined in HdfsBehavior.
  // -----------------------------------------------------------------

  /** Validates {@link GoogleHadoopFileSystem#rename}. */
  @Test
  @Override
  public void testRename() throws IOException {
    renameHelper(
        new HdfsBehavior() {
          /**
           * Returns the {@link MethodOutcome} of trying to rename an existing file into the root
           * directory.
           */
          @Override
          public MethodOutcome renameFileIntoRootOutcome() {
            return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
          }
        });
  }

  @Test
  public void testCouldUseFlatGlob() throws IOException, URISyntaxException {
    Configuration lazyConf = new Configuration();
    lazyConf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), true);
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();
    assertThat(lazyFs.couldUseFlatGlob(new Path(new URI("gs://**/test/")))).isFalse();
    lazyFs.close();
  }

  @Test
  public void testTrimToPrefixWithoutGlob() {
    GoogleHadoopFileSystem lazyFs = new GoogleHadoopFileSystem();
    lazyFs.trimToPrefixWithoutGlob("gs://**/test");
    assertThat(lazyFs.trimToPrefixWithoutGlob("gs://**/test")).isEqualTo("gs://");
  }

  @Test
  public void testGetGcsPath() throws URISyntaxException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    URI gcsPath = new URI("gs://" + myghfs.getUri().getAuthority() + "/dir/obj");
    assertThat(myghfs.getGcsPath(new Path(gcsPath))).isEqualTo(gcsPath);
  }

  @Test
  public void testGetDefaultPortIndicatesPortsAreNotUsed() throws Exception {
    Configuration config = new Configuration();
    config.set("fs.gs.auth.access.token.provider.impl", TestingAccessTokenProvider.class.getName());
    URI gsUri = new URI("gs://foobar/");

    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(gsUri, config);

    assertThat(ghfs.getDefaultPort()).isEqualTo(-1);
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because their behavior differs
  // from the base class.
  // TODO(user): Add tests for subtleties of how global-rooted
  // initialization differs from bucket-rooted initialization.
  // -----------------------------------------------------------------
  @Override
  public void testInitializeSuccess() {}

  @Override
  public void testInitializeSucceedsWhenNoProjectIdConfigured() {}

  @Override
  public void testInitializeWithWorkingDirectory() {}

  @Override
  public void testIOExceptionIsThrowAfterClose() {}

  @Override
  public void testFileSystemIsRemovedFromCacheOnClose() {}

  @Override
  public void testConfigurablePermissions() {}

  @Override
  public void testFileStatusUser() {}

  @Override
  public void testCrc32cFileChecksum() {}

  @Override
  public void testMd5FileChecksum() {}
}
