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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hadoop.gcsio.MethodOutcome;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unittests for GoogleHadoopFileSystem class.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemTest
    extends GoogleHadoopFileSystemIntegrationTest {

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(GoogleHadoopFileSystemTest.class) {
        @Override
        public void before() throws Throwable {
          // Disable logging.
          Logger.getRootLogger().setLevel(Level.OFF);

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

  // -----------------------------------------------------------------
  // Tests that exercise behavior defined in HdfsBehavior.
  // -----------------------------------------------------------------

  /**
   * Validates rename().
   */
  @Test @Override
  public void testRename()
      throws IOException {
    renameHelper(new HdfsBehavior() {
        /**
         * Returns the MethodOutcome of trying to rename an existing file into the root directory.
         */
        @Override
        public MethodOutcome renameFileIntoRootOutcome() {
          return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
        }
      });
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because their behavior differs
  // from the base class.
  // TODO(user): Add tests for subtleties of how global-rooted
  // initialization differs from bucket-rooted initialization.
  // -----------------------------------------------------------------
  @Test @Override
  public void testInitializeSuccess()
      throws IOException, URISyntaxException {
  }

  @Test
  @Override
  public void testInitializeSucceedsWhenNoProjectIdConfigured()
      throws IOException, URISyntaxException {}

  @Test @Override
  public void testInitializeWithWorkingDirectory()
      throws IOException, URISyntaxException {
  }

  @Test @Override
  public void testIOExceptionIsThrowAfterClose()
      throws IOException, URISyntaxException {
  }

  @Test @Override
  public void testFileSystemIsRemovedFromCacheOnClose()
      throws IOException, URISyntaxException {
  }

  @Test @Override
  public void testConfigurablePermissions()
      throws IOException {
  }
}
