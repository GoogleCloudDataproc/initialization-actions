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

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unittests for GoogleHadoopFileSystemBase class.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopGlobalRootedFileSystemTest
    extends GoogleHadoopGlobalRootedFileSystemIntegrationTest {

  @BeforeClass
  public static void beforeAllTests()
      throws IOException {
    // Disable logging.
    Logger.getRootLogger().setLevel(Level.OFF);

    ghfs = GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopGlobalRootedFileSystem();
    ghfsFileSystemDescriptor = (FileSystemDescriptor) ghfs;

    GoogleHadoopGlobalRootedFileSystemIntegrationTest.postCreateInit();
  }

  @AfterClass
  public static void afterAllTests()
      throws IOException {
    GoogleHadoopGlobalRootedFileSystemIntegrationTest.afterAllTests();
  }

  // -----------------------------------------------------------------
  // Inherited tests that we suppress because they do not make sense
  // in the context of this layer.
  // -----------------------------------------------------------------
  @Test @Override
  public void testInitializeSuccess()
      throws IOException, URISyntaxException {
  }
}
