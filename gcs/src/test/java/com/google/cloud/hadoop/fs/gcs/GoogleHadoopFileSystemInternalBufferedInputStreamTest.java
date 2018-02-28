/**
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unittest inheriting from GoogleHadoopFileSystemTest which provides coverage for the
 * GoogleHadoopFSInputStream when fs.gs.inputstream.internalbuffer.enable is true.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemInternalBufferedInputStreamTest
    extends GoogleHadoopFileSystemTest {
  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(
          GoogleHadoopFileSystemInternalBufferedInputStreamTest.class) {
        @Override
        public void before() throws Throwable {
          GoogleHadoopFileSystemTest.storageResource.before();
          ghfs.getConf()
              .setBoolean(
                  GoogleHadoopFileSystemBase.GCS_INPUTSTREAM_INTERNALBUFFER_ENABLE_KEY, true);
        }

        @Override
        public void after() {
          GoogleHadoopFileSystemTest.storageResource.after();
        }
      };

  // -----------------------------------------------------------------------------------------
  // Tests that are expensive and not specific to the input stream; they're already covered
  // in the base test class.
  // -----------------------------------------------------------------------------------------

  @Test @Override
  public void testRepairImplicitDirectory() {
  }

  @Test @Override
  public void testDelete() {
  }

  @Test @Override
  public void testRenameWithContentChecking() {
  }

  @Test @Override
  public void testRename() {
  }

  @Test @Override
  public void testListObjectNamesAndGetItemInfo() {
  }

  @Test @Override
  public void testGlobStatus() {
  }

  @Test @Override
  public void testMkdirs() {
  }

  @Test @Override
  public void testInitializeWithWorkingDirectory() {
  }

  @Test @Override
  public void provideCoverageForUnmodifiedMethods() {
  }
}
