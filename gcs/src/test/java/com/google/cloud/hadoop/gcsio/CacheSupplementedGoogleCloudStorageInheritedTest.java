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

package com.google.cloud.hadoop.gcsio;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Base unittests inherited from GoogleCloudStorageTest for CacheSupplementedGoogleCloudStorage.
 */
@RunWith(JUnit4.class)
public class CacheSupplementedGoogleCloudStorageInheritedTest
    extends GoogleCloudStorageTest {
  /**
   * Simply overriding the createTestInstance() method of the base class runs all the unittests in
   * GoogleCloudStorageTest against a CacheSupplementedGoogleCloudStorage instance. For extended
   * tests of specific CacheSupplementedGoogleCloudStorage functionality, we will create a different
   * instance based on a simple mock GoogleCloudStorage.
   */
  @Override
  protected GoogleCloudStorage createTestInstance() {
    GoogleCloudStorage delegate = super.createTestInstance();
    CacheSupplementedGoogleCloudStorage cacheSupplemented =
        new CacheSupplementedGoogleCloudStorage(delegate);
    cacheSupplemented.setResourceCache(new DirectoryListCache());
    return cacheSupplemented;
  }
}
