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

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Base unittests inherited from GoogleCloudStorageTest for CacheSupplementedGoogleCloudStorage.
 */
@RunWith(Parameterized.class)
public class CacheSupplementedGoogleCloudStorageInheritedTest
    extends GoogleCloudStorageTest {
  @Parameters
  public static Collection<Object[]> getConstructorArguments() throws IOException {
    return Arrays.asList(new Object[][]{
        {DirectoryListCache.Type.FILESYSTEM_BACKED},
        {DirectoryListCache.Type.IN_MEMORY}
    });
  }

  @Rule
  public TemporaryFolder tempDirectoryProvider = new TemporaryFolder();

  private final DirectoryListCache.Type cacheType;

  // The File corresponding to the temporary basePath of the testInstance if cacheType is
  // FILESYSTEM_BACKED.
  private File basePathFile = null;

  /**
   * Should be populated with our Parameterized runner via getConstructorArguments(). Don't
   * create actual instances here, just store the parameters, so that at runtime callers
   * may call createTestInstance() as much as desired for fresh test instances.
   */
  public CacheSupplementedGoogleCloudStorageInheritedTest(DirectoryListCache.Type cacheType) {
    this.cacheType = cacheType;
  }

  /**
   * Simply overriding the createTestInstance() method of the base class runs all the unittests in
   * GoogleCloudStorageTest against a CacheSupplementedGoogleCloudStorage instance. For extended
   * tests of specific CacheSupplementedGoogleCloudStorage functionality, we will create a different
   * instance based on a simple mock GoogleCloudStorage.
   */
  @Override
  protected GoogleCloudStorage createTestInstance(
      GoogleCloudStorageOptions options) {
    GoogleCloudStorage delegate = super.createTestInstance(options);

    DirectoryListCache resourceCache = null;
    switch (cacheType) {
      case IN_MEMORY: {
        resourceCache = new InMemoryDirectoryListCache();
        break;
      }
      case FILESYSTEM_BACKED: {
        try {
          String folderName = options.isAutoRepairImplicitDirectoriesEnabled()
              ? "gcs_metadata" : "gcs_metadata_no_auto";
          basePathFile = tempDirectoryProvider.newFolder(folderName);
          resourceCache =
              new FileSystemBackedDirectoryListCache(basePathFile.toString());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        break;
      }
      default:
        throw new RuntimeException(String.format(
            "Invalid DirectoryListCache.Type: '%s'", cacheType));
    }

    CacheSupplementedGoogleCloudStorage cacheSupplemented =
        new CacheSupplementedGoogleCloudStorage(delegate, resourceCache);
    return cacheSupplemented;
  }
}
