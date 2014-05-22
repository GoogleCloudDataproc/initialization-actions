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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.hadoop.util.LogUtil;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for CacheSupplementedGoogleCloudStorage class.
 */
@RunWith(JUnit4.class)
public abstract class CacheSupplementedGoogleCloudStorageIntegrationTest
    extends GoogleCloudStorageIntegrationTest {
  // Logger.
  protected static LogUtil log = new LogUtil(
      CacheSupplementedGoogleCloudStorageIntegrationTest.class);

  // If set as an environment variable, this test will run without wrapping the raw internal
  // GoogleCloudStorage into a CacheSupplementedGoogleCloudStorage.
  private static final String DISABLE_CACHE_FOR_TEST_ENV = "DISABLE_CACHE_FOR_TEST";

  @BeforeClass
  public static void beforeAllTests()
      throws IOException {
    GoogleCloudStorageIntegrationTest.beforeAllTests();
    if (Boolean.valueOf(System.getenv(DISABLE_CACHE_FOR_TEST_ENV))) {
      log.warn("Disabling cache in CacheSupplementedGoogleCloudStorageIntegrationTest");
    } else {
      GoogleCloudStorageIntegrationTest.gcs = new CacheSupplementedGoogleCloudStorage(
          GoogleCloudStorageIntegrationTest.gcs);
    }
  }

  @Test @Ignore("Replace with InMemoryGoogleCloudStorage with simulated list lag")
  public void testImmediateConsistency()
      throws IOException {
    assertNotNull(bucketName);
    String testPrefix = "index-consistency-test";
    gcs.create(new StorageResourceId(bucketName, testPrefix + "/base.txt")).close();

    int numItems = 999;
    List<String> srcObjects = new ArrayList<>();
    List<String> dstObjects = new ArrayList<>();
    // Copy the same base.txt into 'numItems' different destination objects.
    for (int i = 0; i < numItems; ++i) {
      srcObjects.add(testPrefix + "/base.txt");
      dstObjects.add(testPrefix + "/foo" + i + ".txt");
    }
    gcs.copy(bucketName, srcObjects, bucketName, dstObjects);

    // If not using the CacheSupplementedGoogleCloudStorage, this is expected to fail roughly
    // 25% of the time. It should pass 100% if using CacheSupplementedGoogleCloudStorage.
    assertEquals(numItems, gcs.listObjectInfo(bucketName, testPrefix + "/foo", "/").size());
    
    // Clean up
    List<StorageResourceId> fullObjectNames = new ArrayList<>();
    for (String object : srcObjects) {
      fullObjectNames.add(new StorageResourceId(bucketName, object));
    }
    for (String object : dstObjects) {
      fullObjectNames.add(new StorageResourceId(bucketName, object));
    }
    gcs.deleteObjects(fullObjectNames);
    gcsit.clearBucket(bucketName);
  }
}
