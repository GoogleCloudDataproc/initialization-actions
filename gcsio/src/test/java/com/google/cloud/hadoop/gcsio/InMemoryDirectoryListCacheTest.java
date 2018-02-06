/**
 * Copyright 2014 Google Inc. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * UnitTests for InMemoryDirectoryListCache class.
 */
@RunWith(JUnit4.class)
public class InMemoryDirectoryListCacheTest extends DirectoryListCacheTest {

  @Override
  protected DirectoryListCache getTestInstance() {
    DirectoryListCache cache = new InMemoryDirectoryListCache();
    cache.getMutableConfig()
        .setMaxEntryAgeMillis(MAX_ENTRY_AGE)
        .setMaxInfoAgeMillis(MAX_INFO_AGE);
    return cache;
  }

  @Test
  public void testGetInstance() {
    assertThat(InMemoryDirectoryListCache.getInstance()).isNotNull();
  }

  /**
   * As a consequence of the implementation holding actual listings per-bucket, removing a bucket
   * will also remove all the objects within that bucket.
   */
  @Test
  public void testRemoveNonEmptyBucket() throws IOException {
    CacheEntry objectEntry = cache.putResourceId(objectResourceId);
    assertThat(cache.getInternalNumBuckets()).isEqualTo(1);
    assertThat(cache.getInternalNumObjects()).isEqualTo(1);
    assertThat(cache.getBucketList()).hasSize(1);
    assertThat(cache.getObjectList(BUCKET_NAME, "", null, null)).hasSize(1);

    // Removing the auto-created bucket will auto-remove all its children objects as well.
    cache.removeResourceId(bucketResourceId);
    assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
    assertThat(cache.getInternalNumObjects()).isEqualTo(0);
    assertThat(cache.getBucketList()).isEmpty();
    assertThat(cache.getObjectList(BUCKET_NAME, "", null, null)).isNull();
  }
}
