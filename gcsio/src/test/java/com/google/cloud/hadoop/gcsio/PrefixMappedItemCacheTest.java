/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.PerformanceCachingGoogleCloudStorageTest.createObjectItemInfo;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.base.Ticker;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PrefixMappedItemCacheTest {
  // Sample bucket names.
  private static final String BUCKET_A = "alpha";

  // Sample object names.
  private static final String PREFIX_A = "bar";
  private static final String PREFIX_AA = "bar/apple";

  // Sample item info.
  private static final GoogleCloudStorageItemInfo ITEM_A_A =
      createObjectItemInfo(BUCKET_A, PREFIX_A);
  private static final GoogleCloudStorageItemInfo ITEM_A_AA =
      createObjectItemInfo(BUCKET_A, PREFIX_AA);

  /** Ticker implementation for testing the cache. */
  private TestTicker ticker;
  /** Instance of the cache being tested. */
  private PrefixMappedItemCache cache;

  @Before
  public void setUp() {
    ticker = new TestTicker();
    cache = new PrefixMappedItemCache(ticker, Duration.ofMillis(10));
  }

  /** Test items can be retrieved normally. */
  @Test
  public void testGetItemNormal() {
    cache.putItem(ITEM_A_A);

    GoogleCloudStorageItemInfo actualItem = cache.getItem(ITEM_A_A.getResourceId());

    assertThat(actualItem).isEqualTo(ITEM_A_A);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactly(ITEM_A_A);
  }

  /** Test missing items cannot be retrieved. */
  @Test
  public void testGetItemMissing() {
    GoogleCloudStorageItemInfo actualItem = cache.getItem(ITEM_A_A.getResourceId());

    assertThat(actualItem).isNull();
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).isEmpty();
  }

  /** Test items can be inserted normally. */
  @Test
  public void testPutItemNormal() {
    GoogleCloudStorageItemInfo previousItem = cache.putItem(ITEM_A_A);

    assertThat(previousItem).isNull();
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactly(ITEM_A_A);
  }

  /** Test old but valid items are overwritten and the old value is returned. */
  @Test
  public void testPutItemOverwrite() {
    cache.putItem(ITEM_A_A);

    GoogleCloudStorageItemInfo previousItem = cache.putItem(ITEM_A_A);

    assertThat(previousItem).isEqualTo(ITEM_A_A);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).containsExactly(ITEM_A_A);
  }

  /** Test items can be removed. */
  @Test
  public void testRemoveItemNormal() {
    cache.putItem(ITEM_A_A);

    GoogleCloudStorageItemInfo actualItem = cache.removeItem(ITEM_A_A.getResourceId());

    assertThat(actualItem).isEqualTo(ITEM_A_A);
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).isEmpty();
  }

  /** Test missing items cannot be removed. */
  @Test
  public void testRemoveItemMissing() {
    GoogleCloudStorageItemInfo actualItem = cache.removeItem(ITEM_A_AA.getResourceId());

    assertThat(actualItem).isNull();
    // Verify the state of the cache.
    assertThat(cache.getAllItemsRaw()).isEmpty();
  }

  /** Ticker with a manual time value used for testing the cache. */
  private static class TestTicker extends Ticker {

    private long time;

    @Override
    public long read() {
      return time;
    }

    public void setTimeMillis(long millis) {
      time = TimeUnit.NANOSECONDS.convert(millis, TimeUnit.MILLISECONDS);
    }
  }
}
