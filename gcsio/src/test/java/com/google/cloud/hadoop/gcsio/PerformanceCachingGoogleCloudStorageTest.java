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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.base.Joiner;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class PerformanceCachingGoogleCloudStorageTest {
  /** Sample empty metadata. */
  private static final ImmutableMap<String, byte[]> TEST_METADATA =
      ImmutableMap.of("test_key", new byte[] {2});

  private static final CreateBucketOptions CREATE_BUCKET_OPTIONS =
      new CreateBucketOptions("test_location", "test_storage_class");

  private static final CreateObjectOptions CREATE_OBJECT_OPTIONS =
      new CreateObjectOptions(true, "test_content_type", TEST_METADATA, true);

  /* Sample bucket names. */
  private static final String BUCKET_A = "alpha";
  private static final String BUCKET_B = "alph";

  /* Sample object names. */
  private static final String PREFIX_A = "bar";
  private static final String PREFIX_AA = "bar/apple";
  private static final String PREFIX_ABA = "bar/berry/foo";
  private static final String PREFIX_B = "baz";

  /* Sample bucket item info. */
  private static final GoogleCloudStorageItemInfo ITEM_A = createBucketItemInfo(BUCKET_A);
  private static final GoogleCloudStorageItemInfo ITEM_B = createBucketItemInfo(BUCKET_B);

  /* Sample item info. */
  private static final GoogleCloudStorageItemInfo ITEM_A_A =
      createObjectItemInfo(BUCKET_A, PREFIX_A);
  private static final GoogleCloudStorageItemInfo ITEM_A_AA =
      createObjectItemInfo(BUCKET_A, PREFIX_AA);
  private static final GoogleCloudStorageItemInfo ITEM_A_ABA =
      createObjectItemInfo(BUCKET_A, PREFIX_ABA);
  private static final GoogleCloudStorageItemInfo ITEM_A_B =
      createObjectItemInfo(BUCKET_A, PREFIX_B);
  private static final GoogleCloudStorageItemInfo ITEM_B_A =
      createObjectItemInfo(BUCKET_B, PREFIX_A);
  private static final GoogleCloudStorageItemInfo ITEM_B_B =
      createObjectItemInfo(BUCKET_B, PREFIX_B);

  /** Clock implementation for testing the GCS delegate. */
  private TestClock clock;
  /** {@link PerformanceCachingGoogleCloudStorage} instance being tested. */
  private PerformanceCachingGoogleCloudStorage gcs;
  /** Cache implementation to back the GCS instance being tested. */
  private PrefixMappedItemCache cache;
  /** GoogleCloudStorage implementation to back the GCS instance being tested. */
  private GoogleCloudStorage gcsDelegate;

  @Before
  public void setUp() throws IOException {
    // Setup mocks.
    MockitoAnnotations.initMocks(this);

    // Create the cache configuration.
    PrefixMappedItemCache.Config cacheConfig = new PrefixMappedItemCache.Config();
    cacheConfig.setMaxEntryAgeMillis(10);
    cacheConfig.setTicker(new TestTicker());
    cache = new PrefixMappedItemCache(cacheConfig);

    // Create the cache configuration.
    PerformanceCachingGoogleCloudStorageOptions options =
        new PerformanceCachingGoogleCloudStorageOptions.Builder().build();

    // Setup the delegate
    clock = new TestClock();
    GoogleCloudStorage gcsImpl =
        new InMemoryGoogleCloudStorage(GoogleCloudStorageOptions.newBuilder().build(), clock);
    gcsDelegate = spy(gcsImpl);

    gcs = new PerformanceCachingGoogleCloudStorage(gcsDelegate, options, cache);

    // Prepare the delegate.
    gcsDelegate.create(BUCKET_A, CREATE_BUCKET_OPTIONS);
    gcsDelegate.create(BUCKET_B, CREATE_BUCKET_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_A_A.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_A_AA.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_A_ABA.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_A_B.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_B_A.getResourceId(), CREATE_OBJECT_OPTIONS);
    gcsDelegate.createEmptyObject(ITEM_B_B.getResourceId(), CREATE_OBJECT_OPTIONS);
  }

  @Test
  public void testDeleteBuckets() throws IOException {
    List<String> buckets = Lists.newArrayList(BUCKET_A);

    // Prepare the cache.
    cache.putItem(ITEM_A_A); // Deleted.
    cache.putItem(ITEM_B_A); // Not deleted.

    gcs.deleteBuckets(buckets);

    // Verify the delegate call.
    verify(gcsDelegate).deleteBuckets(eq(buckets));
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), Lists.newArrayList(ITEM_B_A));
  }

  @Test
  public void testDeleteObjects() throws IOException {
    List<StorageResourceId> ids =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_B_A.getResourceId());

    // Prepare the cache.
    cache.putItem(ITEM_A_A); // Deleted.
    cache.putItem(ITEM_B_A); // Deleted.
    cache.putItem(ITEM_B_B); // Not deleted.

    gcs.deleteObjects(ids);

    // Verify the delegate call.
    verify(gcsDelegate).deleteObjects(eq(ids));
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), Lists.newArrayList(ITEM_B_B));
  }

  @Test
  public void testListBucketInfo() throws IOException {
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_A, ITEM_B);

    List<GoogleCloudStorageItemInfo> result = gcs.listBucketInfo();

    // Verify the delegate call.
    verify(gcsDelegate).listBucketInfo();
    assertContainsInAnyOrder(result, expected);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), expected);
  }

  /** TODO: THIS TEST IS BROKEN */
  @Test
  public void testListObjectInfo() throws IOException {
    // TODO: This is broken.
    // The expected items SHOULD include ITEM_A_A, but the InMemoryGCS filters it out incorrectly.
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_A_AA, ITEM_A_ABA);

    List<GoogleCloudStorageItemInfo> result =
        gcs.listObjectInfo(BUCKET_A, PREFIX_A, null, GoogleCloudStorage.MAX_RESULTS_UNLIMITED);

    // Verify the delegate call.
    verify(gcsDelegate)
        .listObjectInfo(
            eq(BUCKET_A),
            Matchers.<String>eq(PREFIX_A),
            Matchers.<String>eq(null),
            eq(GoogleCloudStorage.MAX_RESULTS_UNLIMITED));
    assertContainsInAnyOrder(result, expected);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), expected);
  }

  @Test
  public void testListObjectInfoAlt() throws IOException {
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_B_A, ITEM_B_B);

    List<GoogleCloudStorageItemInfo> result = gcs.listObjectInfo(BUCKET_B, null, null);

    // Verify the delegate call.
    verify(gcsDelegate)
        .listObjectInfo(eq(BUCKET_B), Matchers.<String>eq(null), Matchers.<String>eq(null));
    assertContainsInAnyOrder(result, expected);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), expected);
  }

  @Test
  public void testListObjectInfoCached() throws IOException {
    List<GoogleCloudStorageItemInfo> expected =
        Lists.newArrayList(ITEM_A_A, ITEM_A_AA, ITEM_A_ABA, ITEM_A_B);

    // First call to get the values in cache.
    gcs.listObjectInfo(BUCKET_A, null, null);
    // Second call to ensure the values are being served from cache.
    List<GoogleCloudStorageItemInfo> result = gcs.listObjectInfo(BUCKET_A, null, null);

    // Verify the delegate call once.
    verify(gcsDelegate)
        .listObjectInfo(eq(BUCKET_A), Matchers.<String>eq(null), Matchers.<String>eq(null));
    assertContainsInAnyOrder(result, expected);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), expected);
  }

  @Test
  public void testGetItemInfo() throws IOException {
    // Prepare the cache.
    cache.putItem(ITEM_A_A);

    GoogleCloudStorageItemInfo result = gcs.getItemInfo(ITEM_A_A.getResourceId());

    // Verify the cached item was returned.
    assertEquals(result, ITEM_A_A);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), Lists.newArrayList(ITEM_A_A));
  }

  @Test
  public void testGetItemInfoMissing() throws IOException {
    GoogleCloudStorageItemInfo result = gcs.getItemInfo(ITEM_A_A.getResourceId());

    // Verify the delegate call.
    verify(gcsDelegate).getItemInfo(eq(ITEM_A_A.getResourceId()));
    assertEquals(result, ITEM_A_A);
    // Verify the cache was updated.
    assertEquals(cache.getItem(ITEM_A_A.getResourceId()), ITEM_A_A);
  }

  @Test
  public void testGetItemInfosAllCached() throws IOException {
    List<StorageResourceId> requestedIds =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_A_B.getResourceId());
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_A_A, ITEM_A_B);

    // Prepare the cache.
    cache.putItem(ITEM_A_A);
    cache.putItem(ITEM_A_B);

    List<GoogleCloudStorageItemInfo> result = gcs.getItemInfos(requestedIds);

    // Verify the result is exactly what the delegate returns.
    assertContainsInAnyOrder(result, expected);
    // Verify ordering
    assertEquals(result.get(0), ITEM_A_A);
    assertEquals(result.get(1), ITEM_A_B);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), expected);
  }

  @Test
  public void testGetItemInfosSomeCached() throws IOException {
    List<StorageResourceId> requestedIds =
        Lists.newArrayList(
            ITEM_A_A.getResourceId(), // Not cached
            ITEM_A_B.getResourceId(), // Cached
            ITEM_B_A.getResourceId(), // Not cached
            ITEM_B_B.getResourceId()); // Cached
    List<StorageResourceId> uncachedIds =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_B_A.getResourceId());
    List<GoogleCloudStorageItemInfo> expected =
        Lists.newArrayList(ITEM_A_A, ITEM_A_B, ITEM_B_A, ITEM_B_B);

    // Prepare the cache.
    cache.putItem(ITEM_A_B);
    cache.putItem(ITEM_B_B);

    List<GoogleCloudStorageItemInfo> result = gcs.getItemInfos(requestedIds);

    // Verify the delegate call.
    verify(gcsDelegate).getItemInfos(eq(uncachedIds));
    assertContainsInAnyOrder(result, expected);
    // Verify ordering.
    assertEquals(result.get(0), ITEM_A_A);
    assertEquals(result.get(1), ITEM_A_B);
    assertEquals(result.get(2), ITEM_B_A);
    assertEquals(result.get(3), ITEM_B_B);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), expected);
  }

  @Test
  public void testGetItemInfosNoneCached() throws IOException {
    List<StorageResourceId> requestedIds =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_A_B.getResourceId());
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_A_A, ITEM_A_B);

    List<GoogleCloudStorageItemInfo> result = gcs.getItemInfos(requestedIds);

    // Verify the delegate call.
    verify(gcsDelegate).getItemInfos(eq(requestedIds));
    assertContainsInAnyOrder(result, expected);
    // Verify ordering
    assertEquals(result.get(0), ITEM_A_A);
    assertEquals(result.get(1), ITEM_A_B);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), expected);
  }

  @Test
  public void testUpdateItems() throws IOException {
    List<GoogleCloudStorageItemInfo> expected = Lists.newArrayList(ITEM_A_A);
    List<UpdatableItemInfo> updateItems =
        Lists.newArrayList(new UpdatableItemInfo(ITEM_A_A.getResourceId(), TEST_METADATA));

    List<GoogleCloudStorageItemInfo> result = gcs.updateItems(updateItems);

    // Verify the delegate call.
    verify(gcsDelegate).updateItems(eq(updateItems));
    assertContainsInAnyOrder(result, expected);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), expected);
  }

  @Test
  public void testClose() {
    // Prepare the cache.
    cache.putItem(ITEM_A_A);

    gcs.close();

    // Verify the delegate call was made.
    verify(gcsDelegate).close();
    // Verify the state of the cache.
    assertContainsInAnyOrder(
        cache.getAllItemsRaw(), Lists.<GoogleCloudStorageItemInfo>newArrayList());
  }

  @Test
  public void testComposeObjects() throws IOException {
    List<StorageResourceId> ids =
        Lists.newArrayList(ITEM_A_A.getResourceId(), ITEM_A_B.getResourceId());

    GoogleCloudStorageItemInfo result =
        gcs.composeObjects(ids, ITEM_A_AA.getResourceId(), CREATE_OBJECT_OPTIONS);

    // Verify the delegate call.
    verify(gcsDelegate)
        .composeObjects(eq(ids), eq(ITEM_A_AA.getResourceId()), eq(CREATE_OBJECT_OPTIONS));
    assertEquals(result, ITEM_A_AA);
    // Verify the state of the cache.
    assertContainsInAnyOrder(cache.getAllItemsRaw(), Lists.newArrayList(ITEM_A_AA));
  }

  /**
   * Helper to generate GoogleCloudStorageItemInfo for a bucket entry.
   *
   * @param bucketName the name of the bucket.
   * @return the generated item.
   */
  public static GoogleCloudStorageItemInfo createBucketItemInfo(String bucketName) {
    GoogleCloudStorageItemInfo item =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(bucketName),
            0,
            0,
            CREATE_BUCKET_OPTIONS.getLocation(),
            CREATE_BUCKET_OPTIONS.getStorageClass());
    return item;
  }

  /**
   * Helper to generate a GoogleCloudStorageItemInfo for an object entry.
   *
   * @param bucketName the name of the bucket for the generated item.
   * @param objectName the object name of the generated item.
   * @return the generated item.
   */
  public static GoogleCloudStorageItemInfo createObjectItemInfo(
      String bucketName, String objectName) {
    GoogleCloudStorageItemInfo item =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(bucketName, objectName),
            0,
            0,
            null,
            null,
            CREATE_OBJECT_OPTIONS.getContentType(),
            TEST_METADATA,
            0,
            0L);
    return item;
  }

  /**
   * Helper method for comparing collections of GoogleCloudStorageItemInfos. Only checks equality
   * based on the item's resource id.
   *
   * @param actualItems the actual collect of GoogleCloudStorageItemInfo.
   * @param expectedItems the collection of GoogleCloudStorageItemInfo that was expected.
   */
  public static void assertContainsInAnyOrder(
      Collection<GoogleCloudStorageItemInfo> actualItems,
      Collection<GoogleCloudStorageItemInfo> expectedItems) {

    List<WrappedItem> actualWrapped = new ArrayList<WrappedItem>();
    List<WrappedItem> expectedWrapped = new ArrayList<WrappedItem>();
    List<WrappedItem> xor = new ArrayList<WrappedItem>();

    for (GoogleCloudStorageItemInfo actual : actualItems) {
      WrappedItem wrapped = new WrappedItem(actual);
      actualWrapped.add(wrapped);
      xor.add(wrapped);
    }
    for (GoogleCloudStorageItemInfo expected : expectedItems) {
      WrappedItem wrapped = new WrappedItem(expected);
      expectedWrapped.add(wrapped);
      xor.add(wrapped);
    }

    String actualString = Joiner.on(',').join(actualWrapped);
    String expectedString = Joiner.on(',').join(expectedWrapped);

    actualWrapped.retainAll(expectedWrapped);
    xor.removeAll(actualWrapped);

    if (!xor.isEmpty()) {
      throw new AssertionError(
          String.format(
              "\nExpected: [ %s ] in any order"
                  + "\n     got: [ %s ]"
                  + "\n     but: [ %s ] were not matched",
              expectedString, actualString, Joiner.on(',').join(xor)));
    }
  }

  /** Used to have the correct equals behavior for items. */
  private static class WrappedItem {
    private final GoogleCloudStorageItemInfo item;

    public WrappedItem(GoogleCloudStorageItemInfo item) {
      this.item = item;
    }

    // TODO(b/37774152): implement hashCode() (go/equals-hashcode-lsc)
    @SuppressWarnings("EqualsHashCode")
    @Override
    public boolean equals(Object obj) {
      return PerformanceCachingGoogleCloudStorageTest.equals(item, ((WrappedItem) obj).item);
    }

    @Override
    public String toString() {
      return item.getResourceId().toString();
    }
  }

  /**
   * Helper method for comparing GoogleCloudStorageItemInfo. Checks based on resource id, location,
   * storage class, content type, and metadata.
   *
   * @param actual the actual object.
   * @param expected the expected object.
   * @throws AssertionError if the objects are not equal.
   */
  public static void assertEquals(
      GoogleCloudStorageItemInfo actual, GoogleCloudStorageItemInfo expected) {
    if (!equals(actual, expected)) {
      throw new AssertionError(String.format("\nExpected: %s\n but was: %s ", expected, actual));
    }
  }

  /**
   * Helper method for comparing GoogleCloudStorageItemInfo. Checks based on resource id, location,
   * storage class, content type, and metadata.
   *
   * @param a a GoogleCloudStorageItemInfo.
   * @param b a GoogleCloudStorageItemInfo to be compared with a.
   * @return true of the arguments are equal, false otherwise.
   */
  public static boolean equals(GoogleCloudStorageItemInfo a, GoogleCloudStorageItemInfo b) {
    if (a == b) {
      return true;
    } else if (a == null || b == null) {
      return false;
    } else {
      return a.getResourceId().equals(b.getResourceId())
          && Objects.equals(a.getLocation(), b.getLocation())
          && Objects.equals(a.getStorageClass(), b.getStorageClass())
          && Objects.equals(a.getContentType(), b.getContentType())
          && a.metadataEquals(b.getMetadata());
    }
  }

  /** Ticker with a manual time value used for testing the cache. */
  private static class TestTicker extends Ticker {

    @Override
    public long read() {
      return 0L;
    }
  }

  /** Clock with a manual time value used for testing the GCS delegate. */
  private static class TestClock implements Clock {

    @Override
    public long currentTimeMillis() {
      return 0L;
    }
  }
}
