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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Clock;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * UnitTests for CacheSupplementedGoogleCloudStorage class specific to cache-supplemental
 * functionality. TODO(user): Evaluate using a modified InMemoryGoogleCloudStorage instead of mocks.
 */
@RunWith(JUnit4.class)
public class CacheSupplementedGoogleCloudStorageTest {
  private static final long MAX_ENTRY_AGE = 10000L;
  private static final long MAX_INFO_AGE = 2000L;
  private static final long BASE_TIME = 123L;

  @Mock private GoogleCloudStorage mockGcsDelegate;
  @Mock private WritableByteChannel mockWriteChannel;
  @Mock private Clock mockClock;

  private StorageResourceId bucketResourceId;
  private StorageResourceId objectResourceId;
  private GoogleCloudStorageItemInfo bucketInfo;
  private GoogleCloudStorageItemInfo objectInfo;
  private DirectoryListCache cache;

  // The test instance, set up with a mock GoogleCloudStorage as a delegate. Inherited test cases
  // will *not* use this instance, and will instead use createTestInstance() which simply wraps
  // the parent class's gcs as a delegate. This test instance will be used for subtle cache behavior
  // without having to mock at the messy API level.
  private GoogleCloudStorage gcs;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockClock.currentTimeMillis()).thenReturn(BASE_TIME);

    bucketInfo = DirectoryListCacheTestUtils.createBucketInfo("foo-bucket");
    bucketResourceId = bucketInfo.getResourceId();
    objectInfo = DirectoryListCacheTestUtils.createObjectInfo("foo-bucket", "bar-object");
    objectResourceId = objectInfo.getResourceId();
    cache = new InMemoryDirectoryListCache();
    cache.getMutableConfig()
        .setMaxEntryAgeMillis(MAX_ENTRY_AGE)
        .setMaxInfoAgeMillis(MAX_INFO_AGE);

    CacheEntry.setClock(mockClock);
    cache.setClock(mockClock);

    // Give the testInstance a fresh DirectoryListCache for each test case.
    CacheSupplementedGoogleCloudStorage testInstance =
        new CacheSupplementedGoogleCloudStorage(mockGcsDelegate, cache);
    gcs = testInstance;
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockGcsDelegate);
    verifyNoMoreInteractions(mockWriteChannel);
  }

  @Test
  public void testCreateObject()
      throws IOException {
    when(mockGcsDelegate.create(eq(objectResourceId), eq(CreateObjectOptions.DEFAULT)))
        .thenReturn(mockWriteChannel);
    when(mockWriteChannel.write(any(ByteBuffer.class)))
        .thenReturn(42);
    when(mockWriteChannel.isOpen())
        .thenReturn(true);

    WritableByteChannel channel = gcs.create(objectResourceId);
    assertThat(channel.write(ByteBuffer.allocate(123))).isEqualTo(42);
    assertThat(channel.isOpen()).isTrue();

    // After creating the channel but before closing, the DirectoryListCache will not have been
    // updated yet.
    assertThat(cache.getCacheEntry(objectResourceId)).isNull();
    channel.close();
    assertThat(cache.getCacheEntry(objectResourceId)).isNotNull();

    verify(mockGcsDelegate).create(eq(objectResourceId), eq(CreateObjectOptions.DEFAULT));
    verify(mockWriteChannel).write(any(ByteBuffer.class));
    verify(mockWriteChannel).isOpen();
    verify(mockWriteChannel).close();
  }

  @Test
  public void testOpenObject()
      throws IOException {
    SeekableByteChannel mockChannel = mock(SeekableByteChannel.class);
    when(mockGcsDelegate.open(eq(objectResourceId)))
        .thenReturn(mockChannel);
    assertEquals(mockChannel, gcs.open(objectResourceId));
    verify(mockGcsDelegate).open(objectResourceId);
  }

  @Test
  public void testCreateAndDeleteBuckets()
      throws IOException {
    gcs.create("bucket1");
    gcs.create("bucket2");
    gcs.create("bucket3", CreateBucketOptions.DEFAULT);
    verify(mockGcsDelegate, times(2)).create(any(String.class));
    verify(mockGcsDelegate, times(1)).create(any(String.class), any(CreateBucketOptions.class));
    assertThat(cache.getCacheEntry(new StorageResourceId("bucket1"))).isNotNull();
    assertThat(cache.getCacheEntry(new StorageResourceId("bucket2"))).isNotNull();
    assertThat(cache.getCacheEntry(new StorageResourceId("bucket3"))).isNotNull();

    assertThat(cache.getBucketList()).hasSize(3);

    List<String> bucketsToDelete = ImmutableList.of("bucket2", "bucket3", "bucket4");
    gcs.deleteBuckets(bucketsToDelete);
    verify(mockGcsDelegate).deleteBuckets(eq(bucketsToDelete));

    assertThat(cache.getBucketList()).hasSize(1);
    assertThat(cache.getBucketList().get(0).getResourceId().getBucketName()).isEqualTo("bucket1");
  }

  @Test
  public void testDeleteObjects()
      throws IOException {
    when(mockGcsDelegate.create(any(StorageResourceId.class), any(CreateObjectOptions.class)))
        .thenReturn(mockWriteChannel);

    gcs.create(new StorageResourceId("foo-bucket", "obj1")).close();
    gcs.create(new StorageResourceId("foo-bucket2", "obj2")).close();
    gcs.create(new StorageResourceId("foo-bucket2", "obj3")).close();

    verify(mockGcsDelegate, times(3)).create(
        any(StorageResourceId.class), any(CreateObjectOptions.class));
    verify(mockWriteChannel, times(3)).close();

    assertThat(cache.getObjectList("foo-bucket", "", null, null)).hasSize(1);
    assertThat(cache.getObjectList("foo-bucket2", "", null, null)).hasSize(2);
    assertThat(cache.getBucketList()).hasSize(2);

    List<StorageResourceId> toDelete = ImmutableList.of(
        new StorageResourceId("foo-bucket2", "obj2"),
        new StorageResourceId("foo-bucket2", "obj3"),
        new StorageResourceId("foo-bucket2", "obj4"));
    gcs.deleteObjects(toDelete);
    verify(mockGcsDelegate).deleteObjects(eq(toDelete));

    assertThat(cache.getObjectList("foo-bucket", "", null, null)).hasSize(1);
    assertThat(cache.getObjectList("foo-bucket2", "", null, null)).isEmpty();
    assertThat(cache.getBucketList()).hasSize(2);
  }

  @Test
  public void testCopy()
      throws IOException {
    when(mockGcsDelegate.create(eq(objectResourceId), eq(CreateObjectOptions.DEFAULT)))
        .thenReturn(mockWriteChannel);
    gcs.create(objectResourceId).close();
    verify(mockGcsDelegate).create(eq(objectResourceId), eq(CreateObjectOptions.DEFAULT));
    verify(mockWriteChannel).close();

    List<String> srcObjectNames = ImmutableList.of(objectResourceId.getObjectName());
    List<String> dstObjectNames = ImmutableList.of("dst1", "dst2", "dst3");

    gcs.copy(objectResourceId.getBucketName(), srcObjectNames, "dst-bucket", dstObjectNames);
    verify(mockGcsDelegate).copy(eq(objectResourceId.getBucketName()), eq(srcObjectNames),
        eq("dst-bucket"), eq(dstObjectNames));

    // Srcs still exist in cache.
    assertThat(cache.getCacheEntry(objectResourceId)).isNotNull();
    assertThat(cache.getCacheEntry(new StorageResourceId(objectResourceId.getBucketName())))
        .isNotNull();

    // All destination resources cached.
    assertThat(cache.getCacheEntry(new StorageResourceId("dst-bucket"))).isNotNull();
    assertThat(cache.getCacheEntry(new StorageResourceId("dst-bucket", "dst1"))).isNotNull();
    assertThat(cache.getCacheEntry(new StorageResourceId("dst-bucket", "dst2"))).isNotNull();
    assertThat(cache.getCacheEntry(new StorageResourceId("dst-bucket", "dst3"))).isNotNull();
  }

  @Test
  public void testListBucketNames()
      throws IOException {
    List<String> bucketList = ImmutableList.of("bucket1", "bucket2", "bucket3");

    // Empty cache.
    when(mockGcsDelegate.listBucketNames())
        .thenReturn(bucketList);
    assertEquals(bucketList, gcs.listBucketNames());

    // Put a subset of what the delegate will return in the cache.
    cache.putResourceId(new StorageResourceId("bucket2"));
    cache.putResourceId(new StorageResourceId("bucket3"));
    assertEquals(bucketList, gcs.listBucketNames());

    // Add an extra cache entry which will get supplemented into the final returned list.
    cache.putResourceId(new StorageResourceId("bucket4"));
    List<String> supplementedList = new ArrayList<>(bucketList);
    supplementedList.add("bucket4");
    assertEquals(supplementedList, gcs.listBucketNames());

    long nextTime = MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    // Even after info-expiration-age, the entries still get supplemented.
    assertEquals(supplementedList, gcs.listBucketNames());

    // After expiration, supplementation no longer adds anything; back to original bucketList.
    nextTime += MAX_ENTRY_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    assertEquals(bucketList, gcs.listBucketNames());

    verify(mockGcsDelegate, times(5)).listBucketNames();
  }

  @Test
  public void testListBucketInfo()
      throws IOException {
    long baseTime = DirectoryListCacheTestUtils.BUCKET_BASE_CREATE_TIME;
    List<GoogleCloudStorageItemInfo> bucketList = ImmutableList.of(
        DirectoryListCacheTestUtils.createBucketInfo("bucket1"),
        DirectoryListCacheTestUtils.createBucketInfo("bucket2"),
        DirectoryListCacheTestUtils.createBucketInfo("bucket3"));

    // Empty cache.
    when(mockGcsDelegate.listBucketInfo())
        .thenReturn(bucketList);
    assertEquals(bucketList, gcs.listBucketInfo());

    // Put a subset of what the delegate will return in the cache.
    cache.putResourceId(new StorageResourceId("bucket2"));
    cache.putResourceId(new StorageResourceId("bucket3"));
    assertEquals(bucketList, gcs.listBucketInfo());

    // Add an extra cache entry which will get supplemented into the final returned list. Prepare
    // for a call to getItemInfo; make it fail with !exists() the first time.
    StorageResourceId supplementedId = new StorageResourceId("bucket4");
    cache.putResourceId(supplementedId);
    GoogleCloudStorageItemInfo supplementedInfo =
        DirectoryListCacheTestUtils.createBucketInfo("bucket4");
    when(mockGcsDelegate.getItemInfo(eq(supplementedId)))
        .thenReturn(GoogleCloudStorageImpl.createItemInfoForNotFound(
            supplementedId))
        .thenReturn(supplementedInfo)
        .thenReturn(supplementedInfo);

    // No supplement yet, despite one call to getItemInfo so far.
    assertEquals(bucketList, gcs.listBucketInfo());

    // Since the call to getItemInfo returned NOT_FOUND, we'll expect the item to be entirely
    // removed from the cache. We'll then add it back in as a StorageResourceId-only entry
    // for the next part of this test method.
    assertThat(cache.getCacheEntry(supplementedId)).isNull();
    cache.putResourceId(supplementedId);
    verify(mockGcsDelegate).getItemInfo(eq(supplementedId));

    // Second call succeeds.
    List<GoogleCloudStorageItemInfo> supplementedList = new ArrayList<>(bucketList);
    supplementedList.add(supplementedInfo);
    assertEquals(supplementedList, gcs.listBucketInfo());
    verify(mockGcsDelegate, times(2)).getItemInfo(eq(supplementedId));

    // Check its presence in the cache.
    GoogleCloudStorageItemInfo cacheInfo =
        cache.getCacheEntry(supplementedId).getItemInfo();
    assertThat(cacheInfo).isNotNull();
    assertEquals(supplementedInfo, cacheInfo);

    // Immediate-following call to listBucketInfo doesn't require a new getItemInfo.
    assertEquals(supplementedList, gcs.listBucketInfo());
    verify(mockGcsDelegate, times(2)).getItemInfo(eq(supplementedId));

    // After info-expiration-age, the getItemInfo will have to get called again.
    long nextTime = baseTime + MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    assertEquals(supplementedList, gcs.listBucketInfo());
    verify(mockGcsDelegate, times(3)).getItemInfo(eq(supplementedId));

    // After expiration, supplementation no longer adds anything; back to original bucketList.
    nextTime += MAX_ENTRY_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    assertEquals(bucketList, gcs.listBucketInfo());

    verify(mockGcsDelegate, times(7)).listBucketInfo();
    verify(mockGcsDelegate, times(3)).getItemInfo(eq(supplementedId));
  }

  @Test
  public void testListObjectNames()
      throws IOException {
    String bucketName = "bucket1";
    String prefix = "foo/dir";
    List<String> objectList = ImmutableList.of("foo/dir1/", "foo/dir2");

    // Empty cache.
    when(mockGcsDelegate.listObjectNames(eq(bucketName), eq(prefix), eq("/"),
          eq(GoogleCloudStorage.MAX_RESULTS_UNLIMITED)))
        .thenReturn(objectList);
    assertEquals(objectList, gcs.listObjectNames(bucketName, prefix, "/",
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED));

    // Put a subset of what the delegate will return in the cache.
    cache.putResourceId(new StorageResourceId(bucketName, "foo/dir2"));
    assertEquals(objectList, gcs.listObjectNames(bucketName, prefix, "/",
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED));

    // Add extra cache entries which will get supplemented into the final returned list.
    cache.putResourceId(new StorageResourceId(bucketName, "foo/dir3"));  // matches.
    cache.putResourceId(new StorageResourceId(bucketName, "foo/dir4/"));  // matches.
    cache.putResourceId(new StorageResourceId("bucket2", "foo/dir5"));  // wrong bucket.
    cache.putResourceId(new StorageResourceId(bucketName, "foo/dir6/bar"));  // implicit dir6
    List<String> supplementedList = new ArrayList<>(objectList);
    supplementedList.add("foo/dir3");
    supplementedList.add("foo/dir4/");

    // Set up retrieval of itemInfos.
    GoogleCloudStorageItemInfo supplementedDir3 =
        DirectoryListCacheTestUtils.createObjectInfo(bucketName, "foo/dir3");
    GoogleCloudStorageItemInfo supplementedDir4 =
        DirectoryListCacheTestUtils.createObjectInfo(bucketName, "foo/dir4/");
    when(mockGcsDelegate.getItemInfo(eq(supplementedDir3.getResourceId())))
        .thenReturn(supplementedDir3);
    when(mockGcsDelegate.getItemInfo(eq(supplementedDir4.getResourceId())))
        .thenReturn(supplementedDir4);

    // Before info-expiration, no revalidation occurs with getItemInfo.
    List<String> actualObjects = new ArrayList<>(
        gcs.listObjectNames(bucketName, prefix, "/", GoogleCloudStorage.MAX_RESULTS_UNLIMITED));
    Collections.sort(actualObjects);
    assertEquals(supplementedList, actualObjects);

    // Even after info-expiration-age, the entries still get supplemented.
    long nextTime = MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    actualObjects = new ArrayList<>(
        gcs.listObjectNames(bucketName, prefix, "/", GoogleCloudStorage.MAX_RESULTS_UNLIMITED));
    Collections.sort(actualObjects);
    assertEquals(supplementedList, actualObjects);

    // Infos are revalidated.
    verify(mockGcsDelegate).getItemInfo(eq(supplementedDir3.getResourceId()));
    verify(mockGcsDelegate).getItemInfo(eq(supplementedDir4.getResourceId()));

    // After expiration, supplementation no longer adds anything; back to original objectList.
    nextTime += MAX_ENTRY_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    actualObjects = new ArrayList<>(
        gcs.listObjectNames(bucketName, prefix, "/", GoogleCloudStorage.MAX_RESULTS_UNLIMITED));
    Collections.sort(actualObjects);
    assertEquals(objectList, actualObjects);

    verify(mockGcsDelegate, times(5)).listObjectNames(eq(bucketName),
        eq(prefix), eq("/"),
        eq(GoogleCloudStorage.MAX_RESULTS_UNLIMITED));
  }

  @Test
  public void testListObjectInfo()
      throws IOException {
    String bucketName = "bucket1";
    String prefix = "foo/dir";
    List<GoogleCloudStorageItemInfo> objectList = ImmutableList.of(
        DirectoryListCacheTestUtils.createObjectInfo(bucketName, "foo/dir1/"),
        DirectoryListCacheTestUtils.createObjectInfo(bucketName, "foo/dir2"));

    // Empty cache.
    when(mockGcsDelegate.listObjectInfo(eq(bucketName), eq(prefix), eq("/"),
        eq(GoogleCloudStorage.MAX_RESULTS_UNLIMITED)))
        .thenReturn(objectList);
    assertEquals(objectList, gcs.listObjectInfo(bucketName, prefix, "/",
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED));

    // Put a subset of what the delegate will return in the cache.
    cache.putResourceId(new StorageResourceId(bucketName, "foo/dir2"));
    assertEquals(objectList, gcs.listObjectInfo(bucketName, prefix, "/",
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED));

    // Add extra cache entries which will get supplemented into the final returned list.
    StorageResourceId supplementedId = new StorageResourceId(bucketName, "foo/dir4/");
    cache.putResourceId(supplementedId);  // matches.
    cache.putResourceId(new StorageResourceId("bucket2", "foo/dir5"));  // wrong bucket.
    cache.putResourceId(new StorageResourceId(bucketName, "foo/dir6/bar"));  // implicit dir6
    GoogleCloudStorageItemInfo supplementedInfo = DirectoryListCacheTestUtils.createObjectInfo(
        supplementedId.getBucketName(), supplementedId.getObjectName());
    when(mockGcsDelegate.getItemInfo(eq(supplementedId)))
        .thenReturn(GoogleCloudStorageImpl.createItemInfoForNotFound(
            supplementedId))
        .thenReturn(supplementedInfo)
        .thenReturn(supplementedInfo);

    // No supplement yet, despite one call to getItemInfo so far.
    assertEquals(objectList, gcs.listObjectInfo(bucketName, prefix, "/",
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED));

    // Since the call to getItemInfo returned NOT_FOUND, we'll expect the item to be entirely
    // removed from the cache. We'll then add it back in as a StorageResourceId-only entry
    // for the next part of this test method.
    assertThat(cache.getCacheEntry(supplementedId)).isNull();
    cache.putResourceId(supplementedId);
    verify(mockGcsDelegate).getItemInfo(eq(supplementedId));

    // Second call succeeds.
    List<GoogleCloudStorageItemInfo> supplementedList = new ArrayList<>(objectList);
    supplementedList.add(supplementedInfo);
    assertEquals(supplementedList, gcs.listObjectInfo(bucketName, prefix, "/",
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED));
    verify(mockGcsDelegate, times(2)).getItemInfo(eq(supplementedId));

    // Check its presence in the cache.
    GoogleCloudStorageItemInfo cacheInfo =
        cache.getCacheEntry(supplementedId).getItemInfo();
    assertThat(cacheInfo).isNotNull();
    assertEquals(supplementedInfo, cacheInfo);

    // Immediate-following call to listBucketInfo doesn't require a new getItemInfo.
    assertEquals(supplementedList, gcs.listObjectInfo(bucketName, prefix, "/",
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED));
    verify(mockGcsDelegate, times(2)).getItemInfo(eq(supplementedId));

    // After info-expiration-age, the getItemInfo will have to get called again.
    long nextTime = BASE_TIME + MAX_INFO_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    assertEquals(supplementedList, gcs.listObjectInfo(bucketName, prefix, "/",
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED));
    verify(mockGcsDelegate, times(3)).getItemInfo(eq(supplementedId));

    // After expiration, supplementation no longer adds anything; back to original objectList.
    nextTime += MAX_ENTRY_AGE + 1;
    when(mockClock.currentTimeMillis()).thenReturn(nextTime);
    assertEquals(objectList, gcs.listObjectInfo(bucketName, prefix, "/",
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED));

    verify(mockGcsDelegate, times(7)).listObjectInfo(eq(bucketName),
        eq(prefix), eq("/"),
        eq(GoogleCloudStorage.MAX_RESULTS_UNLIMITED));
    verify(mockGcsDelegate, times(3)).getItemInfo(eq(supplementedId));
  }

  @Test
  public void testGetItemInfos()
      throws IOException {
    List<StorageResourceId> ids = ImmutableList.of(objectResourceId, bucketResourceId);
    List<GoogleCloudStorageItemInfo> infos = ImmutableList.of(objectInfo, bucketInfo);
    when(mockGcsDelegate.getItemInfos(eq(ids)))
        .thenReturn(infos);
    assertEquals(infos, gcs.getItemInfos(ids));
    verify(mockGcsDelegate).getItemInfos(eq(ids));

    // For now, we do not opportunistically update the cache.
    assertThat(cache.getInternalNumBuckets()).isEqualTo(0);
    assertThat(cache.getInternalNumObjects()).isEqualTo(0);
  }

  @Test
  public void testGetItemInfo()
      throws IOException {
    when(mockGcsDelegate.getItemInfo(eq(objectResourceId)))
        .thenReturn(objectInfo);
    assertEquals(objectInfo, gcs.getItemInfo(objectResourceId));
    verify(mockGcsDelegate).getItemInfo(eq(objectResourceId));

    // For now, we do not opportunistically update the cache.
    assertThat(cache.getCacheEntry(objectResourceId)).isNull();
  }

  @Test
  public void testClose()
      throws IOException {
    gcs.close();
    verify(mockGcsDelegate).close();
  }

  @Test
  public void testWaitForBucketEmpty()
      throws IOException {
    gcs.waitForBucketEmpty(bucketResourceId.getBucketName());
    verify(mockGcsDelegate).waitForBucketEmpty(eq(bucketResourceId.getBucketName()));
  }

  @Test
  public void testCompose() throws IOException {
    List<String> sources = ImmutableList.of("object1", "object2");
    String destination = "composedObject";
    String contentType = CreateFileOptions.DEFAULT_CONTENT_TYPE;

    gcs.compose(bucketResourceId.getBucketName(), sources, destination, contentType);

    verify(mockGcsDelegate)
        .compose(
            eq(bucketResourceId.getBucketName()), eq(sources), eq(destination), eq(contentType));
  }
}
