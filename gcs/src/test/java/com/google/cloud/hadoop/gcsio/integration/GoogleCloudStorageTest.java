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

package com.google.cloud.hadoop.gcsio.integration;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.CacheSupplementedGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageWriteChannel;
import com.google.cloud.hadoop.gcsio.InMemoryGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.LaggedGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.LaggedGoogleCloudStorage.ListVisibilityCalculator;
import com.google.cloud.hadoop.gcsio.SeekableReadableByteChannel;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class GoogleCloudStorageTest {

  // Test classes using JUnit4 runner must have only a single constructor. Since we
  // want to be able to pass in dependencies, we'll maintain this base class as
  // @Parameterized with @Parameters.
  @Parameters
  public static Collection<Object[]> getConstructorArguments() throws IOException {
    GoogleCloudStorage gcs = new InMemoryGoogleCloudStorage();
    GoogleCloudStorage cachedGcs =
        new CacheSupplementedGoogleCloudStorage(new InMemoryGoogleCloudStorage());
    GoogleCloudStorage cachedLaggedGcs = new CacheSupplementedGoogleCloudStorage(
        new LaggedGoogleCloudStorage(
            new InMemoryGoogleCloudStorage(),
            Clock.SYSTEM,
            ListVisibilityCalculator.DEFAULT_LAGGED));
    return Arrays.asList(new Object[][]{{gcs}, {cachedGcs}, {cachedLaggedGcs}});
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final GoogleCloudStorage gcs;
  private final TestBucketHelper bucketHelper;

  public GoogleCloudStorageTest(GoogleCloudStorage gcs) {
    this.bucketHelper = new TestBucketHelper("gcs_it");
    this.gcs = gcs;
  }

  private void cleanupTestObjects(String bucketName, StorageResourceId object) throws IOException {
    cleanupTestObjects(bucketName, ImmutableList.of(object));
  }

  private void cleanupTestObjects(String bucketName, List<StorageResourceId> resources)
      throws IOException {
    cleanupTestObjects(ImmutableList.of(bucketName), resources);
  }

  private void cleanupTestObjects(List<String> bucketNames, List<StorageResourceId> resources)
      throws IOException {
    GoogleCloudStorageTestHelper.cleanupTestObjects(gcs, bucketNames, resources);
  }

  @After
  public void cleanup() throws IOException {
    bucketHelper.cleanupTestObjects(gcs);
  }

  @Test
  public void testCreateSuccessfulBucket() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_successful");
    gcs.create(bucketName);

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "CreateTestObject");
    gcs.createEmptyObject(objectToCreate);

    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testCreateExistingBucket() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_existing");
    gcs.create(bucketName);

    expectedException.expect(IOException.class);

    try {
      gcs.create(bucketName);
    } finally {
      gcs.deleteBuckets(ImmutableList.of(bucketName));
    }
  }

  @Test
  public void testCreateInvalidBucket() throws IOException {
    // Buckets must start with a letter or number
    String bucketName = "--" + bucketHelper.getUniqueBucketName("create_invalid");

    expectedException.expect(IOException.class);

    gcs.create(bucketName);
  }


  @Test
  public void testCreateObject() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_object");
    gcs.create(bucketName);

    byte[] bytesToWrite = "SomeText".getBytes(StandardCharsets.UTF_8);

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "CreateTestObject");
    try (WritableByteChannel channel = gcs.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);

    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testCreateZeroLengthObjectUsingCreate() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_empty_obj");
    gcs.create(bucketName);

    byte[] bytesToWrite = new byte[0];

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "CreateEmptyTestObject");
    try (WritableByteChannel channel = gcs.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);
    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testCreate1PageLengthObjectUsingCreate() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_1page");
    gcs.create(bucketName);

    byte[] bytesToWrite = new byte[GoogleCloudStorageWriteChannel.UPLOAD_PIPE_BUFFER_SIZE_DEFAULT];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "Create1PageTestObject");
    try (WritableByteChannel channel = gcs.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);

    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testCreate1PageLengthPlus1byteObjectUsingCreate() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_1page_p1");
    gcs.create(bucketName);

    byte[] bytesToWrite =
        new byte[GoogleCloudStorageWriteChannel.UPLOAD_PIPE_BUFFER_SIZE_DEFAULT + 1];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    StorageResourceId objectToCreate = new StorageResourceId(
        bucketName, "Create1PagePlusOneTestObject");

    try (WritableByteChannel channel = gcs.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);

    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testCreateExistingObject() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_existing_obj");
    gcs.create(bucketName);

    byte[] bytesToWrite = "SomeText".getBytes(StandardCharsets.UTF_8);

    StorageResourceId objectToCreate = new StorageResourceId(
        bucketName, "CreateExistingObject");

    try (WritableByteChannel channel = gcs.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    byte[] overwriteBytesToWrite = "OverwriteText".getBytes(StandardCharsets.UTF_8);

    // We need to write data and close to trigger an IOException.
    try (WritableByteChannel channel2 = gcs.create(objectToCreate)) {
      channel2.write(ByteBuffer.wrap(overwriteBytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, overwriteBytesToWrite);

    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testCreateEmptyObject() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_empty_obj");
    gcs.create(bucketName);

    StorageResourceId objectToCreate = new StorageResourceId(
        bucketName, "CreateEmptyObject");

    gcs.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo =
        gcs.getItemInfo(objectToCreate);

    assertTrue(itemInfo.exists());
    assertEquals(0, itemInfo.getSize());

    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testCreateEmptyExistingObject() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_empty_ex_obj");
    gcs.create(bucketName);

    StorageResourceId objectToCreate = new StorageResourceId(
        bucketName, "CreateEmptyExistingObject");

    gcs.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo =
        gcs.getItemInfo(objectToCreate);

    assertTrue(itemInfo.exists());
    assertEquals(0, itemInfo.getSize());

    gcs.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo secondItemInfo =
        gcs.getItemInfo(objectToCreate);

    assertTrue(secondItemInfo.exists());
    assertEquals(0, secondItemInfo.getSize());
    assertNotSame(itemInfo.getCreationTime(), secondItemInfo.getCreationTime());

    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testGetSingleItemInfo() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("get_singleitem");
    gcs.create(bucketName);

    StorageResourceId objectToCreate = new StorageResourceId(
        bucketName, "GetSingleItemInfoObject");

    gcs.createEmptyObject(objectToCreate);

    GoogleCloudStorageItemInfo itemInfo =
        gcs.getItemInfo(objectToCreate);

    assertTrue(itemInfo.exists());
    assertEquals(0, itemInfo.getSize());

    StorageResourceId secondObjectToCreate = new StorageResourceId(
        bucketName, "GetSingleItemInfoObject2");

    byte[] bytesToWrite = new byte[100];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    try (WritableByteChannel channel = gcs.create(secondObjectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageItemInfo secondItemInfo =
        gcs.getItemInfo(secondObjectToCreate);

    assertTrue(secondItemInfo.exists());
    assertEquals(100, secondItemInfo.getSize());
    assertFalse(secondItemInfo.isBucket());
    assertFalse(secondItemInfo.isRoot());

    GoogleCloudStorageItemInfo nonExistentItemInfo =
        gcs.getItemInfo(new StorageResourceId(bucketName, "SomethingThatDoesntExist"));

    assertFalse(nonExistentItemInfo.exists());
    assertFalse(nonExistentItemInfo.isBucket());
    assertFalse(nonExistentItemInfo.isRoot());

    // Test bucket get item info
    GoogleCloudStorageItemInfo bucketInfo =
        gcs.getItemInfo(new StorageResourceId(bucketName));
    assertTrue(bucketInfo.exists());
    assertTrue(bucketInfo.isBucket());

    GoogleCloudStorageItemInfo rootInfo =
        gcs.getItemInfo(StorageResourceId.ROOT);
    assertTrue(rootInfo.exists());
    assertTrue(rootInfo.isRoot());

    cleanupTestObjects(bucketName, ImmutableList.of(objectToCreate, secondObjectToCreate));
  }

  @Test
  public void testGetMultipleItemInfo() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("get_multi_item");
    gcs.create(bucketName);

    List<StorageResourceId> objectsCreated = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      StorageResourceId objectToCreate = new StorageResourceId(
          bucketName, String.format("GetMultiItemInfoObject_%s", i));
      gcs.createEmptyObject(objectToCreate);
      objectsCreated.add(objectToCreate);
    }

    StorageResourceId bucketResourceId = new StorageResourceId(bucketName);
    StorageResourceId nonExistentResourceId = new StorageResourceId(bucketName, "IDontExist");

    List<StorageResourceId> allResources = Lists.newArrayList();
    allResources.addAll(objectsCreated);
    allResources.add(nonExistentResourceId);
    allResources.add(bucketResourceId);

    List<GoogleCloudStorageItemInfo> allInfo = gcs.getItemInfos(allResources);

    for (int i = 0; i < objectsCreated.size(); i++) {
      StorageResourceId resourceId = objectsCreated.get(i);
      GoogleCloudStorageItemInfo info = allInfo.get(i);

      assertEquals(resourceId, info.getResourceId());
      assertEquals(0, info.getSize());
      assertTrue("Item should exist", info.exists());
      assertNotSame(0, info.getCreationTime());
      assertFalse(info.isBucket());
    }

    GoogleCloudStorageItemInfo nonExistentItemInfo = allInfo.get(allInfo.size() - 2);
    assertFalse(nonExistentItemInfo.exists());

    GoogleCloudStorageItemInfo bucketInfo = allInfo.get(allInfo.size() - 1);
    assertTrue(bucketInfo.exists());
    assertTrue(bucketInfo.isBucket());

    cleanupTestObjects(bucketName, objectsCreated);
  }

  @Test
  public void testGetMultipleItemInfoWithSomeInvalid() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("get_multi_inv_item");
    gcs.create(bucketName);

    List<StorageResourceId> resourceIdList = new ArrayList<>();
    StorageResourceId newOject = new StorageResourceId(bucketName, "CreatedObject");
    resourceIdList.add(newOject);
    gcs.createEmptyObject(newOject);

    StorageResourceId invalidObject = new StorageResourceId(bucketName, "InvalidObject\n");
    resourceIdList.add(invalidObject);

    try {
      expectedException.expect(IOException.class);
      expectedException.expectMessage("Error getting StorageObject");

      List<GoogleCloudStorageItemInfo> infoList = gcs.getItemInfos(resourceIdList);
      assertTrue(infoList.get(0).exists());
      assertFalse(infoList.get(1).exists());
    } finally {
      cleanupTestObjects(bucketName, newOject);
    }
  }

  @Test
  public void testOneInvalidGetItemInfo() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("get_invalid_item");
    gcs.create(bucketName);
    try {
      expectedException.expect(IOException.class);
      expectedException.expectMessage("Error accessing");

      GoogleCloudStorageItemInfo info =
          gcs.getItemInfo(new StorageResourceId(bucketName, "InvalidObject\n"));
    } finally {
      cleanupTestObjects(bucketName, new ArrayList<StorageResourceId>());
    }
  }

  @Test
  public void testSingleObjectDelete() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("delete_single_item");
    gcs.create(bucketName);

    StorageResourceId resource = new StorageResourceId(bucketName, "SomeItem");
    gcs.createEmptyObject(resource);

    GoogleCloudStorageItemInfo info = gcs.getItemInfo(resource);
    assertTrue(info.exists());

    gcs.deleteObjects(ImmutableList.of(resource));

    GoogleCloudStorageItemInfo deletedInfo = gcs.getItemInfo(resource);
    assertFalse(deletedInfo.exists());

    cleanupTestObjects(bucketName, ImmutableList.<StorageResourceId>of());
  }

  @Test
  public void testMultipleObjectDelete() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("delete_multi_item");
    gcs.create(bucketName);

    StorageResourceId resource = new StorageResourceId(bucketName, "SomeItem");
    gcs.createEmptyObject(resource);

    StorageResourceId secondResource = new StorageResourceId(bucketName, "SecondItem");
    gcs.createEmptyObject(secondResource);

    assertTrue(gcs.getItemInfo(resource).exists());
    assertTrue(gcs.getItemInfo(secondResource).exists());

    gcs.deleteObjects(ImmutableList.of(resource, secondResource));

    assertFalse(gcs.getItemInfo(resource).exists());
    assertFalse(gcs.getItemInfo(secondResource).exists());

    cleanupTestObjects(bucketName, ImmutableList.<StorageResourceId>of());
  }

  @Test
  public void testSomeInvalidObjectsDelete() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("delete_inv_item");
    gcs.create(bucketName);

    StorageResourceId resource = new StorageResourceId(bucketName, "SomeItem");
    gcs.createEmptyObject(resource);

    // Don't actually create a GCS object for this resource.
    StorageResourceId secondResource = new StorageResourceId(bucketName, "DoesntExit");
    StorageResourceId invalidName = new StorageResourceId(bucketName, "InvalidObject\n");

    assertTrue(gcs.getItemInfo(resource).exists());
    assertFalse(gcs.getItemInfo(secondResource).exists());

    try {
      expectedException.expect(IOException.class);
      expectedException.expectMessage("Error deleting");

      gcs.deleteObjects(ImmutableList.of(resource, secondResource, invalidName));
    } finally {
      cleanupTestObjects(bucketName, ImmutableList.of(resource, secondResource));
    }
  }

  @Test
  public void testDeleteNonExistingObject() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("delete_ne_item");
    gcs.create(bucketName);

    StorageResourceId resource = new StorageResourceId(bucketName, "SomeItem");

    gcs.deleteObjects(ImmutableList.of(resource));

    cleanupTestObjects(bucketName, resource);
  }

  @Test
  public void testDeleteNonExistingBucket() throws IOException {
    // Composite exception thrown, not a FileNotFoundException.
    expectedException.expect(IOException.class);
    String bucketName = bucketHelper.getUniqueBucketName("delete_ne_bucket");
    gcs.deleteBuckets(ImmutableList.of(bucketName));
  }

  @Test
  public void testSingleDeleteBucket() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("delete_1_bucket");
    gcs.create(bucketName);
    gcs.deleteBuckets(ImmutableList.of(bucketName));

    GoogleCloudStorageItemInfo info = gcs.getItemInfo(new StorageResourceId(bucketName));
    assertFalse(info.exists());

    // Create the bucket again to assure that the previous one was deleted...
    gcs.create(bucketName);

    cleanupTestObjects(bucketName, ImmutableList.<StorageResourceId>of());
  }

  @Test
  public void testMultipleDeleteBucket() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("delete_multi_bucket");
    String bucketName2 = bucketHelper.getUniqueBucketName("delete_multi_bucket2");

    gcs.create(bucketName);
    gcs.create(bucketName2);

    gcs.deleteBuckets(ImmutableList.of(bucketName, bucketName2));

    List<GoogleCloudStorageItemInfo> infoList = gcs.getItemInfos(ImmutableList.of(
        new StorageResourceId(bucketName), new StorageResourceId(bucketName2)));

    for (GoogleCloudStorageItemInfo info : infoList) {
      assertFalse(info.exists());
    }
  }

  @Test
  public void testSomeInvalidDeleteBucket() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("delete_multi_bucket");
    String bucketName2 = bucketHelper.getUniqueBucketName("delete_multi_bucket2");
    String invalidBucketName = "--invalid_delete_multi_bucket";

    gcs.create(bucketName);
    gcs.create(bucketName2);

    try {
      gcs.deleteBuckets(ImmutableList.of(bucketName, bucketName2, invalidBucketName));
      // Expected exception would be a bit more awkward than Assert.fail() with a catch here...
      fail("Delete buckets with an invalid bucket should throw.");
    } catch (IOException ioe) {
      // Expected.
    }

    List<GoogleCloudStorageItemInfo> infoList = gcs.getItemInfos(ImmutableList.of(
        new StorageResourceId(bucketName), new StorageResourceId(bucketName2)));

    for (GoogleCloudStorageItemInfo info : infoList) {
      assertFalse(info.exists());
    }
  }

  @Test
  public void testListBucketInfo() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("test_list_bucketinfo");
    gcs.create(bucketName);
    List<GoogleCloudStorageItemInfo> infoList = gcs.listBucketInfo();
    // This has potential to become flaky...
    assertTrue("At least one bucket should exist", infoList.size() > 0);

    for (GoogleCloudStorageItemInfo info : infoList) {
      assertTrue(info.exists());
      assertTrue(info.isBucket());
      assertFalse(info.isRoot());
    }

    cleanupTestObjects(bucketName, ImmutableList.<StorageResourceId>of());
  }

  @Test
  public void testListBucketNames() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("test_list_bucketinfo");
    gcs.create(bucketName);

    List<String> bucketNames = gcs.listBucketNames();

    assertFalse("Bucket names should not be empty", bucketNames.isEmpty());

    cleanupTestObjects(bucketName, ImmutableList.<StorageResourceId>of());
  }

  @Test
  public void testListObjectInfoWithDirectoryRepair() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("list_repair");
    gcs.create(bucketName);

    List<StorageResourceId> testOjects = new ArrayList<>();

    StorageResourceId d1 = new StorageResourceId(bucketName, "d1/");
    gcs.createEmptyObject(d1);
    testOjects.add(d1);

    StorageResourceId o1 = new StorageResourceId(bucketName, "d1/o1");
    gcs.createEmptyObject(o1);
    testOjects.add(o1);

    // No empty d2/ prefix:
    StorageResourceId d3 = new StorageResourceId(bucketName, "d2/d3/");
    gcs.createEmptyObject(d3);
    testOjects.add(d3);

    StorageResourceId o2 = new StorageResourceId(bucketName, "d2/d3/o2");
    gcs.createEmptyObject(o2);
    testOjects.add(o2);

    GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(new StorageResourceId(bucketName, "d2/"));
    assertFalse(itemInfo.exists());

    List<GoogleCloudStorageItemInfo> rootInfo =  gcs.listObjectInfo(bucketName, null, "/");

    // Specifying any exact values seems like it's begging for this test to become flaky.
    assertFalse("Infos not expected to be empty", rootInfo.isEmpty());

    // Directory repair should have created an empty object for us:
    StorageResourceId d2 = new StorageResourceId(bucketName, "d2/");
    testOjects.add(d2);
    GoogleCloudStorageTestHelper.assertObjectContent(gcs, d2, new byte[0]);

    List<GoogleCloudStorageItemInfo> d2ItemInfo = gcs.listObjectInfo(bucketName, "d2/d3/", "/");
    assertFalse("D2 item info not expected to be empty", d2ItemInfo.isEmpty());

    // Testing GCS treating object names as opaque blobs
    List<GoogleCloudStorageItemInfo> blobNamesInfo = gcs.listObjectInfo(bucketName, null, null);
    assertFalse("blobNamesInfo not expected to be empty", blobNamesInfo.isEmpty());

    cleanupTestObjects(bucketName, testOjects);
  }

  @Test
  public void testCopySingleItem() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("copy_1_item");
    gcs.create(bucketName);

    byte[] bytesToWrite = new byte[4096];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "CopyTestObject");
    try (WritableByteChannel channel = gcs.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);

    StorageResourceId copiedResourceId = new StorageResourceId(bucketName, "CopiedObject");
    // Do the copy:
    gcs.copy(
        bucketName,
        ImmutableList.of("CopyTestObject"),
        bucketName,
        ImmutableList.of(copiedResourceId.getObjectName()));

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, copiedResourceId, bytesToWrite);

    cleanupTestObjects(
        bucketName,
        ImmutableList.of(objectToCreate, copiedResourceId));
  }


  @Test
  public void testCopyToDifferentBucket() throws IOException {
    String sourceBucketName = bucketHelper.getUniqueBucketName("copy_src_bucket");
    String destinationBucketName = bucketHelper.getUniqueBucketName("copy_dst_bucket");

    gcs.create(sourceBucketName);
    gcs.create(destinationBucketName);

    byte[] bytesToWrite = new byte[4096];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(sourceBucketName, "CopyTestObject");
    try (WritableByteChannel channel = gcs.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);

    StorageResourceId copiedResourceId =
        new StorageResourceId(destinationBucketName, "CopiedObject");

    // Do the copy:
    gcs.copy(
        sourceBucketName,
        ImmutableList.of(objectToCreate.getObjectName()),
        destinationBucketName,
        ImmutableList.of(copiedResourceId.getObjectName()));

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, copiedResourceId, bytesToWrite);

    cleanupTestObjects(
        sourceBucketName,
        ImmutableList.of(objectToCreate, copiedResourceId));
  }

  @Test
  public void testCopySingleItemOverExistingItem() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("copy_1_exists");
    gcs.create(bucketName);

    byte[] bytesToWrite = new byte[4096];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    StorageResourceId objectToCopy = new StorageResourceId(bucketName, "CopyTestObject");
    try (WritableByteChannel channel = gcs.create(objectToCopy)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCopy, bytesToWrite);

    StorageResourceId secondObject = new StorageResourceId(bucketName, "CopyTestObject2");
    byte[] secondObjectBytes = new byte[2046];
    GoogleCloudStorageTestHelper.fillBytes(secondObjectBytes);
    try (WritableByteChannel channel = gcs.create(secondObject)) {
      channel.write(ByteBuffer.wrap(secondObjectBytes));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, secondObject, secondObjectBytes);

    gcs.copy(
        bucketName,
        ImmutableList.of(objectToCopy.getObjectName()),
        bucketName,
        ImmutableList.of(secondObject.getObjectName()));

    // Second object should now have the bytes of the first.
    GoogleCloudStorageTestHelper.assertObjectContent(gcs, secondObject, bytesToWrite);

    cleanupTestObjects(
        bucketName,
        ImmutableList.of(objectToCopy, secondObject));
  }

  @Test
  public void testCopySingleItemOverItself() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("copy_item_1_1");
    gcs.create(bucketName);

    byte[] bytesToWrite = new byte[4096];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    StorageResourceId objectToCopy = new StorageResourceId(bucketName, "CopyTestObject");
    try (WritableByteChannel channel = gcs.create(objectToCopy)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCopy, bytesToWrite);
    try {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Copy destination must be different");

      gcs.copy(
          bucketName,
          ImmutableList.of(objectToCopy.getObjectName()),
          bucketName,
          ImmutableList.of(objectToCopy.getObjectName()));
    } finally {
      cleanupTestObjects(
          bucketName,
          ImmutableList.of(objectToCopy));
    }
  }

  static class CopyObjectData {
    public final StorageResourceId sourceResourceId;
    public final StorageResourceId destinationResourceId;
    public final byte[] objectBytes;

    CopyObjectData(
        StorageResourceId sourceResourceId,
        StorageResourceId destinationResourceId,
        byte[] objectBytes) {
      this.sourceResourceId = sourceResourceId;
      this.destinationResourceId = destinationResourceId;
      this.objectBytes = objectBytes;
    }
  }

  @Test
  public void testCopyMultipleItems() throws IOException {
    final int copyObjectCount = 3;
    String bucketName = bucketHelper.getUniqueBucketName("copy_multi_item");
    gcs.create(bucketName);

    List<CopyObjectData> objectsToCopy = new ArrayList<>();
    for (int i = 0; i < copyObjectCount; i++) {
      String sourceObjectName = "sourceObject_" + i;
      String destinationObjectName = "destinationObject_" + i;
      byte[] objectBytes = new byte[1024 * i];
      GoogleCloudStorageTestHelper.fillBytes(objectBytes);

      try (WritableByteChannel channel =
          gcs.create(new StorageResourceId(bucketName, sourceObjectName))) {
        channel.write(ByteBuffer.wrap(objectBytes));
      }

      objectsToCopy.add(
          new CopyObjectData(
              new StorageResourceId(bucketName, sourceObjectName),
              new StorageResourceId(bucketName, destinationObjectName),
              objectBytes));
    }

    List<String> sourceObjects =
        Lists.transform(objectsToCopy, new Function<CopyObjectData, String>(){
      @Override
      public String apply(CopyObjectData copyObjectData) {
        return copyObjectData.sourceResourceId.getObjectName();
      }
    });

    List<String> destinationObjects =
        Lists.transform(objectsToCopy, new Function<CopyObjectData, String>(){
      @Override
      public String apply(CopyObjectData copyObjectData) {
        return copyObjectData.destinationResourceId.getObjectName();
      }
    });

    gcs.copy(bucketName, sourceObjects, bucketName, destinationObjects);

    for (CopyObjectData copyObjectData : objectsToCopy) {
      GoogleCloudStorageTestHelper
          .assertObjectContent(gcs, copyObjectData.sourceResourceId, copyObjectData.objectBytes);
      GoogleCloudStorageTestHelper.assertObjectContent(gcs, copyObjectData.destinationResourceId,
          copyObjectData.objectBytes);
    }

    List<StorageResourceId> objectsToCleanup = new ArrayList<>(copyObjectCount * 2);
    for (CopyObjectData copyObjectData : objectsToCopy) {
      objectsToCleanup.add(copyObjectData.sourceResourceId);
      objectsToCleanup.add(copyObjectData.destinationResourceId);
    }

    cleanupTestObjects(bucketName, objectsToCleanup);
  }

  @Test
  public void testCopyNonExistentItem() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("copy_item_ne");
    gcs.create(bucketName);
    String notExistentName = "IDontExist";

    try {
      expectedException.expect(FileNotFoundException.class);

      gcs.copy(
          bucketName,
          ImmutableList.of(notExistentName),
          bucketName,
          ImmutableList.of("Some_destination"));
    } finally {
      cleanupTestObjects(bucketName, ImmutableList.<StorageResourceId>of());
    }
  }

  @Test
  public void testCopyMultipleItemsToSingleDestination() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("copy_multi_2_1");
    gcs.create(bucketName);

    try {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Must supply same number of elements");

      gcs.copy(
          bucketName,
          ImmutableList.of("SomeSource", "SomeSource2"),
          bucketName,
          ImmutableList.of("Some_destination"));

      fail("Copying multiple items to a single source should fail.");
    }  finally {
      cleanupTestObjects(bucketName, ImmutableList.<StorageResourceId>of());
    }
  }

  @Test
  public void testOpen() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("open_object");
    gcs.create(bucketName);

    byte[] bytesToWrite = new byte[100];

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "OpenTestObject");
    try (WritableByteChannel channel = gcs.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);

    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testOpenNonExistentItem() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("open_ne");
    gcs.create(bucketName);

    try {
      expectedException.expect(FileNotFoundException.class);

      ReadableByteChannel channel = gcs.open(new StorageResourceId(bucketName, "AnObject"));

      fail("Exception expected from opening an non-existent object");
    } finally {
      cleanupTestObjects(bucketName, ImmutableList.<StorageResourceId>of());
    }
  }

  @Test
  public void testOpenEmptyObject() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("open_empty");
    gcs.create(bucketName);

    StorageResourceId resourceId = new StorageResourceId(bucketName, "EmptyObject");
    gcs.createEmptyObject(resourceId);
    GoogleCloudStorageTestHelper.assertObjectContent(gcs, resourceId, new byte[0]);

    cleanupTestObjects(bucketName, resourceId);
  }

  @Test
  public void testOpenLargeObject() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("large_object");

    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "LargeObject");

    gcs.create(bucketName);

    GoogleCloudStorageTestHelper.readAndWriteLargeObject(objectToCreate, gcs);

    cleanupTestObjects(objectToCreate.getBucketName(), objectToCreate);
  }

  @Test
  public void testPlusInObjectNames() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("plus_in_name");
    gcs.create(bucketName);

    StorageResourceId resourceId = new StorageResourceId(bucketName, "an+object");
    gcs.createEmptyObject(resourceId);
    GoogleCloudStorageTestHelper.assertObjectContent(gcs, resourceId, new byte[0]);

    cleanupTestObjects(bucketName, resourceId);
  }

  @Test
  public void testObjectPosition() throws IOException {

    final int totalBytes = 1200;
    byte[] data = new byte[totalBytes];
    GoogleCloudStorageTestHelper.fillBytes(data);

    String bucketName = bucketHelper.getUniqueBucketName("object_position");
    gcs.create(bucketName);

    StorageResourceId resourceId = new StorageResourceId(bucketName, "SeekTestObject");
    try (WritableByteChannel channel = gcs.create(resourceId)) {
      channel.write(ByteBuffer.wrap(data));
    }

    byte[] readBackingArray = new byte[totalBytes];
    ByteBuffer readBuffer = ByteBuffer.wrap(readBackingArray);
    try (SeekableReadableByteChannel readChannel = gcs.open(resourceId)) {
      assertEquals("Expected new file to open at position 0", 0, readChannel.position());
      assertEquals("Unexpected readChannel.size()", totalBytes, readChannel.size());

      readBuffer.limit(4);
      int bytesRead = readChannel.read(readBuffer);
      assertEquals("Unexpected number of bytes read", 4, bytesRead);
      assertEquals("Unexpected position after read()", 4, readChannel.position());

      readChannel.position(4);
      assertEquals("Unexpected position after no-op", 4, readChannel.position());

      readChannel.position(6);
      assertEquals("Unexpected position after explicit position(6)", 6, readChannel.position());

      readChannel.position(data.length - 1);
      assertEquals(
          "Unexpected position after seek to EOF - 1",
          data.length - 1,
          readChannel.position());
      readBuffer.clear();
      bytesRead = readChannel.read(readBuffer);
      assertEquals("Expected to read 1 byte", 1, bytesRead);
      assertEquals(
          "Unexpected data read for last byte",
          data[data.length - 1],
          readBackingArray[0]);

      bytesRead = readChannel.read(readBuffer);
      assertEquals(
          "Expected to read -1 bytes for EOF marker",
          -1,
          bytesRead);

      readChannel.position(0);
      assertEquals(
          "Unexpected position after reset to 0",
          0,
          readChannel.position());

      try {
        readChannel.position(-1);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
        // Expected.
      }

      try {
        readChannel.position(totalBytes);
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
        // Expected.
      }
    }

    cleanupTestObjects(bucketName, resourceId);
  }

  @Test
  public void testReadPartialObjects() throws IOException {
    final int segmentSize = 553;
    final int segmentCount = 5;
    byte[] data = new byte[segmentCount * segmentSize];
    GoogleCloudStorageTestHelper.fillBytes(data);

    String bucketName = bucketHelper.getUniqueBucketName("read_partial");
    gcs.create(bucketName);

    StorageResourceId resourceId = new StorageResourceId(bucketName, "ReadPartialTest");
    try (WritableByteChannel channel = gcs.create(resourceId)) {
      channel.write(ByteBuffer.wrap(data));
    }

    byte[][] readSegments = new byte[segmentCount][segmentSize];
    try (SeekableReadableByteChannel readChannel = gcs.open(resourceId)) {
      for (int i = 0; i < segmentCount; i++) {
        ByteBuffer segmentBuffer = ByteBuffer.wrap(readSegments[i]);
        int bytesRead = readChannel.read(segmentBuffer);
        assertEquals(segmentSize, bytesRead);
        byte[] expectedSegment = Arrays.copyOfRange(
            data,
            i * segmentSize, /* from index */
            (i * segmentSize) + segmentSize /* to index */);
        assertArrayEquals("Unexpected segment data read.", expectedSegment, readSegments[i]);
      }
    }

    cleanupTestObjects(bucketName, resourceId);
  }

  @Test
  public void testSpecialResourceIds() throws IOException {
    assertEquals("Unexpected ROOT item info returned",
          GoogleCloudStorageItemInfo.ROOT_INFO,
          gcs.getItemInfo(StorageResourceId.ROOT));

    expectedException.expect(IllegalArgumentException.class);
    StorageResourceId.createReadableString(null, "objectName");
  }

  @Test
  public void testChannelClosedException() throws IOException {
    final int totalBytes = 1200;
    byte[] data = new byte[totalBytes];
    GoogleCloudStorageTestHelper.fillBytes(data);

    String bucketName = bucketHelper.getUniqueBucketName("channel_closed");
    gcs.create(bucketName);

    StorageResourceId resourceId = new StorageResourceId(bucketName, "ReadClosedChannelTest");
    try (WritableByteChannel channel = gcs.create(resourceId)) {
      channel.write(ByteBuffer.wrap(data));
    }

    try {
      byte[] readArray = new byte[totalBytes];
      SeekableReadableByteChannel readableByteChannel = gcs.open(resourceId);
      ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
      readBuffer.limit(5);
      readableByteChannel.read(readBuffer);
      assertEquals(readBuffer.position(), readableByteChannel.position());

      expectedException.expect(ClosedChannelException.class);

      readableByteChannel.close();
      readBuffer.clear();
      readableByteChannel.read(readBuffer);
    } finally {
      cleanupTestObjects(bucketName, resourceId);
    }
  }

  @Test @Ignore("Not implemented")
  public void testOperationsAfterCloseFail() {

  }
}
