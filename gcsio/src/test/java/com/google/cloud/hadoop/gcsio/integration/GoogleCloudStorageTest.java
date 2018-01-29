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

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.CacheSupplementedGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.FileSystemBackedDirectoryListCache;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageWriteChannel;
import com.google.cloud.hadoop.gcsio.InMemoryDirectoryListCache;
import com.google.cloud.hadoop.gcsio.LaggedGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.LaggedGoogleCloudStorage.ListVisibilityCalculator;
import com.google.cloud.hadoop.gcsio.ListProhibitedGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.ResourceLoggingGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.UpdatableItemInfo;
import com.google.cloud.hadoop.gcsio.VerificationAttributes;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.testing.InMemoryGoogleCloudStorage;
import com.google.common.base.Equivalence;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class GoogleCloudStorageTest {

  /**
   * An Equivalence for byte arrays.
   */
  public static final Equivalence<byte[]> BYTE_ARRAY_EQUIVALENCE = new Equivalence<byte[]>() {
    @Override
    protected boolean doEquivalent(byte[] bytes, byte[] bytes2) {
      return Arrays.equals(bytes, bytes2);
    }

    @Override
    protected int doHash(byte[] bytes) {
      return Arrays.hashCode(bytes);
    }
  };

  /**
   * A method for defining a scope / block of code that should share a single bucket for tests
   * and that when the scope is exited the resources should be freed or deleted. The TestbucketScope
   * is not appropriate for all use cases; e.g., when the test requires multiple buckets or when
   * the test is around creating and deleting buckets.
   */
  public static interface TestBucketScope extends AutoCloseable {

    /**
     * The @{link GoogleCloudStorage} instance to use when interfacing with this scope. Each
     * scope may wrap the raw implementation of GoogleCloudStorage in order to record resources
     * created or prevent certain methods from executing.
     */
    GoogleCloudStorage getStorageInstance();

    /**
     * Get the unique bucket name for use in this scope.
     */
    String getBucketName();

    /** Cleanup resources used by this scope. */
    @Override
    void close() throws IOException;
  }

  /**
   * A @{link TestBucketScope} that shares a single bucket for all instance in the same JVM. Due
   * to the shared nature of this scope, the GCS instance returned by getStorageInstance will not
   * permit the use of any inconsistent List methods or methods that depend on inconsistent lists.
   * The shared bucket to be used by all instances will be created when the first instance is
   * created. Note that these shared buckets will remain until cleanupTestBuckets is invoked,
   */
  public static class SharedBucketScope implements TestBucketScope {
    static Map<GoogleCloudStorage, String> createdBucketMap = new HashMap<>();

    public static void cleanupTestBuckets() throws IOException {
      for (Map.Entry<GoogleCloudStorage, String> toCleanup : createdBucketMap.entrySet()) {
        try {
          toCleanup.getKey().deleteBuckets(ImmutableList.of(toCleanup.getValue()));
        } catch (IOException ioe) {
          // List inconsistency can cause bucket delete to fail.
        }
      }
    }

    protected final ResourceLoggingGoogleCloudStorage gcs;
    protected final GoogleCloudStorage rawStorage;
    protected String sharedBucketName;

    public SharedBucketScope(GoogleCloudStorage gcs) throws IOException {
      this.rawStorage = gcs;
      this.gcs = new ResourceLoggingGoogleCloudStorage(new ListProhibitedGoogleCloudStorage(gcs));
      this.sharedBucketName = createSharedBucketIfRequired(rawStorage);
    }

    private static String createSharedBucketIfRequired(GoogleCloudStorage rawStorage)
        throws IOException {
        synchronized (createdBucketMap) {
          if (!createdBucketMap.containsKey(rawStorage)) {
            String name = String.format("%s-%s",
                TestBucketHelper.makeBucketName("gcs_it"),
                "shared");
            rawStorage.create(name);
            createdBucketMap.put(rawStorage, name);
          }

          return createdBucketMap.get(rawStorage);
        }
    }

    @Override
    public GoogleCloudStorage getStorageInstance() {
      return gcs;
    }

    @Override
    public String getBucketName() {
      return sharedBucketName;
    }

    @Override
    public void close() throws IOException {
      List<StorageResourceId> createdResources = gcs.getCreatedResources();
      GoogleCloudStorageTestHelper.cleanupTestObjects(
          gcs, ImmutableList.<String>of(), createdResources);
    }
  }

  public static class UniqueBucketScope implements TestBucketScope {
    protected final ResourceLoggingGoogleCloudStorage gcs;
    private final String bucketName;

    public UniqueBucketScope(GoogleCloudStorage gcs, String bucketSuffix)
        throws IOException {
      this.gcs = new ResourceLoggingGoogleCloudStorage(gcs);
      this.bucketName = String.format("%s-%s",
          TestBucketHelper.makeBucketName("gcs_it"),
          bucketSuffix);
      gcs.create(bucketName);
    }

    @Override
    public GoogleCloudStorage getStorageInstance() {
      return gcs;
    }

    @Override
    public String getBucketName() {
      return bucketName;
    }

    @Override
    public void close() throws IOException {
      GoogleCloudStorageTestHelper.cleanupTestObjects(
          gcs,
          ImmutableList.of(bucketName),
          gcs.getCreatedResources());
    }
  }

  /**
   * Static instance which we can use inside long-lived GoogleCloudStorage instances, but still
   * may reconfigure to point to new temporary directories in each test case.
   */
  protected static FileSystemBackedDirectoryListCache fileBackedCache =
      FileSystemBackedDirectoryListCache.getUninitializedInstanceForTest();

  // Test classes using JUnit4 runner must have only a single constructor. Since we
  // want to be able to pass in dependencies, we'll maintain this base class as
  // @Parameterized with @Parameters.
  @Parameters
  public static Collection<Object[]> getConstructorArguments() throws IOException {
    GoogleCloudStorage gcs =
        new InMemoryGoogleCloudStorage();
    GoogleCloudStorage zeroLaggedGcs =
        new LaggedGoogleCloudStorage(
            new InMemoryGoogleCloudStorage(),
            Clock.SYSTEM,
            ListVisibilityCalculator.IMMEDIATELY_VISIBLE);
    GoogleCloudStorage cachedGcs =
        new CacheSupplementedGoogleCloudStorage(
            new InMemoryGoogleCloudStorage(),
            InMemoryDirectoryListCache.getInstance());
    GoogleCloudStorage cachedLaggedGcs =
        new CacheSupplementedGoogleCloudStorage(
          new LaggedGoogleCloudStorage(
              new InMemoryGoogleCloudStorage(),
              Clock.SYSTEM,
              ListVisibilityCalculator.DEFAULT_LAGGED),
          InMemoryDirectoryListCache.getInstance());
    GoogleCloudStorage cachedFilebackedLaggedGcs =
        new CacheSupplementedGoogleCloudStorage(
          new LaggedGoogleCloudStorage(
              new InMemoryGoogleCloudStorage(),
              Clock.SYSTEM,
              ListVisibilityCalculator.DEFAULT_LAGGED),
          fileBackedCache);
    return Arrays.asList(new Object[][]{
        {gcs},
        {zeroLaggedGcs},
        {cachedGcs},
        {cachedLaggedGcs},
        {cachedFilebackedLaggedGcs}
    });
  }

  @Rule
  public TemporaryFolder tempDirectoryProvider = new TemporaryFolder();

  private final GoogleCloudStorage rawStorage;
  private final TestBucketHelper bucketHelper;

  public GoogleCloudStorageTest(GoogleCloudStorage rawStorage) {
    this.bucketHelper = new TestBucketHelper("gcs_it");
    this.rawStorage = rawStorage;
  }

  @Before
  public void setUp() throws IOException {
    // Point the shared static cache instance at a new temp directory.
    fileBackedCache.setBasePath(tempDirectoryProvider.newFolder("gcs-metadata").toString());
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
    GoogleCloudStorageTestHelper.cleanupTestObjects(rawStorage, bucketNames, resources);
  }

  @After
  public void cleanup() throws IOException {
    bucketHelper.cleanupTestObjects(rawStorage);
  }

  @AfterClass
  public static void cleanupSharedBuckets() throws IOException {
    SharedBucketScope.cleanupTestBuckets();
  }

  @Test
  public void testCreateSuccessfulBucket() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_successful");
    rawStorage.create(bucketName);

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(bucketName, "CreateTestObject");
    rawStorage.createEmptyObject(objectToCreate);

    cleanupTestObjects(bucketName, objectToCreate);
  }

  @Test
  public void testCreateExistingBucket() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("create_existing");
    rawStorage.create(bucketName);

    try {
      rawStorage.create(bucketName);
      fail();
    } catch (IOException e) {
      // expected
    } finally {
      rawStorage.deleteBuckets(ImmutableList.of(bucketName));
    }
  }

  @Test
  public void testCreateInvalidBucket() throws IOException {
    // Buckets must start with a letter or number
    String bucketName = "--" + bucketHelper.getUniqueBucketName("create_invalid");

    try {
      rawStorage.create(bucketName);
      fail();
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testCreateObject() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      byte[] bytesToWrite = "SomeText".getBytes(StandardCharsets.UTF_8);
      // Verify the bucket exist by creating an object
      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testCreateObject_CreateTestObject");
      try (WritableByteChannel channel = gcs.create(objectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);
    }
  }

  @Test
  public void testCreateInvalidObject() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      byte[] bytesToWrite = "SomeText".getBytes(StandardCharsets.UTF_8);
      // Verify the bucket exist by creating an object
      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testCreateObject_CreateInvalidTestObject\n");

      try {
        try (WritableByteChannel channel = gcs.create(objectToCreate)) {
          channel.write(ByteBuffer.wrap(bytesToWrite));
        }
        fail();
      } catch (IOException e) {
        // expected
      }
    }
  }

  @Test
  public void testCreateZeroLengthObjectUsingCreate() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      byte[] bytesToWrite = new byte[0];

      StorageResourceId objectToCreate = new StorageResourceId(
          bucketName, "testCreateZeroLengthObjectUsingCreate_CreateEmptyTestObject");
      try (WritableByteChannel channel = gcs.create(objectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);
    }
  }

  @Test
  public void testCreate1PageLengthObjectUsingCreate() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      byte[] bytesToWrite =
          new byte[GoogleCloudStorageWriteChannel.UPLOAD_PIPE_BUFFER_SIZE_DEFAULT];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

      StorageResourceId objectToCreate = new StorageResourceId(
          bucketName, "testCreate1PageLengthObjectUsingCreate_Create1PageTestObject");
      try (WritableByteChannel channel = gcs.create(objectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);
    }
  }

  @Test
  public void testCreate1PageLengthPlus1byteObjectUsingCreate() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      byte[] bytesToWrite =
          new byte[GoogleCloudStorageWriteChannel.UPLOAD_PIPE_BUFFER_SIZE_DEFAULT + 1];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

      StorageResourceId objectToCreate = new StorageResourceId(
          bucketName,
          "testCreate1PageLengthPlus1byteObjectUsingCreate_Create1PagePlusOneTestObject");

      try (WritableByteChannel channel = gcs.create(objectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);
    }
  }

  @Test
  public void testCreateExistingObject() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      byte[] bytesToWrite = "SomeText".getBytes(StandardCharsets.UTF_8);

      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testCreateExistingObject_CreateExistingObject");

      try (WritableByteChannel channel = gcs.create(objectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      byte[] overwriteBytesToWrite = "OverwriteText".getBytes(StandardCharsets.UTF_8);

      // We need to write data and close to trigger an IOException.
      try (WritableByteChannel channel2 = gcs.create(objectToCreate)) {
        channel2.write(ByteBuffer.wrap(overwriteBytesToWrite));
      }

      GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, overwriteBytesToWrite);
    }
  }

  @Test
  public void testCreateEmptyObject() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testCreateEmptyObject_CreateEmptyObject");

      gcs.createEmptyObject(objectToCreate);

      GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(objectToCreate);

      assertTrue(itemInfo.exists());
      assertEquals(0, itemInfo.getSize());
    }
  }

  @Test
  public void testCreateEmptyExistingObject() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId objectToCreate = new StorageResourceId(
          bucketName, "testCreateEmptyExistingObject_CreateEmptyExistingObject");

      gcs.createEmptyObject(objectToCreate);

      GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(objectToCreate);

      assertTrue(itemInfo.exists());
      assertEquals(0, itemInfo.getSize());

      gcs.createEmptyObject(objectToCreate);

      GoogleCloudStorageItemInfo secondItemInfo = gcs.getItemInfo(objectToCreate);

      assertTrue(secondItemInfo.exists());
      assertEquals(0, secondItemInfo.getSize());
      assertNotSame(itemInfo.getCreationTime(), secondItemInfo.getCreationTime());
    }
  }

  @Test
  public void testGetSingleItemInfo() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testGetSingleItemInfo_GetSingleItemInfoObject");

      gcs.createEmptyObject(objectToCreate);

      GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(objectToCreate);

      assertTrue(itemInfo.exists());
      assertEquals(0, itemInfo.getSize());

      StorageResourceId secondObjectToCreate =
          new StorageResourceId(bucketName, "testGetSingleItemInfo_GetSingleItemInfoObject2");

      byte[] bytesToWrite = new byte[100];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

      try (WritableByteChannel channel = gcs.create(secondObjectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageItemInfo secondItemInfo = gcs.getItemInfo(secondObjectToCreate);

      assertTrue(secondItemInfo.exists());
      assertEquals(100, secondItemInfo.getSize());
      assertFalse(secondItemInfo.isBucket());
      assertFalse(secondItemInfo.isRoot());

      GoogleCloudStorageItemInfo nonExistentItemInfo =
          gcs.getItemInfo(
              new StorageResourceId(bucketName, "testGetSingleItemInfo_SomethingThatDoesntExist"));

      assertFalse(nonExistentItemInfo.exists());
      assertFalse(nonExistentItemInfo.isBucket());
      assertFalse(nonExistentItemInfo.isRoot());

      // Test bucket get item info
      GoogleCloudStorageItemInfo bucketInfo = gcs.getItemInfo(new StorageResourceId(bucketName));
      assertTrue(bucketInfo.exists());
      assertTrue(bucketInfo.isBucket());

      GoogleCloudStorageItemInfo rootInfo =
          gcs.getItemInfo(StorageResourceId.ROOT);
      assertTrue(rootInfo.exists());
      assertTrue(rootInfo.isRoot());
    }
  }

  @Test
  public void testGetMultipleItemInfo() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      List<StorageResourceId> objectsCreated = new ArrayList<>();

      for (int i = 0; i < 3; i++) {
        StorageResourceId objectToCreate = new StorageResourceId(
            bucketName, String.format("testGetMultipleItemInfo_GetMultiItemInfoObject_%s", i));
        gcs.createEmptyObject(objectToCreate);
        objectsCreated.add(objectToCreate);
      }

      StorageResourceId bucketResourceId = new StorageResourceId(bucketName);
      StorageResourceId nonExistentResourceId =
          new StorageResourceId(bucketName, "testGetMultipleItemInfo_IDontExist");

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
    }
  }

  // TODO(user): Re-enable once a new method of inducing errors is devised.
  @Test @Ignore
  public void testGetMultipleItemInfoWithSomeInvalid() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      List<StorageResourceId> resourceIdList = new ArrayList<>();
      StorageResourceId newOject =
          new StorageResourceId(bucketName, "testGetMultipleItemInfoWithSomeInvalid_CreatedObject");
      resourceIdList.add(newOject);
      gcs.createEmptyObject(newOject);

      StorageResourceId invalidObject = new StorageResourceId(
          bucketName, "testGetMultipleItemInfoWithSomeInvalid_InvalidObject\n");
      resourceIdList.add(invalidObject);

      try {
        gcs.getItemInfos(resourceIdList);
        fail();
      } catch (IOException e) {
        assertThat(e).hasMessageThat().isEqualTo("Error getting StorageObject");
      }
    }
  }

  // TODO(user): Re-enable once a new method of inducing errors is devised.
  @Test @Ignore
  public void testOneInvalidGetItemInfo() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      try {
        gcs.getItemInfo(
            new StorageResourceId(bucketName, "testOneInvalidGetItemInfo_InvalidObject\n"));
        fail();
      } catch (IOException e) {
        assertThat(e).hasMessageThat().isEqualTo("Error accessing");
      }
    }
  }

  @Test
  public void testSingleObjectDelete() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId resource =
          new StorageResourceId(bucketName, "testSingleObjectDelete_SomeItem");
      gcs.createEmptyObject(resource);

      GoogleCloudStorageItemInfo info = gcs.getItemInfo(resource);
      assertTrue(info.exists());

      gcs.deleteObjects(ImmutableList.of(resource));

      GoogleCloudStorageItemInfo deletedInfo = gcs.getItemInfo(resource);
      assertFalse(deletedInfo.exists());
    }
  }

  @Test
  public void testMultipleObjectDelete() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId resource =
          new StorageResourceId(bucketName, "testMultipleObjectDelete_MultiDeleteSomeItem1");
      gcs.createEmptyObject(resource);

      StorageResourceId secondResource =
          new StorageResourceId(bucketName, "testMultipleObjectDelete_MultiDeleteSecondItem");
      gcs.createEmptyObject(secondResource);

      assertTrue(gcs.getItemInfo(resource).exists());
      assertTrue(gcs.getItemInfo(secondResource).exists());

      gcs.deleteObjects(ImmutableList.of(resource, secondResource));

      assertFalse(gcs.getItemInfo(resource).exists());
      assertFalse(gcs.getItemInfo(secondResource).exists());
    }
  }

  // TODO(user): Re-enable once a new method of inducing errors is devised.
  @Test @Ignore
  public void testSomeInvalidObjectsDelete() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId resource =
          new StorageResourceId(bucketName, "testSomeInvalidObjectsDelete_SomeItem");
      gcs.createEmptyObject(resource);

      // Don't actually create a GCS object for this resource.
      StorageResourceId secondResource =
          new StorageResourceId(bucketName, "testSomeInvalidObjectsDelete_DoesntExit");
      StorageResourceId invalidName =
          new StorageResourceId(bucketName, "testSomeInvalidObjectsDelete_InvalidObject\n");

      assertTrue(gcs.getItemInfo(resource).exists());
      assertFalse(gcs.getItemInfo(secondResource).exists());

      try {
        gcs.deleteObjects(ImmutableList.of(resource, secondResource, invalidName));
        fail();
      } catch (IOException e) {
        assertThat(e).hasMessageThat().isEqualTo("Error deleting");
      }
    }
  }

  @Test
  public void testDeleteNonExistingObject() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId resource = new StorageResourceId(
          bucketName, "testDeleteNonExistingObject_DeleteNonExistantItemTest");

      gcs.deleteObjects(ImmutableList.of(resource));
    }
  }

  @Test
  public void testDeleteNonExistingBucket() throws IOException {
    // Composite exception thrown, not a FileNotFoundException.
    String bucketName = bucketHelper.getUniqueBucketName("delete_ne_bucket");

    try {
      rawStorage.deleteBuckets(ImmutableList.of(bucketName));
      fail();
    } catch (IOException e) {
      // expected
    }
  }

  @Test
  public void testSingleDeleteBucket() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("delete_1_bucket");
    rawStorage.create(bucketName);
    rawStorage.deleteBuckets(ImmutableList.of(bucketName));

    GoogleCloudStorageItemInfo info = rawStorage.getItemInfo(new StorageResourceId(bucketName));
    assertFalse(info.exists());

    // Create the bucket again to assure that the previous one was deleted...
    rawStorage.create(bucketName);
    rawStorage.deleteBuckets(ImmutableList.of(bucketName));
  }

  @Test
  public void testMultipleDeleteBucket() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("delete_multi_bucket");
    String bucketName2 = bucketHelper.getUniqueBucketName("delete_multi_bucket2");

    rawStorage.create(bucketName);
    rawStorage.create(bucketName2);

    rawStorage.deleteBuckets(ImmutableList.of(bucketName, bucketName2));

    List<GoogleCloudStorageItemInfo> infoList = rawStorage.getItemInfos(ImmutableList.of(
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

    rawStorage.create(bucketName);
    rawStorage.create(bucketName2);

    try {
      rawStorage.deleteBuckets(ImmutableList.of(bucketName, bucketName2, invalidBucketName));
      // Expected exception would be a bit more awkward than Assert.fail() with a catch here...
      fail("Delete buckets with an invalid bucket should throw.");
    } catch (IOException ioe) {
      // Expected.
    }

    List<GoogleCloudStorageItemInfo> infoList = rawStorage.getItemInfos(ImmutableList.of(
        new StorageResourceId(bucketName), new StorageResourceId(bucketName2)));

    for (GoogleCloudStorageItemInfo info : infoList) {
      assertFalse(info.exists());
    }
  }

  @Test
  public void testListBucketInfo() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("test_list_bucketinfo");
    rawStorage.create(bucketName);
    List<GoogleCloudStorageItemInfo> infoList = rawStorage.listBucketInfo();
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
    rawStorage.create(bucketName);

    List<String> bucketNames = rawStorage.listBucketNames();

    assertFalse("Bucket names should not be empty", bucketNames.isEmpty());

    cleanupTestObjects(bucketName, ImmutableList.<StorageResourceId>of());
  }

  @Test
  public void testListObjectNamesLimited() throws IOException {
    try (TestBucketScope scope =
        new UniqueBucketScope(rawStorage, "list_limited")) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      List<StorageResourceId> testOjects = new ArrayList<>();

      String[] names = { "a", "b", "c", "d" };
      for (String name : names) {
        StorageResourceId id = new StorageResourceId(bucketName, name);
        gcs.createEmptyObject(id);
        testOjects.add(id);
      }

      List<String> gcsNames = gcs.listObjectNames(bucketName, null, "/", 2);

      assertEquals(2, gcsNames.size());

      cleanupTestObjects(ImmutableList.<String>of(), testOjects);
    }
  }

  @Test
  public void testListObjectInfoLimited() throws IOException {
    try (TestBucketScope scope =
        new UniqueBucketScope(rawStorage, "list_limited")) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      List<StorageResourceId> testOjects = new ArrayList<>();

      String[] names = { "x", "y", "z" };
      for (String name : names) {
        StorageResourceId id = new StorageResourceId(bucketName, name);
        gcs.createEmptyObject(id);
        testOjects.add(id);
      }

      List<GoogleCloudStorageItemInfo> info =
          gcs.listObjectInfo(bucketName, null, "/", 2);

      assertEquals(2, info.size());

      cleanupTestObjects(ImmutableList.<String>of(), testOjects);
    }
  }

  @Test
  public void testListObjectInfoWithDirectoryRepair() throws IOException {
    try (TestBucketScope scope = new UniqueBucketScope(rawStorage, "list_repair")) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
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

      GoogleCloudStorageItemInfo itemInfo =
          gcs.getItemInfo(new StorageResourceId(bucketName, "d2/"));
      assertFalse(itemInfo.exists());

      List<GoogleCloudStorageItemInfo> rootInfo =
          gcs.listObjectInfo(bucketName, null, "/",
              GoogleCloudStorage.MAX_RESULTS_UNLIMITED);

      // Specifying any exact values seems like it's begging for this test to become flaky.
      assertFalse("Infos not expected to be empty", rootInfo.isEmpty());

      // Directory repair should have created an empty object for us:
      StorageResourceId d2 = new StorageResourceId(bucketName, "d2/");
      testOjects.add(d2);
      GoogleCloudStorageTestHelper.assertObjectContent(gcs, d2, new byte[0]);

      List<GoogleCloudStorageItemInfo> d2ItemInfo =
          gcs.listObjectInfo(bucketName, "d2/d3/", "/",
              GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
      assertFalse("D2 item info not expected to be empty", d2ItemInfo.isEmpty());

      // Testing GCS treating object names as opaque blobs
      List<GoogleCloudStorageItemInfo> blobNamesInfo =
          gcs.listObjectInfo(bucketName, null, null,
              GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
      assertFalse("blobNamesInfo not expected to be empty", blobNamesInfo.isEmpty());

      // Used to clean up list objects / prefixes.
      cleanupTestObjects(ImmutableList.<String>of(), testOjects);
    }
  }

  @Test
  public void testCopySingleItem() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      byte[] bytesToWrite = new byte[4096];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

      // Verify the bucket exist by creating an object
      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testCopySingleItem_CopySingleItemCopySourceObject");
      try (WritableByteChannel channel = gcs.create(objectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageTestHelper.assertObjectContent(
          gcs, objectToCreate, bytesToWrite);

      StorageResourceId copiedResourceId =
          new StorageResourceId(bucketName, "testCopySingleItem_CopySingleItemDestinationObject");
      // Do the copy:
      gcs.copy(
          bucketName,
          ImmutableList.of(objectToCreate.getObjectName()),
          bucketName,
          ImmutableList.of(copiedResourceId.getObjectName()));

      GoogleCloudStorageTestHelper.assertObjectContent(gcs, copiedResourceId, bytesToWrite);
    }
  }


  @Test
  public void testCopyToDifferentBucket() throws IOException {
    String sourceBucketName = bucketHelper.getUniqueBucketName("copy_src_bucket");
    String destinationBucketName = bucketHelper.getUniqueBucketName("copy_dst_bucket");

    rawStorage.create(sourceBucketName);
    rawStorage.create(destinationBucketName);

    byte[] bytesToWrite = new byte[4096];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

    // Verify the bucket exist by creating an object
    StorageResourceId objectToCreate = new StorageResourceId(sourceBucketName, "CopyTestObject");
    try (WritableByteChannel channel = rawStorage.create(objectToCreate)) {
      channel.write(ByteBuffer.wrap(bytesToWrite));
    }

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, objectToCreate, bytesToWrite);

    StorageResourceId copiedResourceId =
        new StorageResourceId(destinationBucketName, "CopiedObject");

    // Do the copy:
    rawStorage.copy(
        sourceBucketName,
        ImmutableList.of(objectToCreate.getObjectName()),
        destinationBucketName,
        ImmutableList.of(copiedResourceId.getObjectName()));

    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, copiedResourceId, bytesToWrite);

    cleanupTestObjects(
        ImmutableList.of(sourceBucketName, destinationBucketName),
        ImmutableList.of(objectToCreate, copiedResourceId));
  }

  @Test
  public void testCopySingleItemOverExistingItem() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      byte[] bytesToWrite = new byte[4096];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

      StorageResourceId objectToCopy =
          new StorageResourceId(bucketName, "testCopySingleItemOverExistingItem_CopyTestObject");
      try (WritableByteChannel channel = gcs.create(objectToCopy)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCopy, bytesToWrite);

      StorageResourceId secondObject =
          new StorageResourceId(bucketName, "testCopySingleItemOverExistingItem_CopyTestObject2");
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
    }
  }

  @Test
  public void testCopySingleItemOverItself() throws IOException {
    try (TestBucketScope scope = new UniqueBucketScope(rawStorage, "copy_item_1_1")) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      byte[] bytesToWrite = new byte[4096];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

      StorageResourceId objectToCopy = new StorageResourceId(bucketName, "CopyTestObject");
      try (WritableByteChannel channel = gcs.create(objectToCopy)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageTestHelper.assertObjectContent(
          gcs, objectToCopy, bytesToWrite);

      try {
        gcs.copy(
            bucketName,
            ImmutableList.of(objectToCopy.getObjectName()),
            bucketName,
            ImmutableList.of(objectToCopy.getObjectName()));
        fail();
      } catch (IllegalArgumentException e) {
        assertThat(e).hasMessageThat().startsWith("Copy destination must be different");
      }
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
    try (TestBucketScope scope = new UniqueBucketScope(rawStorage, "copy_multi_item")) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      final int copyObjectCount = 3;

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
        GoogleCloudStorageTestHelper.assertObjectContent(
            gcs,
            copyObjectData.sourceResourceId,
            copyObjectData.objectBytes);
        GoogleCloudStorageTestHelper.assertObjectContent(
            gcs,
            copyObjectData.destinationResourceId,
            copyObjectData.objectBytes);
      }

      List<StorageResourceId> objectsToCleanup = new ArrayList<>(copyObjectCount * 2);
      for (CopyObjectData copyObjectData : objectsToCopy) {
        objectsToCleanup.add(copyObjectData.sourceResourceId);
        objectsToCleanup.add(copyObjectData.destinationResourceId);
      }
    }
  }

  @Test
  public void testCopyNonExistentItem() throws IOException {
    try (TestBucketScope scope = new UniqueBucketScope(rawStorage, "copy_item_ne")) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      String notExistentName = "IDontExist";

      try {
        gcs.copy(
            bucketName,
            ImmutableList.of(notExistentName),
            bucketName,
            ImmutableList.of("Some_destination"));
        fail();
      } catch (FileNotFoundException e) {
        // expected
      }
    }
  }

  @Test
  public void testCopyMultipleItemsToSingleDestination() throws IOException {
    try (TestBucketScope scope = new UniqueBucketScope(rawStorage, "copy_mutli_2_1")) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      try {
        gcs.copy(
            bucketName,
            ImmutableList.of("SomeSource", "SomeSource2"),
            bucketName,
            ImmutableList.of("Some_destination"));
      fail("Copying multiple items to a single source should fail.");
      } catch (IllegalArgumentException e) {
        assertThat(e).hasMessageThat().startsWith("Must supply same number of elements");
      }
    }
  }

  @Test
  public void testOpen() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      byte[] bytesToWrite = new byte[100];

      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testOpen_OpenTestObject");
      try (WritableByteChannel channel = gcs.create(objectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageTestHelper.assertObjectContent(gcs, objectToCreate, bytesToWrite);
    }
  }

  @Test
  public void testOpenNonExistentItem() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      try {
        gcs.open(new StorageResourceId(bucketName, "testOpenNonExistentItem_AnObject"));
        fail("Exception expected from opening an non-existent object");
      } catch (FileNotFoundException e) {
        // expected
      }
    }
  }

  @Test
  public void testOpenEmptyObject() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("open_empty");
    rawStorage.create(bucketName);

    StorageResourceId resourceId = new StorageResourceId(bucketName, "EmptyObject");
    rawStorage.createEmptyObject(resourceId);
    GoogleCloudStorageTestHelper.assertObjectContent(rawStorage, resourceId, new byte[0]);

    cleanupTestObjects(bucketName, resourceId);
  }

  @Test
  public void testOpenLargeObject() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testOpenLargeObject_LargeObject");
      GoogleCloudStorageTestHelper.readAndWriteLargeObject(objectToCreate, gcs);
    }
  }

  @Test
  public void testPlusInObjectNames() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId resourceId =
          new StorageResourceId(bucketName, "testPlusInObjectNames_an+object");
      gcs.createEmptyObject(resourceId);
      GoogleCloudStorageTestHelper.assertObjectContent(
          gcs, resourceId, new byte[0]);
    }
  }

  @Test
  public void testObjectPosition() throws IOException {
    final int totalBytes = 1200;
    byte[] data = new byte[totalBytes];
    GoogleCloudStorageTestHelper.fillBytes(data);

    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId resourceId =
          new StorageResourceId(bucketName, "testObjectPosition_SeekTestObject");
      try (WritableByteChannel channel = gcs.create(resourceId)) {
        channel.write(ByteBuffer.wrap(data));
      }

      byte[] readBackingArray = new byte[totalBytes];
      ByteBuffer readBuffer = ByteBuffer.wrap(readBackingArray);
      try (SeekableByteChannel readChannel = gcs.open(resourceId)) {
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
        } catch (EOFException eofe) {
          // Expected.
        }

        try {
          readChannel.position(totalBytes);
          fail("Expected IllegalArgumentException");
        } catch (EOFException eofe) {
          // Expected.
        }
      }

    }
  }

  @Test
  public void testReadPartialObjects() throws IOException {
    final int segmentSize = 553;
    final int segmentCount = 5;
    byte[] data = new byte[segmentCount * segmentSize];
    GoogleCloudStorageTestHelper.fillBytes(data);

    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId resourceId =
          new StorageResourceId(bucketName, "testReadPartialObjects_ReadPartialTest");
      try (WritableByteChannel channel = gcs.create(resourceId)) {
        channel.write(ByteBuffer.wrap(data));
      }

      byte[][] readSegments = new byte[segmentCount][segmentSize];
      try (SeekableByteChannel readChannel = gcs.open(resourceId)) {
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
    }
  }

  @Test
  public void testSpecialResourceIds() throws IOException {
    assertEquals("Unexpected ROOT item info returned",
        GoogleCloudStorageItemInfo.ROOT_INFO,
        rawStorage.getItemInfo(StorageResourceId.ROOT));

    try {
      StorageResourceId.createReadableString(null, "objectName");
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testChannelClosedException() throws IOException {
    final int totalBytes = 1200;
    byte[] data = new byte[totalBytes];
    GoogleCloudStorageTestHelper.fillBytes(data);
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      StorageResourceId resourceId =
          new StorageResourceId(bucketName, "testChannelClosedException_ReadClosedChannelTest");
      try (WritableByteChannel channel = gcs.create(resourceId)) {
        channel.write(ByteBuffer.wrap(data));
      }

      byte[] readArray = new byte[totalBytes];
      SeekableByteChannel readableByteChannel = gcs.open(resourceId);
      ByteBuffer readBuffer = ByteBuffer.wrap(readArray);
      readBuffer.limit(5);
      readableByteChannel.read(readBuffer);
      assertThat(readableByteChannel.position()).isEqualTo(readBuffer.position());

      readableByteChannel.close();
      readBuffer.clear();

      try {
        readableByteChannel.read(readBuffer);
        fail();
      } catch (ClosedChannelException e) {
        // expected
      }
    }
  }

  @Test @Ignore("Not implemented")
  public void testOperationsAfterCloseFail() {

  }

  @Test
  public void testMetadataIsWrittenWhenCreatingObjects() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      byte[] bytesToWrite = new byte[100];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

      Map<String, byte[]> metadata = ImmutableMap.of(
          "key1", "value1".getBytes(StandardCharsets.UTF_8),
          "key2", "value2".getBytes(StandardCharsets.UTF_8));

      // Verify the bucket exist by creating an object
      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testUpdateItemInfoUpdatesMetadata");
      try (WritableByteChannel channel = gcs.create(
          objectToCreate, new CreateObjectOptions(false, metadata))) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      // Verify metadata was set on create.
      GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(objectToCreate);
      assertMapsEqual(metadata, itemInfo.getMetadata(), BYTE_ARRAY_EQUIVALENCE);
    }
  }

  @Test
  public void testMetdataIsWrittenWhenCreatingEmptyObjects() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      Map<String, byte[]> metadata = ImmutableMap.of(
          "key1", "value1".getBytes(StandardCharsets.UTF_8),
          "key2", "value2".getBytes(StandardCharsets.UTF_8));

      // Verify the bucket exist by creating an object
      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testMetdataIsWrittenWhenCreatingEmptyObjects");
      gcs.createEmptyObject(objectToCreate, new CreateObjectOptions(false, metadata));

      // Verify we get metadata from getItemInfo
      GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(objectToCreate);
      assertMapsEqual(metadata, itemInfo.getMetadata(), BYTE_ARRAY_EQUIVALENCE);
    }
  }

  @Test
  public void testUpdateItemInfoUpdatesMetadata() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();

      byte[] bytesToWrite = new byte[100];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);

      Map<String, byte[]> metadata = ImmutableMap.of(
          "key1", "value1".getBytes(StandardCharsets.UTF_8),
          "key2", "value2".getBytes(StandardCharsets.UTF_8));

      // Verify the bucket exist by creating an object
      StorageResourceId objectToCreate =
          new StorageResourceId(bucketName, "testUpdateItemInfoUpdatesMetadata");
      try (WritableByteChannel channel = gcs.create(objectToCreate)) {
        channel.write(ByteBuffer.wrap(bytesToWrite));
      }

      GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(objectToCreate);
      assertEquals("iniital metadata should be empty", 0, itemInfo.getMetadata().size());

      // Verify we can update metadata:
      List<GoogleCloudStorageItemInfo> results =
          gcs.updateItems(ImmutableList.of(new UpdatableItemInfo(objectToCreate, metadata)));

      assertEquals(1, results.size());
      assertMapsEqual(metadata, results.get(0).getMetadata(), BYTE_ARRAY_EQUIVALENCE);

      // Verify we get metadata from getItemInfo
      itemInfo = gcs.getItemInfo(objectToCreate);
      assertMapsEqual(metadata, itemInfo.getMetadata(), BYTE_ARRAY_EQUIVALENCE);

      // Delete key1 from metadata:
      Map<String, byte[]> deletionMap = new HashMap<>();
      deletionMap.put("key1", null);
      gcs.updateItems(ImmutableList.of(new UpdatableItemInfo(objectToCreate, deletionMap)));

      itemInfo = gcs.getItemInfo(objectToCreate);
      // Ensure that only key2:value2 still exists:
      assertMapsEqual(
          ImmutableMap.of(
              "key2", "value2".getBytes(StandardCharsets.UTF_8)),
          itemInfo.getMetadata(),
          BYTE_ARRAY_EQUIVALENCE);
    }
  }

  @Test
  public void testCompose() throws Exception {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      StorageResourceId destinationObject =
          new StorageResourceId(bucketName, "testCompose_DestinationObject");

      // Create source objects
      String content1 = "Content 1";
      String content2 = "Content 2";
      StorageResourceId sourceObject1 =
          new StorageResourceId(bucketName, "testCompose_SourceObject1");
      StorageResourceId sourceObject2 =
          new StorageResourceId(bucketName, "testCompose_SourceObject2");
      try (WritableByteChannel channel = gcs.create(sourceObject1)) {
        channel.write(ByteBuffer.wrap(content1.getBytes(UTF_8)));
      }
      try (WritableByteChannel channel = gcs.create(sourceObject2)) {
        channel.write(ByteBuffer.wrap(content2.getBytes(UTF_8)));
      }
      GoogleCloudStorageTestHelper.assertObjectContent(
          gcs, sourceObject1, content1.getBytes(UTF_8));
      GoogleCloudStorageTestHelper.assertObjectContent(
          gcs, sourceObject2, content2.getBytes(UTF_8));

      // Do the compose
      gcs.compose(
          bucketName,
          ImmutableList.of("testCompose_SourceObject1", "testCompose_SourceObject2"),
          destinationObject.getObjectName(),
          CreateFileOptions.DEFAULT_CONTENT_TYPE);

      GoogleCloudStorageTestHelper.assertObjectContent(
          gcs, destinationObject, content1.concat(content2).getBytes(UTF_8));
    }
  }

  @Test
  public void testObjectVerificationAttributes() throws IOException {
    try (TestBucketScope scope = new SharedBucketScope(rawStorage)) {
      String bucketName = scope.getBucketName();
      GoogleCloudStorage gcs = scope.getStorageInstance();
      StorageResourceId testObject =
          new StorageResourceId(bucketName, "testObjectValidationAttributes");
      byte[] objectBytes = new byte[1024];
      GoogleCloudStorageTestHelper.fillBytes(objectBytes);
      HashCode originalMd5 = Hashing.md5().hashBytes(objectBytes);
      HashCode originalCrc32c = Hashing.crc32c().hashBytes(objectBytes);
      // Note that HashCode#asBytes returns a little-endian encoded array while
      // GCS uses big-endian. We avoid that by grabbing the int value of the CRC32c
      // and running it through Ints.toByteArray which encodes using big-endian.
      byte[] bigEndianCrc32c = Ints.toByteArray(originalCrc32c.asInt());

      // Don't use hashes in object creation, just validate the round trip. This of course
      // could lead to flaky looking tests due to bit flip errors.
      try (WritableByteChannel channel = gcs.create(testObject)) {
        channel.write(ByteBuffer.wrap(objectBytes));
      }

      GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(testObject);

      GoogleCloudStorageTestHelper.assertByteArrayEquals(
          originalMd5.asBytes(), itemInfo.getVerificationAttributes().getMd5hash());
      // These string versions are slightly easier to debug (used when trying to
      // replicate GCS crc32c values in InMemoryGoogleCloudStorage).
      String originalCrc32cString =
          Integer.toHexString(
              Ints.fromByteArray(bigEndianCrc32c));
      String newCrc32cString =
          Integer.toHexString(
              Ints.fromByteArray(itemInfo.getVerificationAttributes().getCrc32c()));
      assertEquals(originalCrc32cString, newCrc32cString);
      GoogleCloudStorageTestHelper.assertByteArrayEquals(
          bigEndianCrc32c, itemInfo.getVerificationAttributes().getCrc32c());

      VerificationAttributes expectedAttributes =
          new VerificationAttributes(originalMd5.asBytes(), bigEndianCrc32c);

      Assert.assertEquals(expectedAttributes, itemInfo.getVerificationAttributes());
    }
  }

  static <K, V> void assertMapsEqual(
      Map<K, V> expected, Map<K, V> result, Equivalence<V> valueEquivalence) {
    MapDifference<K, V> difference = Maps.difference(expected, result, valueEquivalence);
    if (!difference.areEqual()) {
      StringBuilder builder = new StringBuilder();
      builder.append("Maps differ. ");
      builder.append("Entries differing: ").append(difference.entriesDiffering()).append("\n");
      builder.append("Missing entries: ").append(difference.entriesOnlyOnLeft()).append("\n");
      builder.append("Extra entries: ").append(difference.entriesOnlyOnRight()).append("\n");
      fail(builder.toString());
    }
  }
}
