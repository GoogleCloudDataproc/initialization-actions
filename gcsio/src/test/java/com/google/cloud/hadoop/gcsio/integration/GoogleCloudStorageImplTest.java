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
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.fail;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that require a particular configuration of GoogleCloudStorageImpl.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageImplTest {

  TestBucketHelper bucketHelper = new TestBucketHelper("gcs_impl");
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected GoogleCloudStorageImpl makeStorageWithBufferSize(int bufferSize) throws IOException {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageTestHelper.getStandardOptionBuilder();

    builder.getWriteChannelOptionsBuilder()
        .setUploadBufferSize(bufferSize);

    Credential credential = GoogleCloudStorageTestHelper.getCredential();

    return new GoogleCloudStorageImpl(builder.build(), credential);
  }

  protected GoogleCloudStorageImpl makeStorageWithMarkerFileCreation(
      boolean createMarkerFiles) throws IOException {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageTestHelper.getStandardOptionBuilder();

    builder.setCreateMarkerObjects(createMarkerFiles);

    Credential credential = GoogleCloudStorageTestHelper.getCredential();

    return new GoogleCloudStorageImpl(builder.build(), credential);
  }

  protected GoogleCloudStorageImpl makeStorageWithInferImplicit()
      throws IOException {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageTestHelper.getStandardOptionBuilder();

    builder.setAutoRepairImplicitDirectoriesEnabled(false);
    builder.setInferImplicitDirectoriesEnabled(true);

    Credential credential = GoogleCloudStorageTestHelper.getCredential();

    return new GoogleCloudStorageImpl(builder.build(), credential);
  }

  @Test
  public void testReadAndWriteLargeObjectWithSmallBuffer() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("write_large_obj");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "LargeObject");

    GoogleCloudStorageImpl gcs = makeStorageWithBufferSize(1 * 1024 * 1024);

    try {
      gcs.create(bucketName);
      GoogleCloudStorageTestHelper.readAndWriteLargeObject(resourceId, gcs);

    } finally {
      GoogleCloudStorageTestHelper.cleanupTestObjects(
          gcs,
          ImmutableList.of(bucketName),
          ImmutableList.of(resourceId));
    }
  }

  @Test
  public void testNonAlignedWriteChannelBufferSize() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("write_3m_buff_obj");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "LargeObject");

    GoogleCloudStorageImpl gcs = makeStorageWithBufferSize(3 * 1024 * 1024);

    try {
      gcs.create(bucketName);
      GoogleCloudStorageTestHelper.readAndWriteLargeObject(resourceId, gcs);

    } finally {
      GoogleCloudStorageTestHelper.cleanupTestObjects(
          gcs,
          ImmutableList.of(bucketName),
          ImmutableList.of(resourceId));
    }
  }

  @Test
  public void testConflictingWritesWithMarkerFiles() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("with_marker");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "obj1");

    GoogleCloudStorageImpl gcs = makeStorageWithMarkerFileCreation(true);

    try {
      gcs.create(bucketName);
      byte[] bytesToWrite = new byte[1024];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);
      WritableByteChannel byteChannel1 = gcs.create(resourceId, new CreateObjectOptions(false));
      byteChannel1.write(ByteBuffer.wrap(bytesToWrite));

      // This call should fail:
      expectedException.expectMessage("already exists");
      WritableByteChannel byteChannel2 = gcs.create(resourceId, new CreateObjectOptions(false));
      fail("Creating the second byte channel should fail.");
    } finally {
      GoogleCloudStorageTestHelper.cleanupTestObjects(
          gcs,
          ImmutableList.of(bucketName),
          ImmutableList.of(resourceId));
    }
  }

  @Test
  public void testConflictingWritesWithoutMarkerFiles() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("without_marker");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "obj1");

    GoogleCloudStorageImpl gcs = makeStorageWithMarkerFileCreation(false);

    try {
      gcs.create(bucketName);
      byte[] bytesToWrite = new byte[1024];
      GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);
      WritableByteChannel byteChannel1 = gcs.create(resourceId, new CreateObjectOptions(false));
      byteChannel1.write(ByteBuffer.wrap(bytesToWrite));

      // Creating this channel should succeed. Only when we close will an error bubble up.
      WritableByteChannel byteChannel2 = gcs.create(resourceId, new CreateObjectOptions(false));

      byteChannel1.close();

      // Closing byte channel2 should fail:
      expectedException.expectMessage("412 Precondition Failed");
      byteChannel2.close();
      fail("Closing the second byte channel should fail.");
    } finally {
      GoogleCloudStorageTestHelper.cleanupTestObjects(
          gcs,
          ImmutableList.of(bucketName),
          ImmutableList.of(resourceId));
    }
  }

  @Test
  public void testInferImplicitDirectories() throws IOException {
    String bucketName = bucketHelper.getUniqueBucketName("infer_implicit");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "d0/o1");

    GoogleCloudStorageImpl gcs = makeStorageWithInferImplicit();

    try {
      gcs.create(bucketName);
      gcs.createEmptyObject(resourceId);

      GoogleCloudStorageItemInfo itemInfo =
          gcs.getItemInfo(new StorageResourceId(bucketName, "d0/"));
      assertThat(itemInfo.exists()).isFalse();

       List<GoogleCloudStorageItemInfo> d0ItemInfo =
           gcs.listObjectInfo(bucketName, "d0/", "/");
      assertWithMessage("d0 length").that(d0ItemInfo.size()).isEqualTo(1);

    } finally {
      GoogleCloudStorageTestHelper.cleanupTestObjects(
          gcs,
          ImmutableList.of(bucketName),
          ImmutableList.of(resourceId));
    }
  }

  @Test
  public void testCreateCorrectlySetsContentType() throws IOException {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageTestHelper.getStandardOptionBuilder();
    Credential credential = GoogleCloudStorageTestHelper.getCredential();
    GoogleCloudStorageImpl gcs = new GoogleCloudStorageImpl(builder.build(), credential);

    String bucketName = bucketHelper.getUniqueBucketName("my_bucket");
    StorageResourceId resourceId1 = new StorageResourceId(bucketName, "obj1");
    StorageResourceId resourceId2 = new StorageResourceId(bucketName, "obj2");
    StorageResourceId resourceId3 = new StorageResourceId(bucketName, "obj3");

    try {
      gcs.create(bucketName);
      gcs.createEmptyObject(resourceId1,
          new CreateObjectOptions(true, "text/plain", CreateObjectOptions.EMPTY_METADATA));
      gcs.create(resourceId2,
          new CreateObjectOptions(true, "image/png", CreateObjectOptions.EMPTY_METADATA)).close();
      gcs.create(resourceId3).close(); // default content-type: "application/octet-stream"

      assertThat(gcs.getItemInfo(resourceId1).getContentType()).isEqualTo("text/plain");
      assertThat(gcs.getItemInfo(resourceId2).getContentType()).isEqualTo("image/png");
      assertThat(gcs.getItemInfo(resourceId3).getContentType())
          .isEqualTo("application/octet-stream");
    } finally {
      GoogleCloudStorageTestHelper.cleanupTestObjects(
          gcs,
          ImmutableList.of(bucketName),
          ImmutableList.of(resourceId1, resourceId2, resourceId3));
    }
  }
}
