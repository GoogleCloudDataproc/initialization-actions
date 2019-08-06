/*
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

import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.assertObjectContent;
import static com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.writeObject;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests that require a particular configuration of GoogleCloudStorageImpl. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageImplTest {

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper("gcs-impl");

  @AfterClass
  public static void afterAll() throws IOException {
    BUCKET_HELPER.cleanup(
        makeStorage(GoogleCloudStorageTestHelper.getStandardOptionBuilder().build()));
  }

  private static GoogleCloudStorageImpl makeStorage(GoogleCloudStorageOptions options)
      throws IOException {
    Credential credential = GoogleCloudStorageTestHelper.getCredential();
    return new GoogleCloudStorageImpl(options, credential);
  }

  protected GoogleCloudStorageImpl makeStorageWithBufferSize(int bufferSize) throws IOException {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageTestHelper.getStandardOptionBuilder()
            .setWriteChannelOptions(
                AsyncWriteChannelOptions.builder().setUploadChunkSize(bufferSize).build());

    return makeStorage(builder.build());
  }

  protected GoogleCloudStorageImpl makeStorageWithInferImplicit()
      throws IOException {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageTestHelper.getStandardOptionBuilder();

    builder.setAutoRepairImplicitDirectoriesEnabled(false);
    builder.setInferImplicitDirectoriesEnabled(true);

    return makeStorage(builder.build());
  }

  @Test
  public void testReadAndWriteLargeObjectWithSmallBuffer() throws IOException {
    GoogleCloudStorageImpl gcs = makeStorageWithBufferSize(1024 * 1024);

    String bucketName = BUCKET_HELPER.getUniqueBucketName("write-large-obj");
    gcs.create(bucketName);

    StorageResourceId resourceId = new StorageResourceId(bucketName, "LargeObject");
    int partitionsCount = 64;
    byte[] partition =
        writeObject(gcs, resourceId, /* partitionSize= */ 5 * 1024 * 1024, partitionsCount);

    assertObjectContent(gcs, resourceId, partition, partitionsCount);
  }

  @Test
  public void testNonAlignedWriteChannelBufferSize() throws IOException {
    GoogleCloudStorageImpl gcs = makeStorageWithBufferSize(3 * 1024 * 1024);

    String bucketName = BUCKET_HELPER.getUniqueBucketName("write-3m-buff-obj");
    gcs.create(bucketName);

    StorageResourceId resourceId = new StorageResourceId(bucketName, "Object");
    int partitionsCount = 64;
    byte[] partition =
        writeObject(gcs, resourceId, /* partitionSize= */ 1024 * 1024, partitionsCount);

    assertObjectContent(gcs, resourceId, partition, partitionsCount);
  }

  @Test
  public void testConflictingWrites() throws IOException {
    String bucketName = BUCKET_HELPER.getUniqueBucketName("without-marker");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "obj1");

    GoogleCloudStorageImpl gcs =
        makeStorage(GoogleCloudStorageTestHelper.getStandardOptionBuilder().build());

    gcs.create(bucketName);
    byte[] bytesToWrite = new byte[1024];
    GoogleCloudStorageTestHelper.fillBytes(bytesToWrite);
    WritableByteChannel byteChannel1 = gcs.create(resourceId, new CreateObjectOptions(false));
    byteChannel1.write(ByteBuffer.wrap(bytesToWrite));

    // Creating this channel should succeed. Only when we close will an error bubble up.
    WritableByteChannel byteChannel2 = gcs.create(resourceId, new CreateObjectOptions(false));

    byteChannel1.close();

    // Closing byte channel2 should fail:
    Throwable thrown = assertThrows(Throwable.class, byteChannel2::close);
    assertThat(thrown).hasCauseThat().hasMessageThat().contains("412 Precondition Failed");
  }

  @Test
  public void testInferImplicitDirectories() throws IOException {
    String bucketName = BUCKET_HELPER.getUniqueBucketName("infer-implicit");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "d0/o1");

    GoogleCloudStorageImpl gcs = makeStorageWithInferImplicit();

    gcs.create(bucketName);
    gcs.createEmptyObject(resourceId);

    GoogleCloudStorageItemInfo itemInfo = gcs.getItemInfo(new StorageResourceId(bucketName, "d0/"));
    assertThat(itemInfo.exists()).isFalse();

    List<GoogleCloudStorageItemInfo> d0ItemInfo = gcs.listObjectInfo(bucketName, "d0/", "/");
    assertWithMessage("d0 length").that(d0ItemInfo.size()).isEqualTo(1);
  }

  @Test
  public void testCreateCorrectlySetsContentType() throws IOException {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageTestHelper.getStandardOptionBuilder();
    GoogleCloudStorageImpl gcs = makeStorage(builder.build());

    String bucketName = BUCKET_HELPER.getUniqueBucketName("my-bucket");
    StorageResourceId resourceId1 = new StorageResourceId(bucketName, "obj1");
    StorageResourceId resourceId2 = new StorageResourceId(bucketName, "obj2");
    StorageResourceId resourceId3 = new StorageResourceId(bucketName, "obj3");

    gcs.create(bucketName);
    gcs.createEmptyObject(
        resourceId1,
        new CreateObjectOptions(true, "text/plain", CreateObjectOptions.EMPTY_METADATA));
    gcs.create(
            resourceId2,
            new CreateObjectOptions(true, "image/png", CreateObjectOptions.EMPTY_METADATA))
        .close();
    gcs.create(resourceId3).close(); // default content-type: "application/octet-stream"

    assertThat(gcs.getItemInfo(resourceId1).getContentType()).isEqualTo("text/plain");
    assertThat(gcs.getItemInfo(resourceId2).getContentType()).isEqualTo("image/png");
    assertThat(gcs.getItemInfo(resourceId3).getContentType()).isEqualTo("application/octet-stream");
  }

  @Test
  public void testCopySingleItemWithRewrite() throws IOException {
    GoogleCloudStorageImpl gcs =
        makeStorage(
            GoogleCloudStorageTestHelper.getStandardOptionBuilder()
                .setCopyWithRewriteEnabled(true)
                .setMaxBytesRewrittenPerCall(512 * 1024 * 1024)
                .build());

    String srcBucketName = BUCKET_HELPER.getUniqueBucketName("copy-with-rewrite-src");
    gcs.create(srcBucketName);

    String dstBucketName = BUCKET_HELPER.getUniqueBucketName("copy-with-rewrite-dst");
    // Create destination bucket with different location and storage class,
    // because this is supported by rewrite but not copy requests
    gcs.create(dstBucketName, new CreateBucketOptions(null, "coldline"));

    StorageResourceId resourceId =
        new StorageResourceId(srcBucketName, "testCopySingleItemWithRewrite_SourceObject");
    int partitionsCount = 32;
    byte[] partition =
        writeObject(gcs, resourceId, /* partitionSize= */ 64 * 1024 * 1024, partitionsCount);

    StorageResourceId copiedResourceId =
        new StorageResourceId(dstBucketName, "testCopySingleItemWithRewrite_DestinationObject");
    gcs.copy(
        srcBucketName, ImmutableList.of(resourceId.getObjectName()),
        dstBucketName, ImmutableList.of(copiedResourceId.getObjectName()));

    assertObjectContent(gcs, copiedResourceId, partition, partitionsCount);
  }

  @Test
  public void googleCloudStorageItemInfo_metadataEquals() throws IOException {
    GoogleCloudStorageImpl gcs =
        makeStorage(GoogleCloudStorageTestHelper.getStandardOptionBuilder().build());

    String bucketName = BUCKET_HELPER.getUniqueBucketName("metadata-equals");
    gcs.create(bucketName);

    StorageResourceId object = new StorageResourceId(bucketName, "testMetadataEquals_Object");

    Map<String, byte[]> metadata1 =
        ImmutableMap.of(
            "key1", "value1".getBytes(StandardCharsets.UTF_8),
            "key2", "value2".getBytes(StandardCharsets.UTF_8));
    Map<String, byte[]> metadata2 =
        ImmutableMap.of(
            "key3", "value3".getBytes(StandardCharsets.UTF_8),
            "key4", "value4".getBytes(StandardCharsets.UTF_8));

    gcs.createEmptyObject(object, new CreateObjectOptions(true, "text/plain", metadata1));

    GoogleCloudStorageItemInfo itemInfo1 = gcs.getItemInfo(object);

    assertThat(itemInfo1.metadataEquals(metadata1)).isTrue();
    assertThat(itemInfo1.metadataEquals(itemInfo1.getMetadata())).isTrue();
    assertThat(itemInfo1.metadataEquals(metadata2)).isFalse();
  }
}
