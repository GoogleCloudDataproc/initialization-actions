/*
 * Copyright 2020 Google LLC. All Rights Reserved.
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

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** CSEK encryption integration tests. */
@RunWith(JUnit4.class)
public class CsekEncryptionIntegrationTest {

  private static final TestBucketHelper BUCKET_HELPER = new TestBucketHelper("gcs-csek-encryption");

  @AfterClass
  public static void afterAll() throws IOException {
    BUCKET_HELPER.cleanup(
        makeStorage(GoogleCloudStorageTestHelper.getStandardOptionBuilder().build()));
  }

  @Test
  public void uploadAndGetObject() throws IOException {
    GoogleCloudStorageImpl gcs = makeStorage(getCsekStorageOptions().build());

    String bucketName = BUCKET_HELPER.getUniqueBucketName("upload-and-get");
    StorageResourceId resourceId = new StorageResourceId(bucketName, "obj");

    gcs.create(bucketName);
    gcs.createEmptyObject(
        resourceId,
        new CreateObjectOptions(true, "text/plain", CreateObjectOptions.EMPTY_METADATA));

    assertThat(gcs.getItemInfo(resourceId).getContentType()).isEqualTo("text/plain");
  }

  @Test
  public void rewriteObject() throws IOException {
    GoogleCloudStorageImpl gcs =
        makeStorage(
            getCsekStorageOptions()
                .setCopyWithRewriteEnabled(true)
                .setMaxBytesRewrittenPerCall(512 * 1024 * 1024)
                .build());

    String srcBucketName = BUCKET_HELPER.getUniqueBucketName("rewrite-src");
    gcs.create(srcBucketName);

    String dstBucketName = BUCKET_HELPER.getUniqueBucketName("rewrite-dst");
    // Create destination bucket with different location and storage class,
    // because this is supported by rewrite but not copy requests
    gcs.create(dstBucketName, CreateBucketOptions.builder().setStorageClass("coldline").build());

    StorageResourceId srcResourceId = new StorageResourceId(srcBucketName, "encryptedObject");
    int partitionsCount = 32;
    byte[] partition =
        writeObject(gcs, srcResourceId, /* partitionSize= */ 64 * 1024 * 1024, partitionsCount);

    StorageResourceId dstResourceId = new StorageResourceId(dstBucketName, "encryptedObject");
    gcs.copy(
        srcBucketName, ImmutableList.of(srcResourceId.getObjectName()),
        dstBucketName, ImmutableList.of(dstResourceId.getObjectName()));

    assertObjectContent(gcs, dstResourceId, partition, partitionsCount);
  }

  private static GoogleCloudStorageImpl makeStorage(GoogleCloudStorageOptions options)
      throws IOException {
    Credential credential = GoogleCloudStorageTestHelper.getCredential();
    return new GoogleCloudStorageImpl(options, credential);
  }

  private static GoogleCloudStorageOptions.Builder getCsekStorageOptions() {
    return GoogleCloudStorageTestHelper.getStandardOptionBuilder()
        .setEncryptionAlgorithm("AES256")
        .setEncryptionKey("CSX19s0epGWZP3h271Idu8xma2WhMuKT8ZisYfcjLM8=")
        .setEncryptionKeyHash("LpH4y6Bki5zIhYrjGo1J4BuSt12G/1B+n3FwORpdoyQ=");
  }
}
