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


import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/**
 * Tests that require a particular configuration of GoogleCloudStorageImpl.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageImplTest {

  TestBucketHelper bucketHelper = new TestBucketHelper("gcs_impl");

  protected GoogleCloudStorageImpl makeStorageWithBufferSize(int bufferSize) throws IOException {
    GoogleCloudStorageOptions.Builder builder =
        GoogleCloudStorageTestHelper.getStandardOptionBuilder();

    builder.getWriteChannelOptionsBuilder()
        .setUploadBufferSize(bufferSize);

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
}
