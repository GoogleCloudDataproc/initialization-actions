/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.BUCKET_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.HTTP_TRANSPORT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.JSON_FACTORY;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.OBJECT_NAME;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageWriteChannel} class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageWriteChannelTest {

  @Test
  public void createRequest_shouldSetKmsKeyName() throws IOException {
    String kmsKeyName = "testKmsKey";

    GoogleCloudStorageWriteChannel writeChannel =
        new GoogleCloudStorageWriteChannel(
            MoreExecutors.newDirectExecutorService(),
            new Storage(HTTP_TRANSPORT, JSON_FACTORY, r -> {}),
            new ClientRequestHelper<>(),
            BUCKET_NAME,
            OBJECT_NAME,
            "content-type",
            /* contentEncoding= */ null,
            kmsKeyName,
            AsyncWriteChannelOptions.DEFAULT,
            new ObjectWriteConditions(),
            /* objectMetadata= */ null);

    Storage.Objects.Insert request =
        writeChannel.createRequest(
            new InputStreamContent("plain/text", new ByteArrayInputStream(new byte[0])));

    assertThat(request.getKmsKeyName()).isEqualTo(kmsKeyName);
  }
}
