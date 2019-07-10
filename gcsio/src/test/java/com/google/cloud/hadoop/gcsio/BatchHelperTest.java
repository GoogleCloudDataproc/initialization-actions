/*
 * Copyright 2019 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.BUCKET_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.JSON_FACTORY;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.OBJECT_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.metadataResponse;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.batchRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.util.DateTime;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Date;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BatchHelperTest {

  private BatchHelper.Factory batchFactory;

  @Before
  public void setUp() {
    batchFactory = new BatchHelper.Factory();
  }

  @Test
  public void allRequestsAreSentInSingleBatch() throws IOException {
    // 1. Prepare test data
    String objectName1 = OBJECT_NAME + "-01";
    String objectName2 = OBJECT_NAME + "-02";

    StorageObject object1 =
        new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(objectName1)
            .setSize(new BigInteger("123"))
            .setGeneration(1L)
            .setMetageneration(1L)
            .setUpdated(new DateTime(new Date()));
    StorageObject object2 =
        new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(objectName2)
            .setSize(new BigInteger("1234"))
            .setGeneration(2L)
            .setMetageneration(3L)
            .setUpdated(new DateTime(new Date()));

    // 2. Configure mock HTTP transport with test request responses
    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockBatchTransport(
            /* requestsPerBatch= */ 2, metadataResponse(object1), metadataResponse(object2));

    // 3. Configure BatchHelper with mocked HTTP transport
    TrackingHttpRequestInitializer httpRequestInitializer = new TrackingHttpRequestInitializer();
    Storage storage = new Storage(transport, JSON_FACTORY, httpRequestInitializer);
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(
            httpRequestInitializer,
            storage,
            /* maxRequestsPerBatch= */ 2,
            /* totalRequests= */ 2,
            /* maxThreads= */ 0);

    // 4. Queue 1st GET request to BatchHelper
    batchHelper.queue(storage.objects().get(BUCKET_NAME, objectName1), assertCallback(object1));

    // 5. Validate that no requests were sent after 1st request were queued
    assertThat(httpRequestInitializer.getAllRequestStrings()).isEmpty();

    // 6. Queue 2nd GET request to BatchHelper
    batchHelper.queue(storage.objects().get(BUCKET_NAME, objectName2), assertCallback(object2));

    // 7. Validate that 1 batch request consisting of 2 GET requests was sent
    assertThat(httpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(BUCKET_NAME, objectName1),
            getRequestString(BUCKET_NAME, objectName2));

    // 8. Reset httpRequestInitializer before validating `flush` method
    httpRequestInitializer.reset();

    // 9. Call `flush` at the end.
    batchHelper.flush();

    // 10. Validate that `flush` method call is noop if all requests were sent already
    assertThat(httpRequestInitializer.getAllRequestStrings()).isEmpty();
  }

  private JsonBatchCallback<StorageObject> assertCallback(StorageObject expectedObject) {
    return new JsonBatchCallback<StorageObject>() {
      @Override
      public void onSuccess(StorageObject storageObject, HttpHeaders responseHeaders) {
        assertThat(storageObject).isEqualTo(expectedObject);
      }

      @Override
      public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {}
    };
  }
}
