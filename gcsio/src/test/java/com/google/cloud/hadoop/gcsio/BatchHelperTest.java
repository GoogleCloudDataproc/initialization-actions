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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTest.newStorageObject;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.BUCKET_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.HTTP_TRANSPORT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.JSON_FACTORY;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.OBJECT_NAME;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.jsonDataResponse;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.batchRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BatchHelperTest {

  private BatchHelper.Factory batchFactory;
  private TrackingHttpRequestInitializer httpRequestInitializer;

  @Before
  public void setUp() {
    batchFactory = new BatchHelper.Factory();
    httpRequestInitializer = new TrackingHttpRequestInitializer();
  }

  @Test
  public void newBatchHelper_throwsException_whenMaxRequestsPerBatchZero() {
    Storage storage = new Storage(HTTP_TRANSPORT, JSON_FACTORY, httpRequestInitializer);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                batchFactory.newBatchHelper(
                    httpRequestInitializer,
                    storage,
                    /* maxRequestsPerBatch= */ 0,
                    /* totalRequests= */ 1,
                    /* maxThreads= */ 1));

    assertThat(e).hasMessageThat().startsWith("maxRequestsPerBatch should be greater than 0");
  }

  @Test
  public void newBatchHelper_throwsException_whenTotalRequestsZero() {
    Storage storage = new Storage(HTTP_TRANSPORT, JSON_FACTORY, httpRequestInitializer);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                batchFactory.newBatchHelper(
                    httpRequestInitializer,
                    storage,
                    /* maxRequestsPerBatch= */ 1,
                    /* totalRequests= */ 0,
                    /* maxThreads= */ 1));

    assertThat(e).hasMessageThat().startsWith("totalRequests should be greater than 0");
  }

  @Test
  public void newBatchHelper_throwsException_whenMaxThreadsLessThanZero() {
    Storage storage = new Storage(HTTP_TRANSPORT, JSON_FACTORY, httpRequestInitializer);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                batchFactory.newBatchHelper(
                    httpRequestInitializer,
                    storage,
                    /* maxRequestsPerBatch= */ 1,
                    /* totalRequests= */ 1,
                    /* maxThreads= */ -1));

    assertThat(e).hasMessageThat().startsWith("maxThreads should be greater or equal to 0");
  }

  @Test
  public void allRequestsAreSentInSingleBatch_withZeroMaxThreads() throws IOException {
    // 1. Prepare test data
    String objectName1 = OBJECT_NAME + "-01";
    String objectName2 = OBJECT_NAME + "-02";
    StorageObject object1 = newStorageObject(BUCKET_NAME, objectName1);
    StorageObject object2 = newStorageObject(BUCKET_NAME, objectName2);

    // 2. Configure mock HTTP transport with test request responses
    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockBatchTransport(
            /* requestsPerBatch= */ 2, jsonDataResponse(object1), jsonDataResponse(object2));

    // 3. Configure BatchHelper with mocked HTTP transport
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

  @Test
  public void allRequestsAreSentInSingleBatch_withOneMaxThreads() throws IOException {
    // 1. Prepare test data
    String objectName1 = OBJECT_NAME + "-01";
    String objectName2 = OBJECT_NAME + "-02";
    StorageObject object1 = newStorageObject(BUCKET_NAME, objectName1);
    StorageObject object2 = newStorageObject(BUCKET_NAME, objectName2);

    // 2. Configure mock HTTP transport with test request responses
    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockBatchTransport(
            /* requestsPerBatch= */ 2, jsonDataResponse(object1), jsonDataResponse(object2));

    // 3. Configure BatchHelper with mocked HTTP transport
    Storage storage = new Storage(transport, JSON_FACTORY, httpRequestInitializer);
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(
            httpRequestInitializer,
            storage,
            /* maxRequestsPerBatch= */ 2,
            /* totalRequests= */ 2,
            /* maxThreads= */ 1);

    // 4. Queue 1st GET request to BatchHelper
    batchHelper.queue(storage.objects().get(BUCKET_NAME, objectName1), assertCallback(object1));

    // 5. Validate that no requests were sent after 1st request were queued
    assertThat(httpRequestInitializer.getAllRequestStrings()).isEmpty();

    // 6. Queue 2nd GET request to BatchHelper
    batchHelper.queue(storage.objects().get(BUCKET_NAME, objectName2), assertCallback(object2));

    // 7. Validate that no requests were sent after 2nd request were queued
    assertThat(httpRequestInitializer.getAllRequestStrings()).isEmpty();

    // 8. Call `flush` at the end.
    batchHelper.flush();

    // 9. Validate that 1 batch request consisting of 2 GET requests was sent
    assertThat(httpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(BUCKET_NAME, objectName1),
            getRequestString(BUCKET_NAME, objectName2));
  }

  @Test
  public void queue_throwsException_afterFlushMethodWasCalled() throws IOException {
    String objectName1 = OBJECT_NAME + "-01";
    StorageObject object1 = newStorageObject(BUCKET_NAME, objectName1);

    MockHttpTransport transport =
        GoogleCloudStorageTestUtils.mockBatchTransport(
            /* requestsPerBatch= */ 1, jsonDataResponse(object1));

    Storage storage = new Storage(transport, JSON_FACTORY, httpRequestInitializer);
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(httpRequestInitializer, storage, /* maxRequestsPerBatch= */ 1);

    batchHelper.flush();

    Storage.Objects.Get request = storage.objects().get(BUCKET_NAME, objectName1);
    JsonBatchCallback<StorageObject> callback = assertCallback(object1);

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> batchHelper.queue(request, callback));

    assertThat(e)
        .hasMessageThat()
        .startsWith("requestsExecutor should not be terminated to queue batch requests");
    assertThat(httpRequestInitializer.getAllRequests()).isEmpty();
  }

  @Test
  public void testIsEmpty() {
    Storage storage = new Storage(HTTP_TRANSPORT, JSON_FACTORY, httpRequestInitializer);
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(httpRequestInitializer, storage, /* maxRequestsPerBatch= */ 2);

    assertThat(batchHelper.isEmpty()).isTrue();
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
