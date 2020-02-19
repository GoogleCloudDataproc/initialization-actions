/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTest.newStorageObject;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.fakeResponse;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.DateTime;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.BackOffFactory;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for {@link GoogleCloudStorage} class that use Mockito. The underlying Storage API
 * object is mocked, in order to test behavior in response to various types of unexpected
 * exceptions/errors.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageMockitoTest {

  private static final String APP_NAME = "GCS-Unit-Test";
  private static final String PROJECT_ID = "google.com:foo-project";
  private static final String BUCKET_NAME = "foo-bucket";
  private static final String OBJECT_NAME = "bar-object";

  private ExecutorService executorService;

  @Mock private Storage mockStorage;
  @Mock private Storage.Objects mockStorageObjects;
  @Mock private Storage.Objects.Insert mockStorageObjectsInsert;
  @Mock private Storage.Objects.Delete mockStorageObjectsDelete;
  @Mock private Storage.Objects.Get mockStorageObjectsGet;
  @Mock private Storage.Objects.Copy mockStorageObjectsCopy;
  @Mock private Storage.Objects.Compose mockStorageObjectsCompose;
  @Mock private Storage.Objects.List mockStorageObjectsList;
  @Mock private Storage.Buckets mockStorageBuckets;
  @Mock private Storage.Buckets.Insert mockStorageBucketsInsert;
  @Mock private Storage.Buckets.Delete mockStorageBucketsDelete;
  @Mock private Storage.Buckets.Get mockStorageBucketsGet;
  @Mock private Storage.Buckets.Get mockStorageBucketsGet2;
  @Mock private Storage.Buckets.List mockStorageBucketsList;
  @Mock private ApiErrorExtractor mockErrorExtractor;
  @Mock private BatchHelper.Factory mockBatchFactory;
  @Mock private BatchHelper mockBatchHelper;
  @Mock private HttpHeaders mockHeaders;
  @Mock private ClientRequestHelper<StorageObject> mockClientRequestHelper;
  @Mock private Sleeper mockSleeper;
  @Mock private NanoClock mockClock;
  @Mock private BackOff mockBackOff;
  @Mock private BackOff mockReadBackOff;
  @Mock private BackOffFactory mockBackOffFactory;

  private GoogleCloudStorage gcs;

  /**
   * Sets up new mocks and create new instance of GoogleCloudStorage configured to only interact
   * with the mocks, run before every test case.
   */
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    executorService = Executors.newCachedThreadPool();
    gcs = createTestInstance();
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl as the concrete type and
   * setting up the proper mocks.
   */
  protected GoogleCloudStorageOptions.Builder createDefaultCloudStorageOptionsBuilder() {
    return GoogleCloudStorageOptions.builder().setAppName(APP_NAME).setProjectId(PROJECT_ID);
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl as the concrete type and
   * setting up the proper mocks.
   */
  protected GoogleCloudStorage createTestInstance() {
    return createTestInstance(createDefaultCloudStorageOptionsBuilder().build());
  }

  /**
   * Creates an instance of GoogleCloudStorage with the specified options, using
   * GoogleCloudStorageImpl as the concrete type, and setting up the proper mocks.
   */
  protected GoogleCloudStorage createTestInstance(GoogleCloudStorageOptions options) {
    GoogleCloudStorageImpl gcsTestInstance = new GoogleCloudStorageImpl(options, mockStorage);
    gcsTestInstance.setBackgroundTasksThreadPool(executorService);
    gcsTestInstance.setErrorExtractor(mockErrorExtractor);
    gcsTestInstance.setClientRequestHelper(mockClientRequestHelper);
    gcsTestInstance.setBatchFactory(mockBatchFactory);
    gcsTestInstance.setSleeper(mockSleeper);
    gcsTestInstance.setBackOffFactory(mockBackOffFactory);
    return gcsTestInstance;
  }

  protected void setupNonConflictedWrite(Throwable t) throws IOException {
    setupNonConflictedWrite(
        invocation -> {
          throw t;
        });
  }

  protected void setupNonConflictedWrite(Answer<StorageObject> answer) throws IOException {
    when(mockStorageObjects.get(BUCKET_NAME, OBJECT_NAME)).thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute()).thenThrow(new IOException("NotFound"));
    when(mockErrorExtractor.itemNotFound(any(IOException.class))).thenReturn(true);
    when(mockStorageObjectsInsert.execute()).thenAnswer(answer);
  }

  /**
   * Ensure that each test case precisely captures all interactions with the mocks, since all the
   * mocks represent external dependencies.
   */
  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockStorage);
    verifyNoMoreInteractions(mockStorageObjects);
    verifyNoMoreInteractions(mockStorageObjectsInsert);
    verifyNoMoreInteractions(mockStorageObjectsDelete);
    verifyNoMoreInteractions(mockStorageObjectsGet);
    verifyNoMoreInteractions(mockStorageObjectsCopy);
    verifyNoMoreInteractions(mockStorageObjectsCompose);
    verifyNoMoreInteractions(mockStorageObjectsList);
    verifyNoMoreInteractions(mockStorageBuckets);
    verifyNoMoreInteractions(mockStorageBucketsInsert);
    verifyNoMoreInteractions(mockStorageBucketsDelete);
    verifyNoMoreInteractions(mockStorageBucketsGet);
    verifyNoMoreInteractions(mockStorageBucketsGet2);
    verifyNoMoreInteractions(mockStorageBucketsList);
    verifyNoMoreInteractions(mockErrorExtractor);
    verifyNoMoreInteractions(mockBatchFactory);
    verifyNoMoreInteractions(mockBatchHelper);
    verifyNoMoreInteractions(mockHeaders);
    verifyNoMoreInteractions(mockClientRequestHelper);
    verifyNoMoreInteractions(mockSleeper);
    verifyNoMoreInteractions(mockClock);
    verifyNoMoreInteractions(mockBackOff);
    verifyNoMoreInteractions(mockReadBackOff);
    verifyNoMoreInteractions(mockBackOffFactory);
  }

  /**
   * Test handling when the parent thread waiting for the write to finish via the close call is
   * interrupted, that the actual write is cancelled and interrupted as well.
   */
  @Test
  public void testCreateObjectApiInterruptedException() throws Exception {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    when(mockStorageObjects.insert(
            eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    // Set up the mock Insert to wait forever.
    CountDownLatch waitForEverLatch = new CountDownLatch(1);
    CountDownLatch writeStartedLatch = new CountDownLatch(2);
    CountDownLatch threadsDoneLatch = new CountDownLatch(2);
    setupNonConflictedWrite(
        unused -> {
          try {
            writeStartedLatch.countDown();
            waitForEverLatch.await();
            fail("Unexpected to get here.");
            return null;
          } finally {
            threadsDoneLatch.countDown();
          }
        });

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    Future<?> write =
        executorService.submit(
            () -> {
              writeStartedLatch.countDown();
              try {
                IOException ioe = assertThrows(IOException.class, writeChannel::close);
                assertThat(ioe).isInstanceOf(ClosedByInterruptException.class);
              } finally {
                threadsDoneLatch.countDown();
              }
            });
    // Wait for the insert object to be executed, then cancel the writing thread, and finally wait
    // for the two threads to finish.
    assertWithMessage("Neither thread started.")
        .that(writeStartedLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();
    write.cancel(/* interrupt= */ true);
    assertWithMessage("Failed to wait for tasks to get interrupted.")
        .that(threadsDoneLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockStorageObjectsInsert).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockStorageObjectsInsert).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiRuntimeException() throws IOException {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    when(mockStorageObjects.insert(
            eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    // Set up the mock Insert to throw an exception when execute() is called.
    RuntimeException fakeException = new RuntimeException("Fake exception");
    setupNonConflictedWrite(fakeException);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    IOException thrown = assertThrows(IOException.class, writeChannel::close);
    assertThat(thrown).hasCauseThat().isEqualTo(fakeException);

    verify(mockStorageObjectsInsert).execute();
    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockErrorExtractor, atLeastOnce()).itemNotFound(any(IOException.class));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsInsert).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockStorageObjects).get(anyString(), anyString());
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockStorageObjectsInsert).setIfGenerationMatch(anyLong());
  }

  /**
   * Test handling of various types of Errors thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiError() throws IOException {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    // Set up the mock Insert to throw an exception when execute() is called.
    Error fakeError = new Error("Fake error");
    setupNonConflictedWrite(fakeError);

    when(mockStorageObjects.insert(
            eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    Error thrown = assertThrows(Error.class, writeChannel::close);
    assertThat(thrown).isEqualTo(fakeError);

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjects).get(BUCKET_NAME, OBJECT_NAME);
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsInsert).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockStorageObjectsInsert).setIfGenerationMatch(eq(0L));
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockStorageObjectsInsert).execute();
  }

  /**
   * Helper for the shared boilerplate of setting up the low-level "API objects" like
   * mockStorage.objects(), etc., that is common between test cases targeting {@code
   * GoogleCloudStorage.open(StorageResourceId)}.
   *
   * @param size {@link StorageObject} size
   */
  private void setUpBasicMockBehaviorForOpeningReadChannel(long size) throws IOException {
    setUpBasicMockBehaviorForOpeningReadChannel(size, null);
  }

  /**
   * Helper for the shared boilerplate of setting up the low-level "API objects" like
   * mockStorage.objects(), etc., that is common between test cases targeting {@code
   * GoogleCloudStorage.open(StorageResourceId)}.
   *
   * @param size {@link StorageObject} size
   * @param encoding {@link StorageObject} encoding
   */
  private void setUpBasicMockBehaviorForOpeningReadChannel(long size, String encoding)
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockStorageObjectsGet.execute())
        .thenReturn(
            new StorageObject()
                .setBucket(BUCKET_NAME)
                .setName(OBJECT_NAME)
                .setTimeCreated(new DateTime(11L))
                .setUpdated(new DateTime(12L))
                .setSize(BigInteger.valueOf(size))
                .setContentEncoding(encoding)
                .setGeneration(1L)
                .setMetageneration(1L));
  }

  /**
   * Helper for test cases involving {@code GoogleCloudStorage.open(StorageResourceId)} to set up
   * the shared sleeper/clock/backoff mocks and set {@code maxRetries}. Also checks basic invariants
   * of a fresh readChannel, such as its position() and isOpen().
   */
  private void setUpAndValidateReadChannelMocksAndSetMaxRetries(
      GoogleCloudStorageReadChannel readChannel, int maxRetries) throws IOException {
    readChannel.setSleeper(mockSleeper);
    readChannel.setNanoClock(mockClock);
    readChannel.setReadBackOff(mockReadBackOff);
    readChannel.setMaxRetries(maxRetries);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.delete(2).
   */
  @Test
  public void testDeleteObjectApiException() throws IOException {
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.delete(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsDelete);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);

    // Make the errorExtractor claim that our fake notFoundException.
    final GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found exception");
    final GoogleJsonError unexpectedError = new GoogleJsonError();
    unexpectedError.setMessage("Other API exception");

    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> getCallback =
                  (JsonBatchCallback<StorageObject>) invocation.getArguments()[1];
              getCallback.onSuccess(
                  newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(1L), new HttpHeaders());
              return null;
            })
        .doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<Void> callback = (JsonBatchCallback<Void>) args[1];
              try {
                callback.onFailure(notFoundError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> getCallback =
                  (JsonBatchCallback<StorageObject>) invocation.getArguments()[1];
              getCallback.onSuccess(
                  newStorageObject(BUCKET_NAME, OBJECT_NAME).setGeneration(1L), new HttpHeaders());
              return null;
            })
        .doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<Void> callback = (JsonBatchCallback<Void>) args[1];
              try {
                callback.onFailure(unexpectedError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(any(), any());
    doReturn(true).doReturn(false).when(mockErrorExtractor).itemNotFound(any(IOException.class));
    when(mockErrorExtractor.preconditionNotMet(any(IOException.class))).thenReturn(false);

    // First time is the notFoundException; expect the impl to ignore it completely.
    try {
      gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));
    } catch (Exception e) {
      // Make the test output a little more friendly by specifying why an error may have leaked
      // through.
      fail("Expected no exception when mocking itemNotFound error from API call, got " + e);
    }

    // Second time is the unexpectedException.
    assertThrows(
        IOException.class,
        () ->
            gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))));

    verify(mockBatchFactory, times(2))
        .newBatchHelper(any(), eq(mockStorage), anyLong(), anyLong(), anyInt());
    verify(mockStorage, times(4)).objects();
    verify(mockStorageObjects, times(2)).delete(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsDelete, times(2)).setIfGenerationMatch(eq(1L));
    verify(mockBatchHelper, times(4)).queue(any(), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
    verify(mockErrorExtractor).preconditionNotMet(any(IOException.class));
    verify(mockBatchHelper, times(2)).flush();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.copy(4) where srcBucketName == dstBucketName.
   */
  @Test
  public void testCopyObjectsApiExceptionSameBucket() throws IOException {
    String dstObjectName = OBJECT_NAME + "-copy";
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.copy(
            eq(BUCKET_NAME), eq(OBJECT_NAME), eq(BUCKET_NAME), eq(dstObjectName), isNull()))
        .thenReturn(mockStorageObjectsCopy);
    final GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found exception");
    final GoogleJsonError unexpectedError = new GoogleJsonError();
    unexpectedError.setMessage("Other API exception");
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> callback =
                  (JsonBatchCallback<StorageObject>) args[1];
              try {
                callback.onFailure(notFoundError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> callback =
                  (JsonBatchCallback<StorageObject>) args[1];
              try {
                callback.onFailure(unexpectedError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(eq(mockStorageObjectsCopy), any());
    doReturn(true).doReturn(false).when(mockErrorExtractor).itemNotFound(any(IOException.class));

    // Make the test output a little more friendly in case the exception class differs.
    assertThrows(
        FileNotFoundException.class,
        () ->
            gcs.copy(
                BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
                BUCKET_NAME, ImmutableList.of(dstObjectName)));

    assertThrows(
        IOException.class,
        () ->
            gcs.copy(
                BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
                BUCKET_NAME, ImmutableList.of(dstObjectName)));

    verify(mockBatchFactory, times(2))
        .newBatchHelper(any(), eq(mockStorage), anyLong(), anyLong(), anyInt());
    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2))
        .copy(eq(BUCKET_NAME), eq(OBJECT_NAME), eq(BUCKET_NAME), eq(dstObjectName), isNull());
    verify(mockBatchHelper, times(2)).queue(eq(mockStorageObjectsCopy), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
    verify(mockBatchHelper, times(2)).flush();
  }

  @Test
  public void testGetItemInfosApiException() throws IOException {
    when(mockBatchFactory.newBatchHelper(any(), any(Storage.class), anyLong(), anyLong(), anyInt()))
        .thenReturn(mockBatchHelper);

    // Set up the return for the Bucket fetch.
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    final GoogleJsonError unexpectedError = new GoogleJsonError();
    unexpectedError.setMessage("Unexpected API exception ");
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<Bucket> callback = (JsonBatchCallback<Bucket>) args[1];
              try {
                callback.onFailure(unexpectedError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(eq(mockStorageBucketsGet), any());

    // Set up the return for the StorageObject fetch.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> callback =
                  (JsonBatchCallback<StorageObject>) args[1];
              try {
                callback.onFailure(unexpectedError, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(eq(mockStorageObjectsGet), any());

    // We will claim both GoogleJsonErrors are unexpected errors.
    when(mockErrorExtractor.itemNotFound(any(IOException.class))).thenReturn(false);

    // Call in order of StorageObject, ROOT, Bucket.
    IOException ioe =
        assertThrows(
            IOException.class,
            () ->
                gcs.getItemInfos(
                    ImmutableList.of(
                        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                        StorageResourceId.ROOT,
                        new StorageResourceId(BUCKET_NAME))));
    assertThat(ioe.getSuppressed()).isNotNull();
    assertThat(ioe.getSuppressed()).hasLength(2);
    // All invocations still should have been attempted; the exception should have been thrown
    // at the very end.
    verify(mockBatchFactory).newBatchHelper(any(), eq(mockStorage), anyLong(), anyLong(), anyInt());
    verify(mockStorage).buckets();
    verify(mockStorageBuckets).get(eq(BUCKET_NAME));
    verify(mockBatchHelper).queue(eq(mockStorageBucketsGet), any());
    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockBatchHelper).queue(eq(mockStorageObjectsGet), any());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
    verify(mockBatchHelper).flush();
  }

  @Test
  public void testReadWithFailedInplaceSeekSucceeds() throws IOException {
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};
    byte[] testData2 = Arrays.copyOfRange(testData, 3, testData.length);

    setUpBasicMockBehaviorForOpeningReadChannel(testData.length);

    InputStream mockExceptionStream = mock(InputStream.class);
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(1))).thenReturn(1);
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(2)))
        .thenThrow(new IOException("In-place seek IOException"));

    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(fakeResponse("Content-Length", testData.length, mockExceptionStream))
        .thenReturn(
            fakeResponse("Content-Length", testData2.length, new ByteArrayInputStream(testData2)));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(3).build());

    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    assertThat(readChannel.read(ByteBuffer.wrap(new byte[1]))).isEqualTo(1);

    readChannel.position(3);

    byte[] byte3 = new byte[1];
    assertThat(readChannel.read(ByteBuffer.wrap(byte3))).isEqualTo(1);

    assertThat(byte3).isEqualTo(new byte[] {testData[3]});

    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(3)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setAcceptEncoding(eq("gzip"));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=3-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockExceptionStream, times(2)).read(any(byte[].class), eq(0), anyInt());
  }
}
