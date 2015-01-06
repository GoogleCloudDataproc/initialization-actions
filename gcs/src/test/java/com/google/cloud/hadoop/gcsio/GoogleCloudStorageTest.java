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

package com.google.cloud.hadoop.gcsio;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.DateTime;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.BucketAccessControl;
import com.google.api.services.storage.model.Buckets;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.BackOffFactory;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.verification.VerificationMode;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.net.ssl.SSLException;

/**
 * UnitTests for GoogleCloudStorage class. The underlying Storage API object is mocked, in order
 * to test behavior in response to various types of unexpected exceptions/errors.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageTest {
  private static final String APP_NAME = "GCS-Unit-Test";
  private static final String PROJECT_ID = "google.com:foo-project";
  private static final String BUCKET_NAME = "foo-bucket";
  private static final String OBJECT_NAME = "bar-object";
  private static final String[][] ILLEGAL_OBJECTS = {
      {null, "bar-object"}, {"foo-bucket", null}, {"", "bar-object"}, {"foo-bucket", ""}
  };
  private static final Map<String, byte[]> EMPTY_METADATA = ImmutableMap.of();

  @Mock private Storage mockStorage;
  @Mock private Storage.Objects mockStorageObjects;
  @Mock private Storage.Objects.Insert mockStorageObjectsInsert;
  @Mock private Storage.Objects.Delete mockStorageObjectsDelete;
  @Mock private Storage.Objects.Get mockStorageObjectsGet;
  @Mock private Storage.Objects.Copy mockStorageObjectsCopy;
  @Mock private Storage.Objects.List mockStorageObjectsList;
  @Mock private Storage.Buckets mockStorageBuckets;
  @Mock private Storage.Buckets.Insert mockStorageBucketsInsert;
  @Mock private Storage.Buckets.Delete mockStorageBucketsDelete;
  @Mock private Storage.Buckets.Get mockStorageBucketsGet;
  @Mock private Storage.Buckets.Get mockStorageBucketsGet2;
  @Mock private Storage.Buckets.List mockStorageBucketsList;
  @Mock private ApiErrorExtractor mockErrorExtractor;
  @Mock private ExecutorService mockExecutorService;
  @Mock private BatchHelper.Factory mockBatchFactory;
  @Mock private BatchHelper mockBatchHelper;
  @Mock private HttpHeaders mockHeaders;
  @Mock private ClientRequestHelper<StorageObject> mockClientRequestHelper;
  @Mock private Sleeper mockSleeper;
  @Mock private NanoClock mockClock;
  @Mock private BackOff mockBackOff;
  @Mock private BackOffFactory mockBackOffFactory;

  private GoogleCloudStorage gcs;

  /**
   * Performs set up applicable to all tests before any test runs.
   */
  @BeforeClass
  public static void beforeAllTests()
      throws IOException {
  }

  /**
   * Sets up new mocks and create new instance of GoogleCloudStorage configured to only interact
   * with the mocks, run before every test case.
   */
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    gcs = createTestInstance();
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl as the concrete type
   * and setting up the proper mocks.
   */
  protected GoogleCloudStorage createTestInstance() {
    GoogleCloudStorageOptions.Builder optionsBuilder = GoogleCloudStorageOptions.newBuilder();
    optionsBuilder.setAppName(APP_NAME);
    optionsBuilder.setProjectId(PROJECT_ID);

    GoogleCloudStorageImpl gcsTestInstance =
        new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);
    gcsTestInstance.setThreadPool(mockExecutorService);
    gcsTestInstance.setErrorExtractor(mockErrorExtractor);
    gcsTestInstance.setClientRequestHelper(mockClientRequestHelper);
    gcsTestInstance.setBatchFactory(mockBatchFactory);
    gcsTestInstance.setSleeper(mockSleeper);
    gcsTestInstance.setBackOffFactory(mockBackOffFactory);
    return gcsTestInstance;
  }

  protected void setupNonConflictedSuccessfulWrite() throws IOException {
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorageObjects.get(BUCKET_NAME, OBJECT_NAME)).thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute()).thenThrow(new IOException("NotFound"));
    when(mockErrorExtractor.itemNotFound(any(IOException.class))).thenReturn(true);
    when(mockStorageObjectsInsert.execute()).thenReturn(
        new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setGeneration(1L)
            .setMetageneration(1L));
  }

  /**
   * Ensure that each test case precisely captures all interactions with the mocks, since all
   * the mocks represent externel dependencies.
   */
  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockStorage);
    verifyNoMoreInteractions(mockStorageObjects);
    verifyNoMoreInteractions(mockStorageObjectsInsert);
    verifyNoMoreInteractions(mockStorageObjectsDelete);
    verifyNoMoreInteractions(mockStorageObjectsGet);
    verifyNoMoreInteractions(mockStorageObjectsCopy);
    verifyNoMoreInteractions(mockStorageObjectsList);
    verifyNoMoreInteractions(mockStorageBuckets);
    verifyNoMoreInteractions(mockStorageBucketsInsert);
    verifyNoMoreInteractions(mockStorageBucketsDelete);
    verifyNoMoreInteractions(mockStorageBucketsGet);
    verifyNoMoreInteractions(mockStorageBucketsGet2);
    verifyNoMoreInteractions(mockStorageBucketsList);
    verifyNoMoreInteractions(mockErrorExtractor);
    verifyNoMoreInteractions(mockExecutorService);
    verifyNoMoreInteractions(mockBatchFactory);
    verifyNoMoreInteractions(mockBatchHelper);
    verifyNoMoreInteractions(mockHeaders);
    verifyNoMoreInteractions(mockClientRequestHelper);
    verifyNoMoreInteractions(mockSleeper);
    verifyNoMoreInteractions(mockClock);
    verifyNoMoreInteractions(mockBackOff);
    verifyNoMoreInteractions(mockBackOffFactory);
  }

  /**
   * Handles all the boilerplate mocking of the HttpTransport and HttpRequest in order to build a
   * fake HttpResponse with the provided {@code contentLength} and {@code content} to get around
   * the fact that HttpResponse is a "final class".
   */
  private HttpResponse createFakeResponse(final long contentLength, final InputStream content)
      throws IOException {
       return createFakeResponse("Content-Length", Long.toString(contentLength), content);
  }

  /** Like createFakeResponse, but responds with a Content-Range header */
  private HttpResponse createFakeResponseForRange(final long contentLength,
                                                  final InputStream content)
      throws IOException {
       return createFakeResponse("Content-Range", "bytes=0-123/" + contentLength, content);
  }

  private HttpResponse createFakeResponse(final String responseHeader, final String responseValue,
                                          final InputStream content) throws IOException {
    HttpTransport transport = new MockHttpTransport() {
      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
        MockLowLevelHttpRequest req = new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() throws IOException {
            return new MockLowLevelHttpResponse()
                .addHeader(responseHeader, responseValue)
                .setContent(content);
          }
        };
        return req;
      }
    };
    HttpRequest request =
        transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
    return request.execute();
  }

  /**
   * Test argument sanitization for GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectIllegalArguments()
      throws IOException {
    for (String[] objectPair : ILLEGAL_OBJECTS) {
      try {
        gcs.create(new StorageResourceId(objectPair[0], objectPair[1]));
        fail();
      } catch (IllegalArgumentException iae) {
        // Expected.
      }
    }
  }

  /**
   * Test successful operation of GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectNormalOperation()
      throws IOException {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    setupNonConflictedSuccessfulWrite();

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsInsert)))
        .thenReturn(mockHeaders);

    // Get a channel, don't run the async Runnable it created yet.
    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertTrue(writeChannel.isOpen());

    // Verify the initial setup of the insert; capture the API objects and async task for later.
    ArgumentCaptor<StorageObject> storageObjectCaptor =
        ArgumentCaptor.forClass(StorageObject.class);
    final ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), storageObjectCaptor.capture(), inputStreamCaptor.capture());
    verify(mockStorageObjects, times(1)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(1)).execute();
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(1L));
    assertEquals(OBJECT_NAME, storageObjectCaptor.getValue().getName());
    verify(mockHeaders, times(2)).set(startsWith("X-Goog-Upload-"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Insert.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockBackOffFactory).newBackOff();
    verify(mockBackOff).nextBackOffMillis();

    ArgumentCaptor<Runnable> runCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService).execute(runCaptor.capture());

    // Now write some data into the channel before running the captured Runnable; set up our
    // mock Insert to stash away the bytes upon execute().
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    writeChannel.write(ByteBuffer.wrap(testData));
    final byte[] readData = new byte[testData.length];
    when(mockStorageObjectsInsert.execute())
        .thenAnswer(new Answer<BucketAccessControl>() {
          @Override
          public BucketAccessControl answer(InvocationOnMock unused) {
            try {
              inputStreamCaptor.getValue().getInputStream().read(readData);
            } catch (IOException ioe) {
              fail(ioe.getMessage());
            }
            return null;
          }
        });
    runCaptor.getValue().run();

    // Flush, make sure the data made it out the other end.
    writeChannel.close();
    assertFalse(writeChannel.isOpen());
    verify(mockStorageObjectsInsert, times(2)).execute();
    assertArrayEquals(testData, readData);

    // After closing the channel, future writes or calls to close() should throw a
    // ClosedChannelException.
    try {
      writeChannel.close();
      fail("Expected ClosedChannelException");
    } catch (ClosedChannelException ioe) {
      // Expected.
    }
    try {
      writeChannel.write(ByteBuffer.wrap(testData));
      fail("Expected ClosedChannelException");
    } catch (ClosedChannelException ioe) {
      // Expected.
    }
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiIOException()
      throws IOException {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsInsert)))
        .thenReturn(mockHeaders);

    setupNonConflictedSuccessfulWrite();

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertTrue(writeChannel.isOpen());

    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockHeaders, times(2)).set(startsWith("X-Goog-Upload-"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Insert.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(1L));
    verify(mockStorageObjects, times(1)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(1)).execute();
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockBackOffFactory).newBackOff();
    verify(mockBackOff).nextBackOffMillis();


    ArgumentCaptor<Runnable> runCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService).execute(runCaptor.capture());

    // Set up the mock Insert to throw an exception when execute() is called.
    IOException fakeException = new IOException("Fake IOException");
    when(mockStorageObjectsInsert.execute())
        .thenThrow(fakeException);
    runCaptor.getValue().run();

    try {
      writeChannel.close();
      fail("Expected IOException");
    } catch (IOException ioe) {
      assertEquals(fakeException, ioe.getCause());
    }
    verify(mockStorageObjectsInsert, times(2)).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiRuntimeException()
      throws IOException {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    setupNonConflictedSuccessfulWrite();

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute()).thenReturn(new StorageObject().setGeneration(10L));
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsInsert)))
        .thenReturn(mockHeaders);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertTrue(writeChannel.isOpen());

    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjects, times(1)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockErrorExtractor, atLeastOnce()).itemNotFound(any(IOException.class));
    verify(mockBackOffFactory, atLeastOnce()).newBackOff();
    verify(mockBackOff, times(1)).nextBackOffMillis();
    verify(mockStorageObjectsGet, times(1)).execute();
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockStorageObjects, times(1)).get(anyString(), anyString());
    verify(mockHeaders, times(2)).set(startsWith("X-Goog-Upload-"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Insert.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert, times(2)).setIfGenerationMatch(anyLong());
    ArgumentCaptor<Runnable> runCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService).execute(runCaptor.capture());

    // Set up the mock Insert to throw an exception when execute() is called.
    RuntimeException fakeException = new RuntimeException("Fake exception");
    when(mockStorageObjectsInsert.execute())
        .thenThrow(fakeException);
    runCaptor.getValue().run();

    try {
      writeChannel.close();
      fail("Expected IOException");
    } catch (IOException ioe) {
      assertEquals(fakeException, ioe.getCause());
    }
    verify(mockStorageObjectsInsert, times(2)).execute();
  }

  /**
   * Test handling of various types of Errors thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiError()
      throws IOException {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    setupNonConflictedSuccessfulWrite();

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsInsert)))
        .thenReturn(mockHeaders);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertTrue(writeChannel.isOpen());

    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjects).get(BUCKET_NAME, OBJECT_NAME);
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(1L));
    verify(mockErrorExtractor, times(1)).itemNotFound(any(IOException.class));
    verify(mockBackOff, atLeastOnce()).nextBackOffMillis();
    verify(mockBackOffFactory, atLeastOnce()).newBackOff();
    verify(mockHeaders, times(2)).set(startsWith("X-Goog-Upload-"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Insert.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));

    ArgumentCaptor<Runnable> runCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService).execute(runCaptor.capture());

    // Set up the mock Insert to throw an exception when execute() is called.
    Error fakeError = new Error("Fake error");
    when(mockStorageObjectsInsert.execute())
        .thenThrow(fakeError);
    runCaptor.getValue().run();

    try {
      writeChannel.close();
      fail("Expected Error");
    } catch (Error error) {
      assertEquals(fakeError, error);
    }
    verify(mockStorageObjectsInsert, times(2)).execute();
  }

  /**
   * Test successful operation of GoogleCloudStorage.createEmptyObject(1).
   */
  @Test
  public void testCreateEmptyObject()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    gcs.createEmptyObject(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    verify(mockStorage).objects();
    ArgumentCaptor<StorageObject> storageObjectCaptor =
        ArgumentCaptor.forClass(StorageObject.class);
    ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), storageObjectCaptor.capture(), inputStreamCaptor.capture());
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();

    assertEquals(OBJECT_NAME, storageObjectCaptor.getValue().getName());
    assertEquals(0, inputStreamCaptor.getValue().getLength());
  }

  /**
   * Test argument sanitization for GoogleCloudStorage.open(2).
   */
  @Test
  public void testOpenObjectIllegalArguments()
      throws IOException {
    for (String[] objectPair : ILLEGAL_OBJECTS) {
      try {
        gcs.open(new StorageResourceId(objectPair[0], objectPair[1]));
        fail();
      } catch (IllegalArgumentException iae) {
        // Expected.
      }
    }
  }

  @Test
  public void testOpenWithSomeExceptionsDuringRead()
      throws IOException, InterruptedException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockStorageObjectsGet.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L));

    // First returned timeout stream will timout; we'll expect a re-opening where we'll return the
    // real input stream.
    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream))
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream))
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream))
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(testData)));

    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new SocketTimeoutException("fake timeout"))
        .thenThrow(new SSLException("fake SSLException"))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockBackOff.nextBackOffMillis())
        .thenReturn(111L)
        .thenReturn(222L)
        .thenReturn(333L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setSleeper(mockSleeper);
    readChannel.setNanoClock(mockClock);
    readChannel.setBackOff(mockBackOff);
    readChannel.setMaxRetries(3);
    assertTrue(readChannel.isOpen());
    assertEquals(0, readChannel.position());

    byte[] actualData = new byte[testData.length];
    assertEquals(testData.length, readChannel.read(ByteBuffer.wrap(actualData)));

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(4)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(4)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsGet, times(4)).executeMedia();
    verify(mockBackOff).reset();
    verify(mockBackOff, times(3)).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));
    verify(mockSleeper).sleep(eq(222L));
    verify(mockSleeper).sleep(eq(333L));
  }

  @Test
  public void testOpenExceptionsDuringReadTotalElapsedTimeTooGreat()
      throws IOException, InterruptedException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockStorageObjectsGet.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L));

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream));
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockClock.nanoTime())
        .thenReturn(1000000L)  // 'Start time'
        .thenReturn(
            (GoogleCloudStorageReadChannel.DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS + 2) * 1000000L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setSleeper(mockSleeper);
    readChannel.setNanoClock(mockClock);
    readChannel.setMaxRetries(3);
    assertTrue(readChannel.isOpen());
    assertEquals(0, readChannel.position());

    try {
      byte[] actualData = new byte[testData.length];
      readChannel.read(ByteBuffer.wrap(actualData));
      fail("Expected to throw SocketTimeoutException, but nothing was thrown.");
    } catch (IOException ste) {
      // Throws the last one.
      assertEquals("fake generic IOException", ste.getMessage());
    }

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).executeMedia();
    verify(mockStorageObjectsGet).execute();
    verify(mockClock, times(2)).nanoTime();
  }

  @Test
  public void testOpenExceptionsDuringReadInterruptedDuringSleep()
      throws IOException, InterruptedException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockStorageObjectsGet.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L));

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream));
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockBackOff.nextBackOffMillis())
        .thenReturn(111L);
    InterruptedException interrupt = new InterruptedException("fake interrupt");
    doThrow(interrupt)
        .when(mockSleeper).sleep(eq(111L));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setSleeper(mockSleeper);
    readChannel.setNanoClock(mockClock);
    readChannel.setBackOff(mockBackOff);
    readChannel.setMaxRetries(3);
    assertTrue(readChannel.isOpen());
    assertEquals(0, readChannel.position());

    try {
      byte[] actualData = new byte[testData.length];
      readChannel.read(ByteBuffer.wrap(actualData));
      fail("Expected to throw SocketTimeoutException, but nothing was thrown.");
    } catch (IOException ste) {
      // Throws the last one, with the underlying interrupted exception added as the supressed
      // exception.
      assertEquals("fake generic IOException", ste.getMessage());
      assertEquals(1, ste.getSuppressed().length);
      assertEquals(interrupt, ste.getSuppressed()[0]);
    }

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).executeMedia();
    verify(mockStorageObjectsGet).execute();
    verify(mockBackOff).reset();
    verify(mockBackOff).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));
  }

  @Test
  public void testOpenTooManyExceptionsDuringRead()
      throws IOException, InterruptedException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockStorageObjectsGet.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L));

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream))
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream))
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream));

    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new SocketTimeoutException("fake timeout"))
        .thenThrow(new SSLException("fake SSLException"))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockBackOff.nextBackOffMillis())
        .thenReturn(111L)
        .thenReturn(222L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setSleeper(mockSleeper);
    readChannel.setNanoClock(mockClock);
    readChannel.setBackOff(mockBackOff);
    readChannel.setMaxRetries(2);
    assertTrue(readChannel.isOpen());
    assertEquals(0, readChannel.position());

    try {
      byte[] actualData = new byte[testData.length];
      readChannel.read(ByteBuffer.wrap(actualData));
      fail("Expected to throw SocketTimeoutException, but nothing was thrown.");
    } catch (IOException ste) {
      // Throws the last one.
      assertEquals("fake generic IOException", ste.getMessage());
    }

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet).execute();
    verify(mockBackOff).reset();
    verify(mockBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));
    verify(mockSleeper).sleep(eq(222L));
  }

  @Test
  public void testOpenTwoTimeoutsWithIntermittentProgress()
      throws IOException, InterruptedException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockStorageObjectsGet.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L));

    // This stream will immediately timeout.
    InputStream mockTimeoutStream = mock(InputStream.class);

    // This stream will first read 2 bytes and then timeout.
    InputStream mockFlakyStream = mock(InputStream.class);

    final byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    final byte[] testData2 = { 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockTimeoutStream))
        .thenReturn(createFakeResponse(testData.length, mockFlakyStream))
        .thenReturn(createFakeResponse(testData2.length, new ByteArrayInputStream(testData2)));

    when(mockTimeoutStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new SocketTimeoutException("fake timeout 1"));
    when(mockFlakyStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            byte[] inputBuf = (byte[]) args[0];
            // Mimic successfully reading two bytes.
            System.arraycopy(testData, 0, inputBuf, 0, 2);
            return Integer.valueOf(2);
          }
        });
    // After reading two bytes, there will be 3 remaining slots in the input buffer.
    when(mockFlakyStream.read(any(byte[].class), eq(0), eq(3)))
        .thenAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            byte[] inputBuf = (byte[]) args[0];
            // Mimic successfully reading one byte from position 3 of testData.
            System.arraycopy(testData, 2, inputBuf, 0, 1);
            return Integer.valueOf(1);
          }
        });
    when(mockFlakyStream.read(any(byte[].class), eq(0), eq(2)))
        .thenThrow(new SocketTimeoutException("fake timeout 2"));
    when(mockFlakyStream.available())
        .thenReturn(0)  // After first two bytes, claim none are available.
        // Will check after reading 3rd byte, claim 1 more byte is available, but actually throw
        // exception when trying to retrieve the 4th byte.
        .thenReturn(1);

    when(mockBackOff.nextBackOffMillis())
        .thenReturn(111L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setSleeper(mockSleeper);
    readChannel.setNanoClock(mockClock);
    readChannel.setBackOff(mockBackOff);
    readChannel.setMaxRetries(1);
    assertTrue(readChannel.isOpen());
    assertEquals(0, readChannel.position());

    // Should succeed even though, in total, there were more retries than maxRetries, since we
    // made progress between errors.
    byte[] actualData = new byte[testData.length];
    assertEquals(testData.length, readChannel.read(ByteBuffer.wrap(actualData)));
    assertEquals(5, readChannel.position());

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet).execute();
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=3-"));
    verify(mockBackOff, times(2)).reset();
    verify(mockBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper, times(2)).sleep(eq(111L));
  }

  /**
   * Test successful operation of GoogleCloudStorage.open(2).
   */
  @Test
  public void testOpenObjectNormalOperation()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockStorageObjectsGet.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L));

    // Make the response return some fake data, prepare for a second API call when we call
    // 'position' on the returned channel.
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    byte[] testData2 = { 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(testData)))
        .thenReturn(createFakeResponseForRange(
            testData2.length, new ByteArrayInputStream(testData2)));

    SeekableReadableByteChannel readChannel =
        gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertTrue(readChannel.isOpen());
    assertEquals(0, readChannel.position());
    byte[] actualData = new byte[testData.length];
    assertEquals(testData.length, readChannel.read(ByteBuffer.wrap(actualData)));
    assertArrayEquals(testData, actualData);
    assertEquals(testData.length, readChannel.position());

    readChannel.position(2);
    assertTrue(readChannel.isOpen());
    assertEquals(2, readChannel.position());

    // Repositioning to invalid position fails.
    try {
      readChannel.position(-1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
    try {
      readChannel.position(testData.length);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    // Repositioning to current position should result in no API calls.
    readChannel.position(2);
    assertTrue(readChannel.isOpen());
    assertEquals(2, readChannel.position());

    // Reading into a buffer with no room should have no effect.
    assertEquals(0, readChannel.read(ByteBuffer.wrap(new byte[0])));
    assertTrue(readChannel.isOpen());
    assertEquals(2, readChannel.position());

    byte[] actualData2 = new byte[testData2.length];
    assertEquals(testData2.length, readChannel.read(ByteBuffer.wrap(actualData2)));
    assertArrayEquals(testData2, actualData2);

    // Note that position will be testData.length, *not* testData2.length (5, not 3).
    assertEquals(testData.length, readChannel.position());

    // Verify the request being made.
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=2-"));
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockStorageObjectsGet).execute();

    readChannel.close();
    assertFalse(readChannel.isOpen());

    // The BackOff should be lazily instantiated, so if nothing went wrong, it will still be null.
    GoogleCloudStorageReadChannel castedReadChannel = (GoogleCloudStorageReadChannel) readChannel;
    assertNull(castedReadChannel.getBackOff());

    // After closing the channel, future reads or calls to close() should throw a
    // ClosedChannelException.
    try {
      readChannel.close();
      fail("Expected ClosedChannelException");
    } catch (ClosedChannelException ioe) {
      // Expected.
    }
    try {
      readChannel.read(ByteBuffer.wrap(actualData));
      fail("Expected ClosedChannelException");
    } catch (ClosedChannelException ioe) {
      // Expected.
    }
    try {
      readChannel.position(0);
      fail("Expected ClosedChannelException");
    } catch (ClosedChannelException ioe) {
      // Expected.
    }
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.open(2).
   */
  @Test
  public void testOpenObjectApiException()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);

    IOException notFoundException = new IOException("Fake not-found exception");
    IOException rangeNotSatisfiableException =
        new IOException("Fake range-not-satisfiable exception");
    IOException unexpectedException = new IOException("Other API exception");

    when(mockStorageObjectsGet.execute())
        .thenThrow(notFoundException)
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L))
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L));

    when(mockStorageObjectsGet.executeMedia())
        .thenThrow(rangeNotSatisfiableException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(rangeNotSatisfiableException)))
        .thenReturn(false);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);
    when(mockErrorExtractor.rangeNotSatisfiable(eq(rangeNotSatisfiableException)))
        .thenReturn(true);
    when(mockErrorExtractor.rangeNotSatisfiable(eq(unexpectedException)))
        .thenReturn(false);

    // First time is the notFoundException.
    SeekableReadableByteChannel readChannel;
    try {
      readChannel = gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
      fail("Expected FileNotFoundException");
    } catch (FileNotFoundException e) {
      // Expected.
    } catch (Exception e) {
      // Make the test output a little more friendly in case the exception class differs.
      fail("Expected FileNotFoundException, got " + e.getClass().getName());
    }

    // Second time is the rangeNotSatisfiableException.
    readChannel = gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertEquals(0, readChannel.size());

    // Third time is the unexpectedException.
    readChannel = gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    try {
      // Call size() to force the underlying stream to be opened.
      readChannel.size();
      fail("Expected unexpectedException");
    } catch (IOException e) {
      assertEquals(unexpectedException, e.getCause());
    }

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(3)).execute();
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockErrorExtractor, times(3)).itemNotFound(any(IOException.class));
    verify(mockErrorExtractor, times(2)).rangeNotSatisfiable(any(IOException.class));
  }

  /**
   * Test argument sanitization for GoogleCloudStorage.create(String).
   */
  @Test
  public void testCreateBucketIllegalArguments()
      throws IOException {
    try {
      gcs.create((String) null);
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
    try {
      gcs.create("");
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
  }

  /**
   * Test successful operation of GoogleCloudStorage.create(String).
   */
  @Test
  public void testCreateBucketNormalOperation()
      throws IOException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.insert(eq(PROJECT_ID), any(Bucket.class)))
        .thenReturn(mockStorageBucketsInsert);
    gcs.create(BUCKET_NAME);
    verify(mockStorage).buckets();
    verify(mockBackOffFactory).newBackOff();
    verify(mockStorageBuckets).insert(eq(PROJECT_ID), any(Bucket.class));
    verify(mockStorageBucketsInsert).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(String).
   */
  @Test
  public void testCreateBucketApiException()
      throws IOException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.insert(eq(PROJECT_ID), any(Bucket.class)))
        .thenReturn(mockStorageBucketsInsert);
    when(mockStorageBucketsInsert.execute())
        .thenThrow(new IOException("Fake exception"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(false);

    try {
      gcs.create(BUCKET_NAME);
      fail();
    } catch (IOException ioe) {
      // Expected.
      // TODO(user): Switch to testing for FileExistsException once implemented.
    }

    verify(mockStorage).buckets();
    verify(mockBackOffFactory).newBackOff();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageBuckets).insert(eq(PROJECT_ID), any(Bucket.class));
    verify(mockStorageBucketsInsert).execute();
  }

  /**
   * Test handling of rate-limiting and back-off in GoogleCloudStorage.create(String).
   */
  @Test
  public void testCreateBucketRateLimited()
      throws IOException, InterruptedException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.insert(eq(PROJECT_ID), any(Bucket.class)))
        .thenReturn(mockStorageBucketsInsert);
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorageBucketsInsert.execute())
        .thenThrow(new IOException("Fake exception")) // Will be interpreted as rate-limited
        .thenReturn(new Bucket());
    when(mockBackOff.nextBackOffMillis()).thenReturn(100L);
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);

    gcs.create(BUCKET_NAME);

    verify(mockStorage).buckets();
    verify(mockBackOffFactory).newBackOff();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockBackOff).nextBackOffMillis();
    verify(mockSleeper).sleep(100L);
    verify(mockStorageBuckets).insert(eq(PROJECT_ID), any(Bucket.class));
    verify(mockStorageBucketsInsert, times(2)).execute();
  }

  /**
   * Test argument sanitization for GoogleCloudStorage.delete(1).
   */
  @Test
  public void testDeleteBucketIllegalArguments()
      throws IOException {
    try {
      gcs.deleteBuckets(Lists.<String>newArrayList((String) null));
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
    try {
      gcs.deleteBuckets(Lists.newArrayList(""));
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
  }

  /**
   * Test successful operation of GoogleCloudStorage.delete(1).
   */
  @Test
  public void testDeleteBucketNormalOperation()
      throws IOException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.delete(eq(BUCKET_NAME)))
        .thenReturn(mockStorageBucketsDelete);

    gcs.deleteBuckets(Lists.newArrayList(BUCKET_NAME));

    verify(mockStorage).buckets();
    verify(mockStorageBuckets).delete(eq(BUCKET_NAME));
    verify(mockStorageBucketsDelete).execute();
    verify(mockBackOffFactory).newBackOff();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.delete(1).
   */
  @Test
  public void testDeleteBucketApiException()
      throws IOException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.delete(eq(BUCKET_NAME)))
        .thenReturn(mockStorageBucketsDelete);

    final IOException notFoundException = new IOException("Fake not found exception");
    final IOException unexpectedException = new IOException("Fake unknown exception");

    when(mockStorageBucketsDelete.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException);

    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);

    // First time is the notFoundException.
    try {
      gcs.deleteBuckets(Lists.newArrayList(BUCKET_NAME));
      fail("Expected FileNotFoundException");
    } catch (FileNotFoundException e) {
      // Expected.
    } catch (Exception e) {
      // Make the test output a little more friendly in case the exception class differs.
      fail("Expected FileNotFoundException, got " + e.getClass().getName());
    }

    // Second time is the unexpectedException.
    try {
      gcs.deleteBuckets(Lists.newArrayList(BUCKET_NAME));
      fail("Expected IOException");
    } catch (IOException e) {
      // Expected.
    }

    verify(mockStorage, times(2)).buckets();
    verify(mockStorageBuckets, times(2)).delete(eq(BUCKET_NAME));
    verify(mockStorageBucketsDelete, times(2)).execute();
    verify(mockErrorExtractor, times(2)).rateLimited(any(IOException.class));
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
    verify(mockBackOffFactory, times(2)).newBackOff();
  }

  /**
   * Test handling of rate-limiting and back-off in GoogleCloudStorage.delete(1).
   */
  @Test
  public void testDeleteBucketRateLimited()
      throws IOException, InterruptedException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.delete(eq(BUCKET_NAME)))
        .thenReturn(mockStorageBucketsDelete);
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorageBucketsDelete.execute())
        .thenThrow(new IOException("Fake Exception"))
        .thenReturn(null);
    when(mockBackOff.nextBackOffMillis()).thenReturn(100L);
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);

    gcs.deleteBuckets(ImmutableList.of(BUCKET_NAME));

    verify(mockStorage).buckets();
    verify(mockBackOffFactory).newBackOff();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockBackOff).nextBackOffMillis();
    verify(mockSleeper).sleep(100L);
    verify(mockStorageBuckets).delete(eq(BUCKET_NAME));
    verify(mockStorageBucketsDelete, times(2)).execute();
  }

  /**
   * Test argument sanitization for GoogleCloudStorage.delete(2).
   */
  @Test
  public void testDeleteObjectIllegalArguments()
      throws IOException {
    for (String[] objectPair : ILLEGAL_OBJECTS) {
      try {
        gcs.deleteObjects(
            Lists.newArrayList(new StorageResourceId(objectPair[0], objectPair[1])));
        fail();
      } catch (IllegalArgumentException iae) {
        // Expected.
      }
    }
  }

  /**
   * Test successful operation of GoogleCloudStorage.delete(2).
   */
  @Test
  public void testDeleteObjectNormalOperation()
      throws IOException {
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class),
        any(Storage.class), any(Long.class))).thenReturn(mockBatchHelper);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjects.delete(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsDelete);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        JsonBatchCallback<StorageObject> getCallback =
            (JsonBatchCallback<StorageObject>) invocationOnMock.getArguments()[1];
        getCallback.onSuccess(
            new StorageObject()
                .setBucket(BUCKET_NAME)
                .setName(OBJECT_NAME)
                .setUpdated(new DateTime(11L))
                .setSize(BigInteger.valueOf(111L))
                .setGeneration(1L)
                .setMetageneration(1L),
            new HttpHeaders());
        return null;
      }
    }).doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        JsonBatchCallback<Void> callback =
            (JsonBatchCallback<Void>) invocationOnMock.getArguments()[1];
        callback.onSuccess(null, new HttpHeaders());
        return null;
      }
    }).when(mockBatchHelper).queue(
        Matchers.<StorageRequest<Object>>anyObject(),
        Matchers.<JsonBatchCallback<Object>>anyObject());

    when(mockBatchHelper.isEmpty())
        .thenReturn(false)
        .thenReturn(true);

    gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    verify(mockBatchFactory).newBatchHelper(any(HttpRequestInitializer.class),
        eq(mockStorage), any(Long.class));
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects).delete(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsDelete).setIfGenerationMatch(eq(1L));
    verify(mockBatchHelper, times(2)).isEmpty();
    verify(mockBatchHelper, times(2)).flush();
    verify(mockBatchHelper, times(2))
        .queue(
            Matchers.<StorageRequest<Object>>anyObject(),
            Matchers.<JsonBatchCallback<Object>>anyObject());
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.delete(2).
   */
  @Test
  public void testDeleteObjectApiException()
      throws IOException {
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class),
        any(Storage.class), any(Long.class))).thenReturn(mockBatchHelper);
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

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        JsonBatchCallback<StorageObject> getCallback =
            (JsonBatchCallback<StorageObject>) invocationOnMock.getArguments()[1];
        getCallback.onSuccess(
            new StorageObject()
                .setBucket(BUCKET_NAME)
                .setName(OBJECT_NAME)
                .setUpdated(new DateTime(11L))
                .setSize(BigInteger.valueOf(111L))
                .setGeneration(1L)
                .setMetageneration(1L),
            new HttpHeaders());
        return null;
      }
    }).doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<Void> callback = (JsonBatchCallback<Void>) args[1];
        try {
          callback.onFailure(notFoundError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        JsonBatchCallback<StorageObject> getCallback =
            (JsonBatchCallback<StorageObject>) invocationOnMock.getArguments()[1];
        getCallback.onSuccess(
            new StorageObject()
                .setBucket(BUCKET_NAME)
                .setName(OBJECT_NAME)
                .setUpdated(new DateTime(11L))
                .setSize(BigInteger.valueOf(111L))
                .setGeneration(1L)
                .setMetageneration(1L),
            new HttpHeaders());
        return null;
      }
    }).doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<Void> callback = (JsonBatchCallback<Void>) args[1];
        try {
          callback.onFailure(unexpectedError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).when(mockBatchHelper).queue(
        Matchers.<StorageRequest<Object>>anyObject(),
        Matchers.<JsonBatchCallback<Object>>anyObject());

    when(mockErrorExtractor.itemNotFound(eq(notFoundError)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedError)))
        .thenReturn(false);
    when(mockErrorExtractor.preconditionNotMet(any(GoogleJsonError.class)))
        .thenReturn(false);

    when(mockBatchHelper.isEmpty())
        .thenReturn(false)
        .thenReturn(true)
        .thenReturn(false)
        .thenReturn(true);

    // First time is the notFoundException; expect the impl to ignore it completely.
    try {
      gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));
    } catch (Exception e) {
      // Make the test output a little more friendly by specifying why an error may have leaked
      // through.
      fail("Expected no exception when mocking itemNotFound error from API call, got "
           + e.toString());
    }

    // Second time is the unexpectedException.
    try {
      gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));
      fail("Expected IOException");
    } catch (IOException e) {
      // Expected.
    }

    verify(mockBatchFactory, times(2)).newBatchHelper(any(HttpRequestInitializer.class),
        eq(mockStorage), any(Long.class));
    verify(mockStorage, times(4)).objects();
    verify(mockStorageObjects, times(2)).delete(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsDelete, times(2)).setIfGenerationMatch(eq(1L));
    verify(mockBatchHelper, times(4)).queue(
        Matchers.<StorageRequest<Object>>anyObject(),
        Matchers.<JsonBatchCallback<Object>>anyObject());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(GoogleJsonError.class));
    verify(mockErrorExtractor, times(1)).preconditionNotMet(any(GoogleJsonError.class));
    verify(mockBatchHelper, times(4)).flush();
    verify(mockBatchHelper, times(4)).isEmpty();
  }

  /**
   * Test argument sanitization for GoogleCloudStorage.copy(4).
   */
  @Test
  public void testCopyObjectsIllegalArguments()
      throws IOException {
    for (String[] objectPair : ILLEGAL_OBJECTS) {
      try {
        // Src is bad.
        gcs.copy(objectPair[0], Lists.newArrayList(objectPair[1]),
                 BUCKET_NAME, Lists.newArrayList(OBJECT_NAME));
        fail();
      } catch (IllegalArgumentException iae) {
        // Expected.
      }
      try {
        // Dst is bad.
        gcs.copy(BUCKET_NAME, Lists.newArrayList(OBJECT_NAME),
                 objectPair[0], Lists.newArrayList(objectPair[1]));
        fail();
      } catch (IllegalArgumentException iae) {
        // Expected.
      }
    }

    // Failure if src == dst.
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               BUCKET_NAME, ImmutableList.of(OBJECT_NAME));
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    // Null lists.
    try {
      gcs.copy(BUCKET_NAME, null,
               BUCKET_NAME, ImmutableList.of(OBJECT_NAME));
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               BUCKET_NAME, null);
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    // Mismatched number of objects.
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               BUCKET_NAME, ImmutableList.of(OBJECT_NAME + "1", OBJECT_NAME + "2"));
      fail();
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
  }

  /**
   * Test successful operation of GoogleCloudStorage.copy(4) where srcBucketName == dstBucketName.
   */
  @Test
  public void testCopyObjectsNormalOperationSameBucket()
      throws IOException {
    String dstObjectName = OBJECT_NAME + "-copy";
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class),
        any(Storage.class), any(Long.class))).thenReturn(mockBatchHelper);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.copy(
        eq(BUCKET_NAME), eq(OBJECT_NAME), eq(BUCKET_NAME), eq(dstObjectName),
        (StorageObject) isNull()))
        .thenReturn(mockStorageObjectsCopy);

    gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
             BUCKET_NAME, ImmutableList.of(dstObjectName));

    verify(mockBatchFactory).newBatchHelper(any(HttpRequestInitializer.class),
        eq(mockStorage), any(Long.class));
    verify(mockStorage).objects();
    verify(mockStorageObjects).copy(
        eq(BUCKET_NAME), eq(OBJECT_NAME), eq(BUCKET_NAME), eq(dstObjectName),
        (StorageObject) isNull());
    verify(mockBatchHelper).queue(
        eq(mockStorageObjectsCopy), Matchers.<JsonBatchCallback<StorageObject>>anyObject());

    verify(mockBatchHelper).flush();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.copy(4) where srcBucketName == dstBucketName.
   */
  @Test
  public void testCopyObjectsApiExceptionSameBucket()
      throws IOException {
    String dstObjectName = OBJECT_NAME + "-copy";
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class),
        any(Storage.class), any(Long.class))).thenReturn(mockBatchHelper);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.copy(
        eq(BUCKET_NAME), eq(OBJECT_NAME), eq(BUCKET_NAME), eq(dstObjectName),
        (StorageObject) isNull()))
        .thenReturn(mockStorageObjectsCopy);
    final GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found exception");
    final GoogleJsonError unexpectedError = new GoogleJsonError();
    unexpectedError.setMessage("Other API exception");
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          callback.onFailure(notFoundError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          callback.onFailure(unexpectedError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).when(mockBatchHelper).queue(
        eq(mockStorageObjectsCopy), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    when(mockErrorExtractor.itemNotFound(eq(notFoundError)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedError)))
        .thenReturn(false);

    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               BUCKET_NAME, ImmutableList.of(dstObjectName));
      fail("Expected FileNotFoundException");
    } catch (FileNotFoundException e) {
      // Expected.
    } catch (Exception e) {
      // Make the test output a little more friendly in case the exception class differs.
      fail("Expected FileNotFoundException, got " + e.getClass().getName());
    }
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               BUCKET_NAME, ImmutableList.of(dstObjectName));
      fail("Expected IOException");
    } catch (IOException e) {
      // Expected.
    }

    verify(mockBatchFactory, times(2)).newBatchHelper(any(HttpRequestInitializer.class),
        eq(mockStorage), any(Long.class));
    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).copy(
        eq(BUCKET_NAME), eq(OBJECT_NAME), eq(BUCKET_NAME), eq(dstObjectName),
        (StorageObject) isNull());
    verify(mockBatchHelper, times(2)).queue(
        eq(mockStorageObjectsCopy), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(GoogleJsonError.class));
    verify(mockBatchHelper, times(2)).flush();
  }

  /**
   * Test successful operation of GoogleCloudStorage.copy(4) where srcBucketName != dstBucketName.
   */
  @Test
  public void testCopyObjectsNormalOperationDifferentBucket()
      throws IOException {
    String dstObjectName = OBJECT_NAME + "-copy";
    String dstBucketName = BUCKET_NAME + "-copy";
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class),
        any(Storage.class), any(Long.class))).thenReturn(mockBatchHelper);
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    when(mockStorageBuckets.get(eq(dstBucketName))).thenReturn(mockStorageBucketsGet2);
    when(mockStorageBucketsGet.execute())
        .thenReturn(new Bucket()
            .setName(BUCKET_NAME)
            .setTimeCreated(new DateTime(1111L))
            .setLocation("us-west-123")
            .setStorageClass("class-af4"));
    when(mockStorageBucketsGet2.execute())
        .thenReturn(new Bucket()
            .setName(dstBucketName)
            .setTimeCreated(new DateTime(2222L))
            .setLocation("us-west-123")
            .setStorageClass("class-af4"));
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.copy(
        eq(BUCKET_NAME), eq(OBJECT_NAME), eq(dstBucketName), eq(dstObjectName),
        (StorageObject) isNull()))
        .thenReturn(mockStorageObjectsCopy);

    gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
             dstBucketName, ImmutableList.of(dstObjectName));

    verify(mockStorage, times(2)).buckets();
    verify(mockStorageBuckets, times(2)).get(any(String.class));
    verify(mockStorageBucketsGet).execute();
    verify(mockStorageBucketsGet2).execute();
    verify(mockBatchFactory).newBatchHelper(any(HttpRequestInitializer.class), eq(mockStorage),
        any(Long.class));
    verify(mockStorage).objects();
    verify(mockStorageObjects).copy(
        eq(BUCKET_NAME), eq(OBJECT_NAME), eq(dstBucketName), eq(dstObjectName),
        (StorageObject) isNull());
    verify(mockBatchHelper).queue(
        eq(mockStorageObjectsCopy), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    verify(mockBatchHelper).flush();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.copy(4) where srcBucketName != dstBucketName.
   */
  @Test
  public void testCopyObjectsApiExceptionDifferentBucket()
      throws IOException {
    String dstObjectName = OBJECT_NAME + "-copy";
    String dstBucketName = BUCKET_NAME + "-copy";
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);

    // mockStorageBucketsGet corresponds to fetches of the srcBucket, and mockStorageBucketsGet2
    // corresponds to fetches of the dstBucket.
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    when(mockStorageBuckets.get(eq(dstBucketName))).thenReturn(mockStorageBucketsGet2);
    IOException notFoundException = new IOException("Fake not-found exception");
    IOException unexpectedException = new IOException("Other API exception");
    IOException wrappedUnexpectedException1 = GoogleCloudStorageExceptions.wrapException(
        unexpectedException, "Error accessing", BUCKET_NAME, null);
    IOException wrappedUnexpectedException2 = GoogleCloudStorageExceptions.wrapException(
        unexpectedException, "Error accessing", dstBucketName, null);
    Bucket returnedBucket = new Bucket()
        .setName(BUCKET_NAME)
        .setTimeCreated(new DateTime(1111L))
        .setLocation("us-west-123")
        .setStorageClass("class-af4");
    when(mockStorageBucketsGet.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException)
        .thenReturn(returnedBucket)
        .thenReturn(returnedBucket);
    when(mockStorageBucketsGet2.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);

    // Order of exceptions:
    // 1. Src 404
    // 2. Src unexpected error
    // 3. Dst 404
    // 4. Dst unexpected error
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               dstBucketName, ImmutableList.of(dstObjectName));
      fail("Expected FileNotFoundException");
    } catch (FileNotFoundException e) {
      // Expected.
      assertTrue(e.getMessage().indexOf(BUCKET_NAME) >= 0);
    } catch (Exception e) {
      // Make the test output a little more friendly in case the exception class differs.
      fail("Expected FileNotFoundException, got " + e.getClass().getName());
    }
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               dstBucketName, ImmutableList.of(dstObjectName));
      fail("Expected unexpectedException");
    } catch (IOException e) {
      assertEquals(wrappedUnexpectedException1.getMessage(), e.getMessage());
      assertEquals(unexpectedException, e.getCause());
    }
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               dstBucketName, ImmutableList.of(dstObjectName));
      fail("Expected FileNotFoundException");
    } catch (FileNotFoundException e) {
      // Expected.
      assertTrue(e.getMessage().indexOf(dstBucketName) >= 0);
    } catch (Exception e) {
      // Make the test output a little more friendly in case the exception class differs.
      e.printStackTrace();
      fail("Expected FileNotFoundException, got " + e.getClass().getName());
    }
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               dstBucketName, ImmutableList.of(dstObjectName));
      fail("Expected unexpectedException");
    } catch (IOException e) {
      assertEquals(wrappedUnexpectedException2.getMessage(), e.getMessage());
      assertEquals(unexpectedException, e.getCause());
    }

    verify(mockStorage, times(6)).buckets();
    verify(mockStorageBuckets, times(6)).get(any(String.class));
    verify(mockStorageBucketsGet, times(4)).execute();
    verify(mockStorageBucketsGet2, times(2)).execute();
    verify(mockErrorExtractor, times(4)).itemNotFound(any(IOException.class));
  }

  /**
   * Test behavior of GoogleCloudStorage.copy(4) where srcBucketName != dstBucketName and the
   * retrieved bucket metadata indicates they are not compatible for copying.
   */
  @Test
  public void testCopyObjectsIncompatibleBuckets()
      throws IOException {
    String dstObjectName = OBJECT_NAME + "-copy";
    String dstBucketName = BUCKET_NAME + "-copy";
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    when(mockStorageBuckets.get(eq(dstBucketName))).thenReturn(mockStorageBucketsGet2);
    when(mockStorageBucketsGet.execute())
        .thenReturn(new Bucket()
            .setName(BUCKET_NAME)
            .setTimeCreated(new DateTime(1111L))
            .setLocation("us-east")  // Incompatible location.
            .setStorageClass("class-af4"))
        .thenReturn(new Bucket()
            .setName(BUCKET_NAME)
            .setTimeCreated(new DateTime(1111L))
            .setLocation("us-west-123")
            .setStorageClass("class-be2"));  // Incompatible storage class.
    when(mockStorageBucketsGet2.execute())
        .thenReturn(new Bucket()
            .setName(dstBucketName)
            .setTimeCreated(new DateTime(2222L))
            .setLocation("us-west-123")
            .setStorageClass("class-af4"))
        .thenReturn(new Bucket()
            .setName(dstBucketName)
            .setTimeCreated(new DateTime(2222L))
            .setLocation("us-west-123")
            .setStorageClass("class-af4"));

    // Note: This is slightly fragile to check for getMessage() contents, but it's better to at
    // least do some checking that this is indeed the exception we intended to throw and now some
    // other generic IOException from unknown causes.
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               dstBucketName, ImmutableList.of(dstObjectName));
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().indexOf("not supported") >= 0
                 && e.getMessage().indexOf("storage location") >= 0);
    }
    try {
      gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME),
               dstBucketName, ImmutableList.of(dstObjectName));
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().indexOf("not supported") >= 0
                 && e.getMessage().indexOf("storage class") >= 0);
    }

    verify(mockStorage, times(4)).buckets();
    verify(mockStorageBuckets, times(4)).get(any(String.class));
    verify(mockStorageBucketsGet, times(2)).execute();
    verify(mockStorageBucketsGet2, times(2)).execute();
  }

  /**
   * Test for GoogleCloudStorage.listBucketNames(0).
   */
  @Test
  public void testListBucketNames()
      throws IOException {
    setupMocksForListBuckets();

    List<String> bucketNames = gcs.listBucketNames();
    assertEquals(3, bucketNames.size());
    assertEquals("bucket0", bucketNames.get(0));
    assertEquals("bucket1", bucketNames.get(1));
    assertEquals("bucket2", bucketNames.get(2));

    verifyMocksForListBuckets();
  }

  /**
   * Test for GoogleCloudStorage.listBucketInfo(0).
   */
  @Test
  public void testListBucketInfo()
      throws IOException {
    setupMocksForListBuckets();

    List<GoogleCloudStorageItemInfo> bucketInfos = gcs.listBucketInfo();
    assertEquals(3, bucketInfos.size());
    assertEquals("bucket0", bucketInfos.get(0).getBucketName());
    assertEquals("bucket1", bucketInfos.get(1).getBucketName());
    assertEquals("bucket2", bucketInfos.get(2).getBucketName());

    verifyMocksForListBuckets();
  }

  /**
   * Sets up mocks for a list-bucket operation.
   */
  private void setupMocksForListBuckets()
      throws IOException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.list(eq(PROJECT_ID))).thenReturn(mockStorageBucketsList);
    when(mockStorageBucketsList.execute())
        .thenReturn(new Buckets()
            .setItems(ImmutableList.of(
                createTestBucket(0),
                createTestBucket(1)))
            .setNextPageToken("token0"))
        .thenReturn(new Buckets()
            .setItems(ImmutableList.of(
                createTestBucket(2)))
            .setNextPageToken(null));
  }

  /**
   * Verifies mock operations for a list-bucket operation.
   */
  private void verifyMocksForListBuckets()
      throws IOException {
    verify(mockStorage).buckets();
    verify(mockStorageBuckets).list(eq(PROJECT_ID));
    verify(mockStorageBucketsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageBucketsList).setPageToken("token0");
    verify(mockStorageBucketsList, times(2)).execute();
  }

  /**
   * Creates a test bucket based on the given number.
   */
  private Bucket createTestBucket(int bucketNumber) {
    Bucket b = new Bucket();
    String bucketNumberStr = Integer.toString(bucketNumber);
    b.setName("bucket" + bucketNumberStr);
    b.setTimeCreated(new DateTime(new Date()));
    b.setLocation("location");
    b.setStorageClass("DRA");
    return b;
  }

  /**
   * Test successful operation of GoogleCloudStorage.listObjectNames(3).
   */
  @Test
  public void testListObjectNamesPrefix()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(eq(BUCKET_NAME)))
        .thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.execute())
        .thenReturn(new Objects()
            .setPrefixes(ImmutableList.of(
                "foo/bar/baz/dir0/",
                "foo/bar/baz/dir1/"))
            .setNextPageToken("token0"))
        .thenReturn(new Objects()
            .setItems(ImmutableList.of(
                new StorageObject().setName("foo/bar/baz/"),
                new StorageObject().setName("foo/bar/baz/obj0"),
                new StorageObject().setName("foo/bar/baz/obj1")))
            .setNextPageToken(null));

    List<String> objectNames = gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter);
    assertEquals(4, objectNames.size());
    assertEquals("foo/bar/baz/dir0/", objectNames.get(0));
    assertEquals("foo/bar/baz/dir1/", objectNames.get(1));
    assertEquals("foo/bar/baz/obj0", objectNames.get(2));
    assertEquals("foo/bar/baz/obj1", objectNames.get(3));

    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).setPageToken("token0");
    verify(mockStorageObjectsList, times(2)).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.listObjectNames(3).
   */
  @Test
  public void testListObjectNamesPrefixApiException()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(eq(BUCKET_NAME)))
        .thenReturn(mockStorageObjectsList);

    IOException notFoundException = new IOException("Fake not-found exception");
    IOException unexpectedException = new IOException("Other API exception");
    when(mockStorageObjectsList.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);

    // First time should just return empty list.
    List<String> objectNames = gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter);
    assertTrue(objectNames.isEmpty());

    // Second time throws.
    try {
      gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter);
      fail("Expected IOException");
    } catch (IOException e) {
      // Expected.
    }

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList, times(2)).setMaxResults(
        eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList, times(2)).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList, times(2)).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList, times(2)).execute();
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
  }

  @Test
  public void testListObjectInfoBasic()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(eq(BUCKET_NAME)))
        .thenReturn(mockStorageObjectsList);
    List<StorageObject> fakeObjectList = ImmutableList.of(
        new StorageObject()
            .setName("foo/bar/baz/")
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L),
        new StorageObject()
            .setName("foo/bar/baz/obj0")
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L),
        new StorageObject()
            .setName("foo/bar/baz/obj1")
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(33L))
            .setSize(BigInteger.valueOf(333L))
            .setGeneration(3L)
            .setMetageneration(3L));
    when(mockStorageObjectsList.execute())
        .thenReturn(new Objects()
            .setItems(fakeObjectList)
            .setNextPageToken(null));

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    // The item exactly matching the input objectPrefix will be discarded.
    assertEquals(2, objectInfos.size());
    assertEquals(fakeObjectList.get(1).getName(), objectInfos.get(0).getObjectName());
    assertEquals(fakeObjectList.get(1).getUpdated().getValue(),
                 objectInfos.get(0).getCreationTime());
    assertEquals(fakeObjectList.get(1).getSize().longValue(),
                 objectInfos.get(0).getSize());
    assertEquals(fakeObjectList.get(2).getName(), objectInfos.get(1).getObjectName());
    assertEquals(fakeObjectList.get(2).getUpdated().getValue(),
                 objectInfos.get(1).getCreationTime());
    assertEquals(fakeObjectList.get(2).getSize().longValue(),
                 objectInfos.get(1).getSize());

    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).execute();
  }

  @Test
  public void testListObjectInfoReturnPrefixes()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(eq(BUCKET_NAME)))
        .thenReturn(mockStorageObjectsList);
    final List<StorageObject> fakeObjectList = ImmutableList.of(
        new StorageObject()
            .setName("foo/bar/baz/dir0/")
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L),
        new StorageObject()
            .setName("foo/bar/baz/dir1/")
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L));
    when(mockStorageObjectsList.execute())
        .thenReturn(new Objects()
            .setPrefixes(ImmutableList.of(
                "foo/bar/baz/dir0/",
                "foo/bar/baz/dir1/"))
            .setNextPageToken(null));
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class),
        any(Storage.class), any(Long.class))).thenReturn(mockBatchHelper);
    when(mockStorageObjects.get(eq(BUCKET_NAME), any(String.class)))
        .thenReturn(mockStorageObjectsGet);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          callback.onSuccess(fakeObjectList.get(0), new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    })
    .doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          callback.onSuccess(fakeObjectList.get(1), new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    })
    .when(mockBatchHelper).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    assertEquals(2, objectInfos.size());
    assertEquals(fakeObjectList.get(0).getName(), objectInfos.get(0).getObjectName());
    assertEquals(fakeObjectList.get(0).getUpdated().getValue(),
                 objectInfos.get(0).getCreationTime());
    assertEquals(fakeObjectList.get(0).getSize().longValue(),
                 objectInfos.get(0).getSize());
    assertEquals(fakeObjectList.get(1).getName(), objectInfos.get(1).getObjectName());
    assertEquals(fakeObjectList.get(1).getUpdated().getValue(),
                 objectInfos.get(1).getCreationTime());
    assertEquals(fakeObjectList.get(1).getSize().longValue(),
                 objectInfos.get(1).getSize());

    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).execute();
    verify(mockBatchFactory).newBatchHelper(any(HttpRequestInitializer.class), eq(mockStorage),
        any(Long.class));
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), any(String.class));
    verify(mockBatchHelper, times(2)).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    verify(mockBatchHelper).flush();
  }

  @Test
  public void testListObjectInfoReturnPrefixesNotFound()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";

    // Set up the initial list to return three prefixes, two of which don't exist.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(any(String.class)))
        .thenReturn(mockStorageObjectsList);
    final List<StorageObject> fakeObjectList = ImmutableList.of(
        new StorageObject()
            .setName("foo/bar/baz/dir0/")
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L),
        new StorageObject()
            .setName("foo/bar/baz/dir1/")
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L),
        new StorageObject()
            .setName("foo/bar/baz/dir2/")
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(33L))
            .setSize(BigInteger.valueOf(333L))
            .setGeneration(3L)
            .setMetageneration(3L));
    when(mockStorageObjectsList.execute())
        .thenReturn(new Objects()
            .setPrefixes(ImmutableList.of(
                "foo/bar/baz/dir0/",
                "foo/bar/baz/dir1/",
                "foo/bar/baz/dir2/"))
            .setNextPageToken(null));

    // Set up the follow-up getItemInfos to just return a batch with "not found".
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class), any(Storage.class),
        any(Long.class))).thenReturn(mockBatchHelper);
    when(mockStorageObjects.get(any(String.class), any(String.class)))
        .thenReturn(mockStorageObjectsGet);
    final GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found exception");
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          // First time for object 0, return "not found".
          callback.onFailure(notFoundError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          // First time for object 1, return it successfully.
          callback.onSuccess(fakeObjectList.get(1), new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          // First time for object 2, return "not found".
          callback.onFailure(notFoundError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          // Second time for object 0, return it successfully.
          callback.onSuccess(fakeObjectList.get(0), new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          // Second time for object 2, return it successfully.
          callback.onSuccess(fakeObjectList.get(2), new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    })
    .when(mockBatchHelper).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    when(mockErrorExtractor.itemNotFound(eq(notFoundError)))
        .thenReturn(true);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        Runnable callback = (Runnable) args[0];
        callback.run();
        return null;
      }
    })
    .when(mockExecutorService).execute(any(Runnable.class));


    // Set up the "create" for auto-repair after a failed getItemInfos.
    when(mockStorageObjects.insert(
        any(String.class), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    // objects().list, objects().get x 3, objects().insert x 2, objects().get x 2
    verify(mockStorage, times(8)).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).execute();

    // Original batch get.
    verify(mockBatchFactory, times(2)).newBatchHelper(any(HttpRequestInitializer.class),
        eq(mockStorage), any(Long.class));
    verify(mockStorageObjects, times(5)).get(eq(BUCKET_NAME), any(String.class));
    verify(mockBatchHelper, times(5)).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    verify(mockBatchHelper, times(2)).flush();
    verify(mockErrorExtractor, times(2)).itemNotFound(any(GoogleJsonError.class));

    // Auto-repair insert.
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper, times(2))
        .setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert, times(2)).execute();

    // Check logical contents after all the "verify" calls, otherwise the mock verifications won't
    // be executed and we'll have misleading "NoInteractionsWanted" errors.
    assertEquals(fakeObjectList.size(), objectInfos.size());

    Map<String, GoogleCloudStorageItemInfo> itemLookup = new HashMap<>();
    for (GoogleCloudStorageItemInfo item : objectInfos) {
      itemLookup.put(item.getObjectName(), item);
    }
    for (StorageObject fakeObject : fakeObjectList) {
      GoogleCloudStorageItemInfo listedInfo = itemLookup.get(fakeObject.getName());
      assertEquals(fakeObject.getName(), listedInfo.getObjectName());
      assertEquals(fakeObject.getUpdated().getValue(), listedInfo.getCreationTime());
      assertEquals(fakeObject.getSize().longValue(), listedInfo.getSize());
    }
  }

  /**
   * Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent ROOT.
   */
  @Test
  public void testGetItemInfoRoot()
      throws IOException {
    GoogleCloudStorageItemInfo info = gcs.getItemInfo(StorageResourceId.ROOT);
    assertEquals(GoogleCloudStorageItemInfo.ROOT_INFO, info);
  }

  /**
   * Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent only a bucket.
   */
  @Test
  public void testGetItemInfoBucket()
      throws IOException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    when(mockStorageBucketsGet.execute())
        .thenReturn(new Bucket()
            .setName(BUCKET_NAME)
            .setTimeCreated(new DateTime(1234L))
            .setLocation("us-west-123")
            .setStorageClass("class-af4"));
    GoogleCloudStorageItemInfo info = gcs.getItemInfo(new StorageResourceId(BUCKET_NAME));
    GoogleCloudStorageItemInfo expected = new GoogleCloudStorageItemInfo(
        new StorageResourceId(BUCKET_NAME), 1234L, 0L, "us-west-123", "class-af4");
    assertEquals(expected, info);

    verify(mockStorage).buckets();
    verify(mockStorageBuckets).get(eq(BUCKET_NAME));
    verify(mockStorageBucketsGet).execute();
  }

  /**
   * Test handling of mismatch in Bucket.getName() vs StorageResourceId.getBucketName().
   */
  @Test
  public void testGetItemInfoBucketReturnMismatchedName()
      throws IOException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    when(mockStorageBucketsGet.execute())
        .thenReturn(new Bucket()
            .setName("wrong-bucket-name")
            .setTimeCreated(new DateTime(1234L))
            .setLocation("us-west-123")
            .setStorageClass("class-af4"));
    try {
      gcs.getItemInfo(new StorageResourceId(BUCKET_NAME));
      fail("Expected IllegalArgumentException with a wrong-bucket-name");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    verify(mockStorage).buckets();
    verify(mockStorageBuckets).get(eq(BUCKET_NAME));
    verify(mockStorageBucketsGet).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.getItemInfo(2) when arguments represent only a bucket.
   */
  @Test
  public void testGetItemInfoBucketApiException()
      throws IOException {
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    IOException notFoundException = new IOException("Fake not-found exception");
    IOException unexpectedException = new IOException("Other API exception");
    when(mockStorageBucketsGet.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);

    // Not found.
    GoogleCloudStorageItemInfo info = gcs.getItemInfo(new StorageResourceId(BUCKET_NAME));
    GoogleCloudStorageItemInfo expected = new GoogleCloudStorageItemInfo(
        new StorageResourceId(BUCKET_NAME), 0L, -1L, null, null);
    assertEquals(expected, info);

    // Throw.
    try {
      gcs.getItemInfo(new StorageResourceId(BUCKET_NAME));
      fail("Exception IOException");
    } catch (IOException e) {
      // Expected.
    }

    verify(mockStorage, times(2)).buckets();
    verify(mockStorageBuckets, times(2)).get(eq(BUCKET_NAME));
    verify(mockStorageBucketsGet, times(2)).execute();
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
  }

  /**
   * Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent an object in
   * a bucket.
   */
  @Test
  public void testGetItemInfoObject()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(1234L))
            .setSize(BigInteger.valueOf(42L))
            .setGeneration(1L)
            .setMetageneration(1L));
    GoogleCloudStorageItemInfo info =
        gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    GoogleCloudStorageItemInfo expected = new GoogleCloudStorageItemInfo(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        1234L,
        42L,
        null,
        null,
        EMPTY_METADATA,
        1L,
        1L);
    assertEquals(expected, info);

    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  /**
   * Test handling of mismatch in StorageObject.getBucket() and StorageObject.getName() vs
   * respective values in the queried StorageResourceId.
   */
  @Test
  public void testGetItemInfoObjectReturnMismatchedName()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(new StorageObject()
            .setBucket("wrong-bucket-name")
            .setName(OBJECT_NAME)
            .setUpdated(new DateTime(1234L))
            .setSize(BigInteger.valueOf(42L))
            .setGeneration(1L)
            .setMetageneration(1L))
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName("wrong-object-name")
            .setUpdated(new DateTime(1234L))
            .setSize(BigInteger.valueOf(42L))
            .setGeneration(1L)
            .setMetageneration(1L));

    try {
      gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
      fail("Expected IllegalArgumentException with a wrong-bucket-name");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
    try {
      gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
      fail("Expected IllegalArgumentException with a wrong-object-name");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(2)).execute();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent an object in a
   * bucket.
   */
  @Test
  public void testGetItemInfoObjectApiException()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    IOException notFoundException = new IOException("Fake not-found exception");
    IOException unexpectedException = new IOException("Other API exception");
    when(mockStorageObjectsGet.execute())
        .thenThrow(notFoundException)
        .thenThrow(unexpectedException);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException)))
        .thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(unexpectedException)))
        .thenReturn(false);

    // Not found.
    GoogleCloudStorageItemInfo info =
        gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    GoogleCloudStorageItemInfo expected = new GoogleCloudStorageItemInfo(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        0L,
        -1L,
        null,
        null,
        EMPTY_METADATA,
        0 /* Content Generation */,
        0 /* Meta generation */);
    assertEquals(expected, info);

    // Throw.
    try {
      gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
      fail("Exception IOException");
    } catch (IOException e) {
      // Expected.
    }

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockErrorExtractor, times(2)).itemNotFound(any(IOException.class));
  }

  @Test
  public void testGetItemInfos()
      throws IOException {
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class),
        any(Storage.class), any(Long.class))).thenReturn(mockBatchHelper);

    // Set up the return for the Bucket fetch.
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<Bucket> callback = (JsonBatchCallback<Bucket>) args[1];
        try {
          callback.onSuccess(
              new Bucket()
                  .setName(BUCKET_NAME)
                  .setTimeCreated(new DateTime(1234L))
                  .setLocation("us-west-123")
                  .setStorageClass("class-af4"),
              new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).when(mockBatchHelper).queue(
        eq(mockStorageBucketsGet), Matchers.<JsonBatchCallback<Bucket>>anyObject());

    // Set up the return for the StorageObject fetch.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          callback.onSuccess(
              new StorageObject()
                  .setBucket(BUCKET_NAME)
                  .setName(OBJECT_NAME)
                  .setUpdated(new DateTime(1234L))
                  .setSize(BigInteger.valueOf(42L))
                  .setGeneration(1L)
                  .setMetageneration(1L),
              new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).when(mockBatchHelper).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());

    // Call in order of StorageObject, ROOT, Bucket.
    List<GoogleCloudStorageItemInfo> itemInfos = gcs.getItemInfos(ImmutableList.of(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        StorageResourceId.ROOT,
        new StorageResourceId(BUCKET_NAME)));

    GoogleCloudStorageItemInfo expectedObject = new GoogleCloudStorageItemInfo(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        1234L,
        42L,
        null,
        null,
        EMPTY_METADATA,
        1 /* Content Generation */,
        1 /* Meta generation */);
    GoogleCloudStorageItemInfo expectedRoot = GoogleCloudStorageItemInfo.ROOT_INFO;
    GoogleCloudStorageItemInfo expectedBucket = new GoogleCloudStorageItemInfo(
        new StorageResourceId(BUCKET_NAME), 1234L, 0L, "us-west-123", "class-af4");

    assertEquals(expectedObject, itemInfos.get(0));
    assertEquals(expectedRoot, itemInfos.get(1));
    assertEquals(expectedBucket, itemInfos.get(2));

    verify(mockBatchFactory).newBatchHelper(any(HttpRequestInitializer.class),
        eq(mockStorage), any(Long.class));
    verify(mockStorage).buckets();
    verify(mockStorageBuckets).get(eq(BUCKET_NAME));
    verify(mockBatchHelper).queue(
        eq(mockStorageBucketsGet), Matchers.<JsonBatchCallback<Bucket>>anyObject());
    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockBatchHelper).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    verify(mockBatchHelper).flush();
  }

  @Test
  public void testGetItemInfosNotFound()
      throws IOException {
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class),
        any(Storage.class), any(Long.class))).thenReturn(mockBatchHelper);

    // Set up the return for the Bucket fetch.
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    final GoogleJsonError notFoundError = new GoogleJsonError();
    notFoundError.setMessage("Fake not-found error");
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<Bucket> callback = (JsonBatchCallback<Bucket>) args[1];
        try {
          callback.onFailure(notFoundError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).when(mockBatchHelper).queue(
        eq(mockStorageBucketsGet), Matchers.<JsonBatchCallback<Bucket>>anyObject());

    // Set up the return for the StorageObject fetch.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          callback.onFailure(notFoundError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).when(mockBatchHelper).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());

    // We will claim both GoogleJsonErrors are "not found" errors.
    when(mockErrorExtractor.itemNotFound(eq(notFoundError)))
        .thenReturn(true);

    // Call in order of StorageObject, ROOT, Bucket.
    List<GoogleCloudStorageItemInfo> itemInfos = gcs.getItemInfos(ImmutableList.of(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        StorageResourceId.ROOT,
        new StorageResourceId(BUCKET_NAME)));

    GoogleCloudStorageItemInfo expectedObject = GoogleCloudStorageImpl.createItemInfoForNotFound(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    GoogleCloudStorageItemInfo expectedRoot = GoogleCloudStorageItemInfo.ROOT_INFO;
    GoogleCloudStorageItemInfo expectedBucket = GoogleCloudStorageImpl.createItemInfoForNotFound(
        new StorageResourceId(BUCKET_NAME));

    assertEquals(expectedObject, itemInfos.get(0));
    assertEquals(expectedRoot, itemInfos.get(1));
    assertEquals(expectedBucket, itemInfos.get(2));

    verify(mockBatchFactory).newBatchHelper(any(HttpRequestInitializer.class), eq(mockStorage),
        any(Long.class));
    verify(mockStorage).buckets();
    verify(mockStorageBuckets).get(eq(BUCKET_NAME));
    verify(mockBatchHelper).queue(
        eq(mockStorageBucketsGet), Matchers.<JsonBatchCallback<Bucket>>anyObject());
    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockBatchHelper).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(GoogleJsonError.class));
    verify(mockBatchHelper).flush();
  }

  @Test
  public void testGetItemInfosApiException()
      throws IOException {
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class), any(Storage.class),
        any(Long.class))).thenReturn(mockBatchHelper);

    // Set up the return for the Bucket fetch.
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.get(eq(BUCKET_NAME))).thenReturn(mockStorageBucketsGet);
    final GoogleJsonError unexpectedError = new GoogleJsonError();
    unexpectedError.setMessage("Unexpected API exception ");
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<Bucket> callback = (JsonBatchCallback<Bucket>) args[1];
        try {
          callback.onFailure(unexpectedError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).when(mockBatchHelper).queue(
        eq(mockStorageBucketsGet), Matchers.<JsonBatchCallback<Bucket>>anyObject());

    // Set up the return for the StorageObject fetch.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        @SuppressWarnings("unchecked")
        JsonBatchCallback<StorageObject> callback = (JsonBatchCallback<StorageObject>) args[1];
        try {
          callback.onFailure(unexpectedError, new HttpHeaders());
        } catch (IOException ioe) {
          fail(ioe.toString());
        }
        return null;
      }
    }).when(mockBatchHelper).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());

    // We will claim both GoogleJsonErrors are unexpected errors.
    when(mockErrorExtractor.itemNotFound(eq(unexpectedError)))
        .thenReturn(false);

    // Call in order of StorageObject, ROOT, Bucket.
    try {
      List<GoogleCloudStorageItemInfo> itemInfos = gcs.getItemInfos(ImmutableList.of(
          new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
          StorageResourceId.ROOT,
          new StorageResourceId(BUCKET_NAME)));
      fail(String.format("Expected IOException to get thrown, instead returned: %s", itemInfos));
    } catch (IOException ioe) {
      // Expected composite exception.
      assertNotNull(ioe.getSuppressed());
      assertEquals(2, ioe.getSuppressed().length);
    }

    // All invocations still should have been attempted; the exception should have been thrown
    // at the very end.
    verify(mockBatchFactory).newBatchHelper(any(HttpRequestInitializer.class), eq(mockStorage),
        any(Long.class));
    verify(mockStorage).buckets();
    verify(mockStorageBuckets).get(eq(BUCKET_NAME));
    verify(mockBatchHelper).queue(
        eq(mockStorageBucketsGet), Matchers.<JsonBatchCallback<Bucket>>anyObject());
    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockBatchHelper).queue(
        eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    verify(mockErrorExtractor, times(2)).itemNotFound(any(GoogleJsonError.class));
    verify(mockBatchHelper).flush();
  }

  /**
   * Test for GoogleCloudStorage.close(0).
   */
  @Test
  public void testClose() {
    gcs.close();
    verify(mockExecutorService).shutdown();
  }

  /**
   * Test for argument sanitization in GoogleCloudStorage.waitForBucketEmpty(1).
   */
  @Test
  public void testWaitForBucketEmptyIllegalArguments()
      throws IOException {
    try {
      gcs.waitForBucketEmpty(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
    try {
      gcs.waitForBucketEmpty("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
  }

  /**
   * Test for successful GoogleCloudStorage.waitForBucketEmpty(1) including a sleep/retry.
   */
  @Test
  public void testWaitForBucketEmptySuccess()
      throws IOException, InterruptedException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(eq(BUCKET_NAME)))
        .thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.execute())
        .thenReturn(new Objects()
            .setPrefixes(ImmutableList.of("foo"))
            .setItems(ImmutableList.<StorageObject>of()))
        .thenReturn(new Objects()
            .setPrefixes(ImmutableList.<String>of())
            .setItems(ImmutableList.<StorageObject>of()));

    gcs.waitForBucketEmpty(BUCKET_NAME);

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList, times(2))
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList, times(2)).setDelimiter(eq(GoogleCloudStorage.PATH_DELIMITER));
    verify(mockStorageObjectsList, times(2)).execute();
    verify(mockSleeper, times(1)).sleep(
        eq((long) GoogleCloudStorageImpl.BUCKET_EMPTY_WAIT_TIME_MS));
  }

  /**
   * Test for failed GoogleCloudStorage.waitForBucketEmpty(1) after exhausting allowable retries.
   */
  @Test
  public void testWaitForBucketEmptyFailure()
      throws IOException, InterruptedException {
    try {
      gcs.waitForBucketEmpty(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
    try {
      gcs.waitForBucketEmpty("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    when(mockStorageObjects.list(eq(BUCKET_NAME))).thenReturn(mockStorageObjectsList);

    OngoingStubbing<Objects> stub = when(mockStorageObjectsList.execute());
    for (int i = 0; i < GoogleCloudStorageImpl.BUCKET_EMPTY_MAX_RETRIES; i++) {
      stub = stub.thenReturn(new Objects()
          .setPrefixes(ImmutableList.of("foo"))
          .setItems(ImmutableList.<StorageObject>of()));
    }

    try {
      gcs.waitForBucketEmpty(BUCKET_NAME);
      fail("Expected IOException");
    } catch (IOException e) {
      assertTrue(e.getMessage().indexOf("not empty") >= 0);
    }

    VerificationMode retryTimes = times(GoogleCloudStorageImpl.BUCKET_EMPTY_MAX_RETRIES);
    verify(mockStorage, retryTimes).objects();
    verify(mockStorageObjects, retryTimes).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList, retryTimes)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList, retryTimes).setDelimiter(eq(GoogleCloudStorage.PATH_DELIMITER));
    verify(mockStorageObjectsList, retryTimes).execute();
    verify(mockSleeper, retryTimes).sleep(
        eq((long) GoogleCloudStorageImpl.BUCKET_EMPTY_WAIT_TIME_MS));
  }

  /**
   * Test validation of Storage passing constructor.
   */
  @Test
  public void testStoragePassedConstructor() {
    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleCloudStorageOptions.newBuilder();

    optionsBuilder
        .setAppName("appName")
        .setProjectId("projectId");

    // Verify that fake projectId/appName and mock storage does not throw.
    new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);

    // Verify that projectId == null or empty throws IllegalArgumentException.
    optionsBuilder.setProjectId(null);
    try {
      new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    optionsBuilder.setProjectId("");
    try {
      new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    optionsBuilder.setProjectId("projectId");

    // Verify that appName == null or empty throws IllegalArgumentException.

    optionsBuilder.setAppName(null);
    try {
      new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    optionsBuilder.setAppName("");
    try {
      new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }

    optionsBuilder.setAppName("appName");

    // Verify that gcs == null throws IllegalArgumentException.
    try {
      new GoogleCloudStorageImpl(optionsBuilder.build(), (Storage) null);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
  }

  /**
   * Provides coverage for default constructor. No real validation is performed.
   */
  @Test
  public void testCoverDefaultConstructor() {
    new GoogleCloudStorageImpl();
  }
}
