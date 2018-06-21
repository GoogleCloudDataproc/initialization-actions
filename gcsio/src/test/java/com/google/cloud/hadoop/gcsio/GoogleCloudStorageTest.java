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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.createItemInfoForStorageObject;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo.createInferredDirectory;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
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
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
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
import com.google.api.services.storage.model.Buckets;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.BackOffFactory;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.SSLException;
import org.junit.After;
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
  private static final ImmutableMap<String, byte[]> EMPTY_METADATA = ImmutableMap.of();

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
    executorService = Executors.newCachedThreadPool();
    gcs = createTestInstance();
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl
   * as the concrete type and setting up the proper mocks.
   */
  protected GoogleCloudStorageOptions.Builder
      createDefaultCloudStorageOptionsBuilder() {
    return GoogleCloudStorageOptions.newBuilder()
        .setAppName(APP_NAME)
        .setProjectId(PROJECT_ID)
        .setCreateMarkerObjects(true);
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl
   * as the concrete type and setting up the proper mocks.
   */
  protected GoogleCloudStorage createTestInstance() {
    GoogleCloudStorageOptions.Builder optionsBuilder =
        createDefaultCloudStorageOptionsBuilder();
    return createTestInstance(optionsBuilder.build());
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl
   * as the concrete type and setting up the proper mocks,
   * with the specified value for autoRepairImplicitDirectories.
   */
  protected GoogleCloudStorage createTestInstance(
      boolean autoRepairImplicitDirectories) {
    GoogleCloudStorageOptions.Builder optionsBuilder =
        createDefaultCloudStorageOptionsBuilder();
    optionsBuilder
        .setAutoRepairImplicitDirectoriesEnabled(autoRepairImplicitDirectories);
    return createTestInstance(optionsBuilder.build());
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl
   * as the concrete type and setting up the proper mocks,
   * with autoRepairImplicitDirectories set false,
   * and the specified value for inferImplicitDirectories.
   */
  private GoogleCloudStorage createTestInstanceWithInferImplicit(
      boolean inferImplicitDirectories) {
    GoogleCloudStorageOptions.Builder optionsBuilder =
        createDefaultCloudStorageOptionsBuilder();
    optionsBuilder
        .setAutoRepairImplicitDirectoriesEnabled(false)
        .setInferImplicitDirectoriesEnabled(inferImplicitDirectories);
    return createTestInstance(optionsBuilder.build());
  }

  /**
   * Creates an instance of GoogleCloudStorage using GoogleCloudStorageImpl
   * as the concrete type and setting up the proper mocks,
   * with the specified autoRepairImplicitDirectories and
   * inferImplicitDirectories.
   */
  private GoogleCloudStorage createTestInstanceWithAutoRepairWithInferImplicit(
      boolean autoRepairImplicitDirectories, boolean inferImplicitDirectories) {
    GoogleCloudStorageOptions.Builder optionsBuilder =
        createDefaultCloudStorageOptionsBuilder();
    optionsBuilder
        .setAutoRepairImplicitDirectoriesEnabled(autoRepairImplicitDirectories)
        .setInferImplicitDirectoriesEnabled(inferImplicitDirectories);
    return createTestInstance(optionsBuilder.build());
  }

  /**
   * Creates an instance of GoogleCloudStorage with the specified options,
   * using GoogleCloudStorageImpl as the concrete type,
   * and setting up the proper mocks.
   */
  protected GoogleCloudStorage createTestInstance(
      GoogleCloudStorageOptions options) {
    GoogleCloudStorageImpl gcsTestInstance =
        new GoogleCloudStorageImpl(options, mockStorage);
    gcsTestInstance.setThreadPool(executorService);
    gcsTestInstance.setErrorExtractor(mockErrorExtractor);
    gcsTestInstance.setClientRequestHelper(mockClientRequestHelper);
    gcsTestInstance.setBatchFactory(mockBatchFactory);
    gcsTestInstance.setSleeper(mockSleeper);
    gcsTestInstance.setBackOffFactory(mockBackOffFactory);
    return gcsTestInstance;
  }

  protected void setupNonConflictedWrite(final Throwable t) throws IOException {
    setupNonConflictedWrite(new Answer<StorageObject>() {
      @Override
      public StorageObject answer(InvocationOnMock invocation) throws Throwable {
        throw t;
      }
    });
  }

  protected void setupNonConflictedWrite(Answer<StorageObject> answer)
      throws IOException {
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorageObjects.get(BUCKET_NAME, OBJECT_NAME)).thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute()).thenThrow(new IOException("NotFound"));
    when(mockErrorExtractor.itemNotFound(any(IOException.class))).thenReturn(true);
    when(mockStorageObjectsInsert.execute()).thenReturn(
        new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setGeneration(1L)
            .setMetageneration(1L)).thenAnswer(answer);
  }

  protected StorageObject getStorageObjectForEmptyObjectWithMetadata(
      Map<String, byte[]> metadata) {
    return new StorageObject()
        .setBucket(BUCKET_NAME)
        .setName(OBJECT_NAME)
        .setGeneration(1L)
        .setMetageneration(1L)
        .setSize(BigInteger.ZERO)
        .setUpdated(new DateTime(11L))
        .setMetadata(
            metadata == null ? null : GoogleCloudStorageImpl.encodeMetadata(metadata));
  }

  protected GoogleCloudStorageItemInfo getItemInfoForEmptyObjectWithMetadata(
      Map<String, byte[]> metadata) {
    return createItemInfoForStorageObject(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        getStorageObjectForEmptyObjectWithMetadata(metadata));
  }

  /**
   * Ensure that each test case precisely captures all interactions with the mocks, since all
   * the mocks represent external dependencies.
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
      assertThrows(
          IllegalArgumentException.class,
          () -> gcs.create(new StorageResourceId(objectPair[0], objectPair[1])));
    }
  }

  /**
   * Test successful operation of GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectNormalOperation()
      throws Exception {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    // Setup the argument captor so we can get back the data that we write.
    final ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    final CountDownLatch waitTillWritesAreDoneLatch = new CountDownLatch(1);
    final byte[] readData = new byte[testData.length];
    setupNonConflictedWrite(new Answer<StorageObject>() {
      @Override
      public StorageObject answer(InvocationOnMock unused) throws Throwable {
        synchronized (readData) {
          waitTillWritesAreDoneLatch.await();
          inputStreamCaptor.getValue().getInputStream().read(readData);
          return new StorageObject()
              .setBucket(BUCKET_NAME)
              .setName(OBJECT_NAME)
              .setUpdated(new DateTime(11L))
              .setSize(BigInteger.valueOf(5L))
              .setGeneration(12345L)
              .setMetageneration(1L);
        }
      }
    });
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsInsert)))
        .thenReturn(mockHeaders);

    // Get a channel which will execute the insert object.
    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    // Verify the initial setup of the insert; capture the input stream.
    ArgumentCaptor<StorageObject> storageObjectCaptor =
        ArgumentCaptor.forClass(StorageObject.class);
    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), storageObjectCaptor.capture(), inputStreamCaptor.capture());

    writeChannel.write(ByteBuffer.wrap(testData));

    // The writes are now done, we can return from the execute method.
    // There is an issue with how PipedInputStream functions since it checks
    // to see if the sending and receiving threads are alive.
    waitTillWritesAreDoneLatch.countDown();

    // Flush, make sure the data made it out the other end.
    writeChannel.close();

    assertThat(writeChannel.isOpen()).isFalse();
    verify(mockStorageObjects, times(1)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(1)).execute();
    verify(mockStorageObjectsInsert, times(1)).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(1L));
    assertThat(storageObjectCaptor.getValue().getName()).isEqualTo(OBJECT_NAME);
    verify(mockHeaders, times(1)).set(
        eq("X-Goog-Upload-Desired-Chunk-Granularity"),
        eq(AbstractGoogleAsyncWriteChannel.GCS_UPLOAD_GRANULARITY));
    verify(mockHeaders, times(0)).set(eq("X-Goog-Upload-Max-Raw-Size"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(AbstractGoogleClientRequest.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockBackOffFactory).newBackOff();
    verify(mockBackOff).nextBackOffMillis();
    verify(mockStorageObjectsInsert, times(2)).execute();
    synchronized (readData) {
      assertThat(readData).isEqualTo(testData);
    }

    // Closing the closed channel should have no effect.
    writeChannel.close();

    // On close(), it should have stashed away the completed ItemInfo based on the returned
    // StorageObject.
    GoogleCloudStorageItemInfo finishedInfo =
        ((GoogleCloudStorageItemInfo.Provider) writeChannel).getItemInfo();
    assertThat(finishedInfo.getBucketName()).isEqualTo(BUCKET_NAME);
    assertThat(finishedInfo.getObjectName()).isEqualTo(OBJECT_NAME);
    assertThat(finishedInfo.getContentGeneration()).isEqualTo(12345L);

    // Further writes to a closed channel should throw a ClosedChannelException.
    assertThrows(ClosedChannelException.class, () -> writeChannel.write(ByteBuffer.wrap(testData)));
  }

  /**
   * Test successful operation of GoogleCloudStorage.create(2) with generationId.
   */
  @Test
  public void testCreateObjectWithGenerationId()
      throws Exception {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    // Setup the argument captor so we can get back the data that we write.
    final ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    final CountDownLatch waitTillWritesAreDoneLatch = new CountDownLatch(1);
    final byte[] readData = new byte[testData.length];
    setupNonConflictedWrite(new Answer<StorageObject>() {
      @Override
      public StorageObject answer(InvocationOnMock unused) throws Throwable {
        synchronized (readData) {
          waitTillWritesAreDoneLatch.await();
          inputStreamCaptor.getValue().getInputStream().read(readData);
          return new StorageObject()
              .setBucket(BUCKET_NAME)
              .setName(OBJECT_NAME)
              .setUpdated(new DateTime(11L))
              .setSize(BigInteger.valueOf(5L))
              .setGeneration(12345L)
              .setMetageneration(1L);
        }
      }
    });
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsInsert)))
        .thenReturn(mockHeaders);

    // Get a channel which will execute the insert object with an existing generationId to
    // overwrite.
    WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME, 222L));
    assertThat(writeChannel.isOpen()).isTrue();

    // Verify the initial setup of the insert; capture the input stream.
    ArgumentCaptor<StorageObject> storageObjectCaptor =
        ArgumentCaptor.forClass(StorageObject.class);
    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), storageObjectCaptor.capture(), inputStreamCaptor.capture());

    writeChannel.write(ByteBuffer.wrap(testData));

    // The writes are now done, we can return from the execute method.
    // There is an issue with how PipedInputStream functions since it checks
    // to see if the sending and receiving threads are alive.
    waitTillWritesAreDoneLatch.countDown();

    // Flush, make sure the data made it out the other end.
    writeChannel.close();

    assertThat(writeChannel.isOpen()).isFalse();
    verify(mockStorageObjectsInsert, times(1)).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(222L));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(1L));
    assertThat(storageObjectCaptor.getValue().getName()).isEqualTo(OBJECT_NAME);
    verify(mockHeaders, times(1)).set(
        eq("X-Goog-Upload-Desired-Chunk-Granularity"),
        eq(AbstractGoogleAsyncWriteChannel.GCS_UPLOAD_GRANULARITY));
    verify(mockHeaders, times(0)).set(eq("X-Goog-Upload-Max-Raw-Size"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(AbstractGoogleClientRequest.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockBackOffFactory).newBackOff();
    verify(mockBackOff).nextBackOffMillis();
    verify(mockStorageObjectsInsert, times(2)).execute();
    synchronized (readData) {
      assertThat(readData).isEqualTo(testData);
    }

    // Closing the closed channel should have no effect.
    writeChannel.close();

    // On close(), it should have stashed away the completed ItemInfo based on the returned
    // StorageObject.
    GoogleCloudStorageItemInfo finishedInfo =
        ((GoogleCloudStorageItemInfo.Provider) writeChannel).getItemInfo();
    assertThat(finishedInfo.getBucketName()).isEqualTo(BUCKET_NAME);
    assertThat(finishedInfo.getObjectName()).isEqualTo(OBJECT_NAME);
    assertThat(finishedInfo.getContentGeneration()).isEqualTo(12345L);

    // Further writes to a closed channel should throw a ClosedChannelException.
    assertThrows(ClosedChannelException.class, () -> writeChannel.write(ByteBuffer.wrap(testData)));
  }

  /**
   * Test handling when the parent thread waiting for the write to finish via
   * the close call is interrupted, that the actual write is cancelled and interrupted
   * as well.
   */
  @Test
  public void testCreateObjectApiInterruptedException()
      throws Exception {
    // Prepare the mock return values before invoking the method being tested.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsInsert)))
        .thenReturn(mockHeaders);

    // Set up the mock Insert to wait forever.
    final CountDownLatch waitForEverLatch = new CountDownLatch(1);
    final CountDownLatch writeStartedLatch = new CountDownLatch(2);
    final CountDownLatch threadsDoneLatch = new CountDownLatch(2);
    setupNonConflictedWrite(new Answer<StorageObject>() {
      @Override
      public StorageObject answer(InvocationOnMock invocation) throws Throwable {
        try {
          writeStartedLatch.countDown();
          waitForEverLatch.await();
          fail("Unexpected to get here.");
          return null;
        } finally {
          threadsDoneLatch.countDown();
        }
      }
    });

    final WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    Future<?> write =
        executorService.submit(
            new Runnable() {
              @Override
              public void run() {
                try {
                  writeStartedLatch.countDown();
                  writeChannel.close();
                  fail("Expected IOException");
                } catch (IOException ioe) {
                  assertThat(ioe.getClass()).isEqualTo(ClosedByInterruptException.class);
                } finally {
                  threadsDoneLatch.countDown();
                }
              }
            });
    // Wait for the insert object to be executed, then cancel the writing thread, and finally wait
    // for the two threads to finish.
    assertWithMessage("Neither thread started.")
        .that(writeStartedLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();
    write.cancel(true /* interrupt */);
    assertWithMessage("Failed to wait for tasks to get interrupted.")
        .that(threadsDoneLatch.await(5000, TimeUnit.MILLISECONDS))
        .isTrue();

    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert, times(1)).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockHeaders, times(1)).set(
        eq("X-Goog-Upload-Desired-Chunk-Granularity"),
        eq(AbstractGoogleAsyncWriteChannel.GCS_UPLOAD_GRANULARITY));
    verify(mockHeaders, times(0)).set(eq("X-Goog-Upload-Max-Raw-Size"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(AbstractGoogleClientRequest.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(1L));
    verify(mockStorageObjects, times(1)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(1)).execute();
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockBackOffFactory).newBackOff();
    verify(mockBackOff).nextBackOffMillis();
    verify(mockStorageObjectsInsert, times(2)).execute();
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

    // Set up the mock Insert to throw an exception when execute() is called.
    IOException fakeException = new IOException("Fake IOException");
    setupNonConflictedWrite(fakeException);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    IOException thrown = assertThrows(IOException.class, () -> writeChannel.close());
    assertThat(thrown).hasCauseThat().isEqualTo(fakeException);

    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert, times(1)).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockHeaders, times(1)).set(
        eq("X-Goog-Upload-Desired-Chunk-Granularity"),
        eq(AbstractGoogleAsyncWriteChannel.GCS_UPLOAD_GRANULARITY));
    verify(mockHeaders, times(0)).set(eq("X-Goog-Upload-Max-Raw-Size"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(AbstractGoogleClientRequest.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(1L));
    verify(mockStorageObjects, times(1)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(1)).execute();
    verify(mockErrorExtractor).itemNotFound(any(IOException.class));
    verify(mockBackOffFactory).newBackOff();
    verify(mockBackOff).nextBackOffMillis();
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

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsInsert)))
        .thenReturn(mockHeaders);

    // Set up the mock Insert to throw an exception when execute() is called.
    RuntimeException fakeException = new RuntimeException("Fake exception");
    setupNonConflictedWrite(fakeException);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    IOException thrown = assertThrows(IOException.class, () -> writeChannel.close());
    assertThat(thrown).hasCauseThat().isEqualTo(fakeException);

    verify(mockStorageObjectsInsert, times(2)).execute();
    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjects, times(1)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockErrorExtractor, atLeastOnce()).itemNotFound(any(IOException.class));
    verify(mockBackOffFactory, atLeastOnce()).newBackOff();
    verify(mockBackOff, times(1)).nextBackOffMillis();
    verify(mockStorageObjectsGet, times(1)).execute();
    verify(mockStorageObjectsInsert, times(1)).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockStorageObjects, times(1)).get(anyString(), anyString());
    verify(mockHeaders, times(1)).set(
        eq("X-Goog-Upload-Desired-Chunk-Granularity"),
        eq(AbstractGoogleAsyncWriteChannel.GCS_UPLOAD_GRANULARITY));
    verify(mockHeaders, times(0)).set(eq("X-Goog-Upload-Max-Raw-Size"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(AbstractGoogleClientRequest.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert, times(2)).setIfGenerationMatch(anyLong());

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

    // Set up the mock Insert to throw an exception when execute() is called.
    Error fakeError = new Error("Fake error");
    setupNonConflictedWrite(fakeError);

    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsInsert)))
        .thenReturn(mockHeaders);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    Error thrown = assertThrows(Error.class, () -> writeChannel.close());
    assertThat(thrown).isEqualTo(fakeError);

    verify(mockStorage, times(3)).objects();
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjects).get(BUCKET_NAME, OBJECT_NAME);
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjectsInsert, times(1)).setName(eq(OBJECT_NAME));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(0L));
    verify(mockStorageObjectsInsert, times(1)).setIfGenerationMatch(eq(1L));
    verify(mockErrorExtractor, times(1)).itemNotFound(any(IOException.class));
    verify(mockBackOff, atLeastOnce()).nextBackOffMillis();
    verify(mockBackOffFactory, atLeastOnce()).newBackOff();
    verify(mockHeaders, times(1)).set(
        eq("X-Goog-Upload-Desired-Chunk-Granularity"),
        eq(AbstractGoogleAsyncWriteChannel.GCS_UPLOAD_GRANULARITY));
    verify(mockHeaders, times(0)).set(eq("X-Goog-Upload-Max-Raw-Size"), anyInt());
    verify(mockClientRequestHelper).getRequestHeaders(any(AbstractGoogleClientRequest.class));
    verify(mockClientRequestHelper).setChunkSize(any(Storage.Objects.Insert.class), anyInt());
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
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

    assertThat(storageObjectCaptor.getValue().getName()).isEqualTo(OBJECT_NAME);
    assertThat(inputStreamCaptor.getValue().getLength()).isEqualTo(0);
  }

  /**
   * Helper for the shared boilerplate of setting up the low-level "API objects" like
   * mockStorage.objects(), etc., that is common between test cases targeting {@code
   * GoogleCloudStorage.open(StorageResourceId)}.
   */
  private void setUpBasicMockBehaviorForOpeningReadChannel() throws IOException {
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
    readChannel.setBackOff(mockBackOff);
    readChannel.setReadBackOff(mockReadBackOff);
    readChannel.setMaxRetries(maxRetries);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);
  }

  /**
   * Test argument sanitization for GoogleCloudStorage.open(2).
   */
  @Test
  public void testOpenObjectIllegalArguments()
      throws IOException {
    for (String[] objectPair : ILLEGAL_OBJECTS) {
      assertThrows(
          IllegalArgumentException.class,
          () -> gcs.open(new StorageResourceId(objectPair[0], objectPair[1])));
    }
  }

  @Test
  public void testGcsReadChannelCloseIdempotent() throws IOException {
    GoogleCloudStorageReadChannel channel = new GoogleCloudStorageReadChannel();
    channel.close();
    channel.close();
  }

  @Test
  public void testOpenWithSomeExceptionsDuringRead()
      throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

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

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L).thenReturn(222L).thenReturn(333L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(4)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(4)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockStorageObjectsGet, times(4)).executeMedia();
    verify(mockBackOff, times(1)).reset();
    verify(mockReadBackOff, times(1)).reset();
    verify(mockReadBackOff, times(3)).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));
    verify(mockSleeper).sleep(eq(222L));
    verify(mockSleeper).sleep(eq(333L));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);
  }

  @Test
  public void testOpenWithExceptionDuringReadAndCloseForRetry()
      throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // First returned timeout stream will timout; we'll expect a re-opening where we'll return the
    // real input stream.
    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream))
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(testData)));

    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new SSLException("fake SSLException"));
    doThrow(new SSLException("fake SSLException on close()"))
        .doThrow(new SSLException("second fake SSLException on close()"))
        .when(mockExceptionStream).close();

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockBackOff, times(1)).reset();
    verify(mockReadBackOff, times(1)).reset();
    verify(mockReadBackOff).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);
  }

  @Test
  public void testClosesWithRuntimeExceptionDuringReadAndClose()
      throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // First returned timeout stream will timout;
    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream));

    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new RuntimeException("fake RuntimeException"));
    doThrow(new SSLException("fake SSLException on close()"))
        .when(mockExceptionStream).close();


    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];

    assertThrows(RuntimeException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));

    assertThat(readChannel.readChannel).isNull();
    assertThat(readChannel.lazySeekPending).isFalse();
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(1)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(1)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockStorageObjectsGet, times(1)).executeMedia();
    verify(mockBackOff).reset();  // Called because backoff is set in test.
  }

  @Test
  public void testCloseWithExceptionDuringClose()
      throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // First returned timeout stream will timout;
    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream));

    doThrow(new SSLException("fake SSLException on close()"))
        .when(mockExceptionStream).close();

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 0);
    readChannel.performLazySeek();
    assertThat(readChannel.readChannel).isNotNull();

    // Should not throw exception. If it does, it will be caught by the test harness.
    readChannel.close();

    assertThat(readChannel.readChannel).isNull();
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(1)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(1)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockStorageObjectsGet, times(1)).executeMedia();
    verify(mockBackOff).reset();  // Called because backoff is set in test.
  }

  @Test
  public void testRetrysOnGetMetadata() throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);
    SocketTimeoutException socketException = new SocketTimeoutException("Socket1");
    when(mockStorageObjectsGet.execute()).thenThrow(socketException);
    readChannel.setBackOff(mockBackOff);
    readChannel.setSleeper(mockSleeper);

    IOException thrown = assertThrows(IOException.class, () -> readChannel.getMetadata());
    assertWithMessage("Expected " + socketException + " inside IOException")
        .that(thrown)
        .hasCauseThat()
        .isEqualTo(socketException);
    verify(mockBackOff, times(1)).reset();
    verify(mockBackOff, times(3)).nextBackOffMillis();
    verify(mockSleeper, times(3)).sleep(anyLong());
    verify(mockStorage, times(2)).objects();
    verify(mockErrorExtractor, times(1)).itemNotFound(any(IOException.class));
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(5)).execute(); //4 repeated, 1 from GoogleCloudStorageImpl
  }

  @Test
  public void testOpenAndReadWithPrematureEndOfStreamRetriesFail()
      throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // We'll claim a Content-Length of testData.length, but then only return a stream containing
    // truncatedData. The channel should throw an exception upon detecting this premature
    // end-of-stream.
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    byte[] truncatedData = { 0x01, 0x02, 0x03 };
    byte[] truncatedRetryData = { 0x05 };
    when(mockStorageObjectsGet.executeMedia())
        // First time: Claim  we'll provide 5 bytes, but only give 3.
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(truncatedData)))
        // Second time: Claim we'll provide the 2 remaining bytes, but only give one byte.
        // This retry counts toward the maxRetries of the "first" attempt, but the nonzero bytes
        // returned resets the counter; when this ends prematurely we'll expect yet another "retry"
        // even though we'll set maxRetries == 1.
        .thenReturn(createFakeResponse(2, new ByteArrayInputStream(truncatedRetryData)))
        // Third time, we claim we'll deliver the one remaining byte, but give none. Since no
        // progress is made, the retry counter does not get reset and we've exhausted all retries.
        .thenReturn(createFakeResponse(1, new ByteArrayInputStream(new byte[0])));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    // Only allow one retry for this test.
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 1);

    byte[] actualData = new byte[testData.length];
    assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));

    // Both "retries" reset the mockBackOff, since they are "independent" retries with progress
    // in-between. One initial retry for getMetadata.
    verify(mockBackOff, times(1)).reset();
    verify(mockReadBackOff, times(2)).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper, times(2)).sleep(anyLong());
    verify(mockStorage, times(5)).objects();
    verify(mockStorageObjects, times(5)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(1)).setRange(eq("bytes=0-"));
    verify(mockHeaders, times(1)).setRange(eq("bytes=3-"));
    verify(mockHeaders, times(1)).setRange(eq("bytes=4-"));
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockStorageObjectsGet, times(3)).executeMedia();
  }

  @Test
  public void testOpenAndReadWithPrematureEndOfStreamRetriesSucceed()
      throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    // We'll claim a Content-Length of testData.length, but then only return a stream containing
    // truncatedData. The channel should throw an exception upon detecting this premature
    // end-of-stream.
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    byte[] truncatedData = { 0x01, 0x02, 0x03 };
    byte[] truncatedRetryData = { 0x05 };
    byte[] finalRetryData = { 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        // First time: Claim  we'll provide 5 bytes, but only give 3.
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(truncatedData)))
        // Second time: Claim we'll provide the 2 remaining bytes, but only give one byte.
        // This retry counts toward the maxRetries of the "first" attempt, but the nonzero bytes
        // returned resets the counter; when this ends prematurely we'll expect yet another "retry"
        // even though we'll set maxRetries == 1.
        .thenReturn(createFakeResponse(2, new ByteArrayInputStream(truncatedRetryData)))
        // Third time, we claim we'll deliver the one remaining byte, but give none. Since no
        // progress is made, the retry counter does not get reset and we've exhausted all retries.
        .thenReturn(createFakeResponse(1, new ByteArrayInputStream(finalRetryData)));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    // Only allow one retry for this test.
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 1);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    // Both "retries" reset the mockBackOff, since they are "independent" retries with progress
    // in-between. One initial retry for getMetadata.
    verify(mockBackOff, times(1)).reset();
    verify(mockReadBackOff, times(2)).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper, times(2)).sleep(anyLong());
    verify(mockStorage, times(5)).objects();
    verify(mockStorageObjects, times(5)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(1)).setRange(eq("bytes=0-"));
    verify(mockHeaders, times(1)).setRange(eq("bytes=3-"));
    verify(mockHeaders, times(1)).setRange(eq("bytes=4-"));
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockStorageObjectsGet, times(3)).executeMedia();

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);
  }

  @Test
  public void testOpenExceptionsDuringReadTotalElapsedTimeTooGreat()
      throws IOException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream));
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockClock.nanoTime())
        .thenReturn(1000000L)
        .thenReturn(1000001L)
        .thenReturn(
            (GoogleCloudStorageReadChannel.DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS + 3) * 1000000L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);
    readChannel.setBackOff(null);
    readChannel.setReadBackOff(null);

    byte[] actualData = new byte[testData.length];
    IOException thrown =
        assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));
    assertThat(thrown).hasMessageThat().isEqualTo("fake generic IOException");

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClock, times(3)).nanoTime();
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).executeMedia();
    verify(mockStorageObjectsGet, times(2)).execute();
  }

  @Test
  public void testOpenExceptionsDuringReadInterruptedDuringSleep()
      throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, mockExceptionStream));
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenThrow(new IOException("fake generic IOException"));

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L);
    InterruptedException interrupt = new InterruptedException("fake interrupt");
    doThrow(interrupt)
        .when(mockSleeper).sleep(eq(111L));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];
    IOException thrown =
        assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));
    assertThat(thrown).hasMessageThat().isEqualTo("fake generic IOException");
    assertThat(thrown.getSuppressed()).hasLength(1);
    assertThat(thrown.getSuppressed()[0]).isEqualTo(interrupt);
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).executeMedia();
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockBackOff, times(1)).reset();
    verify(mockReadBackOff, times(1)).reset();
    verify(mockReadBackOff, times(1)).nextBackOffMillis();
    verify(mockSleeper, times(1)).sleep(eq(111L));
  }

  @Test
  public void testOpenTooManyExceptionsDuringRead()
      throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

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

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L).thenReturn(222L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 2);

    byte[] actualData = new byte[testData.length];
    IOException thrown =
        assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));
    assertThat(thrown).hasMessageThat().isEqualTo("fake generic IOException");

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockBackOff, times(1)).reset();
    verify(mockReadBackOff, times(1)).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper).sleep(eq(111L));
    verify(mockSleeper).sleep(eq(222L));
  }

  @Test
  public void testOpenTwoTimeoutsWithIntermittentProgress()
      throws IOException, InterruptedException {
    setUpBasicMockBehaviorForOpeningReadChannel();

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

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(111L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 1);

    // Should succeed even though, in total, there were more retries than maxRetries, since we
    // made progress between errors.
    byte[] actualData = new byte[testData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(testData.length);
    assertThat(readChannel.position()).isEqualTo(5);

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=3-"));
    verify(mockBackOff, times(1)).reset();
    verify(mockReadBackOff, times(2)).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper, times(2)).sleep(eq(111L));
  }

  /**
   * Generate a random byte array from a fixed size.
   */
  private static byte[] generateTestData(int size) {
    Random random = new Random(0);
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  /**
   * Test successful operation of GoogleCloudStorage.open(2) with Content-Encoding: gzip files.
   */
  @Test
  public void testOpenGzippedObjectNormalOperation() throws IOException {
    // Generate 1k test data to compress.
    byte[] testData = generateTestData(1000);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    GZIPOutputStream gzipper = new GZIPOutputStream(os);
    gzipper.write(testData);
    byte[] compressedData = os.toByteArray();

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
                .setUpdated(new DateTime(11L))
                .setSize(BigInteger.valueOf(compressedData.length))
                .setGeneration(1L)
                .setMetageneration(1L)
                .setContentEncoding("gzip"));
    // content. Mock this by providing a stream that simply provides the uncompressed content.
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(compressedData.length, new ByteArrayInputStream(testData)))
        .thenReturn(createFakeResponse(compressedData.length, new ByteArrayInputStream(testData)))
        .thenReturn(createFakeResponse(compressedData.length, new ByteArrayInputStream(testData)));

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);
    assertThat(readChannel.size()).isEqualTo(compressedData.length);
    byte[] actualData = new byte[testData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(testData.length);
    assertThat(readChannel.readChannel != null).isTrue();
    assertThat(actualData).isEqualTo(testData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    // Repositioning to an invalid position fails.
    assertThrows(EOFException.class, () -> readChannel.position(-1));

    // Repositioning to a position both before and after size() succeeds.
    readChannel.position(2);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(2);
    byte[] partialData = Arrays.copyOfRange(testData, 2, testData.length);
    actualData = new byte[partialData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(partialData.length);
    assertThat(actualData).isEqualTo(partialData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    int compressedLength = compressedData.length;
    readChannel.position(compressedLength);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(compressedLength);
    partialData = Arrays.copyOfRange(testData, compressedLength, testData.length);
    actualData = new byte[partialData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(partialData.length);
    assertThat(actualData).isEqualTo(partialData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    verify(mockStorage, times(5)).objects();
    verify(mockStorageObjects, times(5)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setRange(eq("bytes=0-"));
  }

  /**
   * If we disable the supportContentEncoding option when opening a channel, and disable
   * failing fast on nonexistent objects we should expect no extraneous metadata-GET calls at all.
   */
  @Test
  public void testOpenNoSupportContentEncodingAndNoFailFastOnNotFound() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(testData)));
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenReturn(testData.length);
    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
            new GoogleCloudStorageReadOptions.Builder()
                .setFastFailOnNotFound(false)
                .setSupportContentEncoding(false)
                .build());

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));
    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).executeMedia();

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);
  }

  /**
   * Test in-place forward seeks smaller than seek buffer, smaller than limit.
   */
  @Test
  public void testInplaceSeekSmallerThanSeekBuffer() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(testData)));
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenReturn(testData.length);
    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
            new GoogleCloudStorageReadOptions.Builder()
                .setFastFailOnNotFound(false)
                .setSupportContentEncoding(false)
                .setInplaceSeekLimit(2)
                .build());

    byte[] actualData1 = new byte[1];
    int bytesRead1 = readChannel.read(ByteBuffer.wrap(actualData1));

    // Jump 2 bytes forwards; this should be done in-place without any new executeMedia() call.
    readChannel.position(3);
    assertThat(readChannel.position()).isEqualTo(3);

    byte[] actualData2 = new byte[2];
    int bytesRead2 = readChannel.read(ByteBuffer.wrap(actualData2));

    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).executeMedia();

    assertThat(bytesRead1).isEqualTo(1);
    assertThat(bytesRead2).isEqualTo(2);
    assertThat(actualData1).isEqualTo(new byte[] {0x01});
    assertThat(actualData2).isEqualTo(new byte[] {0x05, 0x08});
  }

  /**
   * Test in-place forward seeks larger than seek buffer but smaller than limit.
   */
  @Test
  public void testInplaceSeekLargerThanSeekBuffer() throws Exception {
    setUpBasicMockBehaviorForOpeningReadChannel();

    InputStream mockExceptionStream = mock(InputStream.class);
    byte[] testData = new byte[GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE + 5];
    testData[0] = 0x01;
    testData[testData.length - 4] = 0x02;
    testData[testData.length - 3] = 0x03;
    testData[testData.length - 2] = 0x05;
    testData[testData.length - 1] = 0x08;
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(testData)));
    when(mockExceptionStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenReturn(testData.length);
    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
            new GoogleCloudStorageReadOptions.Builder()
                .setFastFailOnNotFound(false)
                .setSupportContentEncoding(false)
                .setInplaceSeekLimit(2 * GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE)
                .build());

    byte[] actualData1 = new byte[1];
    int bytesRead1 = readChannel.read(ByteBuffer.wrap(actualData1));

    // Jump 2 bytes + SKIP_BUFFER_SIZE forwards; this should be done in-place without any
    // new executeMedia() call.
    readChannel.position(GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE + 3);
    assertThat(readChannel.position())
        .isEqualTo(GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE + 3);

    byte[] actualData2 = new byte[2];
    int bytesRead2 = readChannel.read(ByteBuffer.wrap(actualData2));

    verify(mockStorage).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet).executeMedia();

    assertThat(bytesRead1).isEqualTo(1);
    assertThat(bytesRead2).isEqualTo(2);
    assertThat(actualData1).isEqualTo(new byte[] {0x01});
    assertThat(actualData2).isEqualTo(new byte[] {0x05, 0x08});
  }

  /**
   * Test operation of GoogleCloudStorage.open(2) with Content-Encoding: gzip files when exceptions
   * occur during reading.
   */
  @Test
  public void testOpenGzippedObjectExceptionsDuringRead() throws Exception {
    final byte[] testData = generateTestData(1000);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    GZIPOutputStream gzipper = new GZIPOutputStream(os);
    gzipper.write(testData);
    byte[] compressedData = os.toByteArray();
    final int compressedLength = compressedData.length;

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
                .setUpdated(new DateTime(11L))
                .setSize(BigInteger.valueOf(compressedData.length))
                .setGeneration(1L)
                .setMetageneration(1L)
                .setContentEncoding("gzip"));

    // Content-Length will be the size of the compressed data. During the first read attempt,
    // we will read < size bytes before throwing an exception. During the second read
    // attempt, we will throw an exception after > size bytes have been read. Finally, we
    // will finish reading.
    InputStream firstTimeoutStream = mock(InputStream.class);
    InputStream secondTimeoutStream = mock(InputStream.class);
    when(firstTimeoutStream.read(any(byte[].class), eq(0), eq(testData.length)))
        .thenAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            byte[] inputBuf = (byte[]) args[0];
            // Read < size bytes
            System.arraycopy(testData, 0, inputBuf, 0, compressedLength / 2);
            return Integer.valueOf(compressedLength / 2);
          }
        });
    when(
        firstTimeoutStream.read(
            any(byte[].class), eq(0), eq(testData.length - compressedLength / 2)))
        .thenThrow(new SocketTimeoutException("fake timeout 1"));

    when(
        secondTimeoutStream.read(
            any(byte[].class), eq(0), eq(testData.length - compressedLength / 2)))
        .thenAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            byte[] inputBuf = (byte[]) args[0];
            // Read > size bytes
            System.arraycopy(testData, compressedLength / 2, inputBuf, 0, compressedLength / 2);
            return Integer.valueOf(compressedLength / 2);
          }
        });
    when(secondTimeoutStream.read(any(byte[].class), eq(0), eq(testData.length - compressedLength)))
        .thenThrow(new SocketTimeoutException("fake timeout 2"));
    when(firstTimeoutStream.available())
        .thenReturn(testData.length)
        .thenReturn(testData.length - compressedLength / 2);
    when(secondTimeoutStream.available())
        .thenReturn(testData.length - compressedLength / 2)
        .thenReturn(testData.length - compressedLength);

    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, firstTimeoutStream))
        .thenReturn(createFakeResponse(testData.length, secondTimeoutStream))
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(testData)));

    when(mockReadBackOff.nextBackOffMillis()).thenReturn(1L);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    setUpAndValidateReadChannelMocksAndSetMaxRetries(readChannel, 3);

    byte[] actualData = new byte[testData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(testData.length);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    verify(mockStorage, times(5)).objects();
    verify(mockStorageObjects, times(5)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(3)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).execute();
    verify(mockClientRequestHelper, times(3)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(3)).setRange(eq("bytes=0-"));
    verify(mockBackOff, times(1)).reset();
    verify(mockReadBackOff, times(2)).reset();
    verify(mockReadBackOff, times(2)).nextBackOffMillis();
    verify(mockSleeper, times(2)).sleep(eq(1L));
  }

  /**
   * Test successful operation of GoogleCloudStorage.open(2).
   */
  @Test
  public void testOpenObjectNormalOperation()
      throws IOException {
    // Make the response return some fake data, prepare for a second API call when we call
    // 'position' on the returned channel.
    byte[] testData = { 0x01, 0x02, 0x03, 0x05, 0x08 };
    byte[] testData2 = { 0x03, 0x05, 0x08 };
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockClientRequestHelper.getRequestHeaders(eq(mockStorageObjectsGet)))
        .thenReturn(mockHeaders);
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorageObjectsGet.execute())
        .thenReturn(
            new StorageObject()
                .setBucket(BUCKET_NAME)
                .setName(OBJECT_NAME)
                .setUpdated(new DateTime(11L))
                .setSize(BigInteger.valueOf(testData.length))
                .setGeneration(1L)
                .setMetageneration(1L));
    when(mockStorageObjectsGet.executeMedia())
        .thenReturn(createFakeResponse(testData.length, new ByteArrayInputStream(testData)))
        .thenReturn(createFakeResponseForRange(
            testData2.length, new ByteArrayInputStream(testData2)));

   GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);
    byte[] actualData = new byte[testData.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData))).isEqualTo(testData.length);
    assertThat(readChannel.readChannel != null).isTrue();
    assertThat(actualData).isEqualTo(testData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    readChannel.position(2);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(2);

    // Repositioning to invalid position fails.
    assertThrows(EOFException.class, () -> readChannel.position(-1));
    assertThrows(EOFException.class, () -> readChannel.position(testData.length));

    // Repositioning to current position should result in no API calls.
    readChannel.position(2);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(2);

    // Reading into a buffer with no room should have no effect.
    assertThat(readChannel.read(ByteBuffer.wrap(new byte[0]))).isEqualTo(0);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(2);

    byte[] actualData2 = new byte[testData2.length];
    assertThat(readChannel.read(ByteBuffer.wrap(actualData2))).isEqualTo(testData2.length);
    assertThat(actualData2).isEqualTo(testData2);

    // Note that position will be testData.length, *not* testData2.length (5, not 3).
    assertThat(readChannel.position()).isEqualTo(testData.length);

    // Verify the request being made.
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders).setRange(eq("bytes=0-"));
    verify(mockHeaders).setRange(eq("bytes=2-"));
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockStorageObjectsGet, times(2)).execute();

    readChannel.close();
    assertThat(readChannel.isOpen()).isFalse();

    // After closing the channel, future reads should throw a ClosedChannelException.
    assertThrows(ClosedChannelException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));
    assertThrows(ClosedChannelException.class, () -> readChannel.position(0));
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
    assertThrows(
        FileNotFoundException.class,
        () -> gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    // Second time is the rangeNotSatisfiableException.
    SeekableByteChannel readChannel2 = gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(readChannel2.size()).isEqualTo(0);

    // Third time is the unexpectedException.
    SeekableByteChannel readChannel3 = gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    IOException thrown = assertThrows(IOException.class, () -> readChannel3.size());
    assertThat(thrown).hasCauseThat().isEqualTo(unexpectedException);

    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects, atLeastOnce()).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockClientRequestHelper, times(2)).getRequestHeaders(any(Storage.Objects.Get.class));
    verify(mockHeaders, times(2)).setRange(eq("bytes=0-"));
    verify(mockStorageObjectsGet, times(5)).execute();
    verify(mockStorageObjectsGet, times(2)).executeMedia();
    verify(mockErrorExtractor, times(3)).itemNotFound(any(IOException.class));
    verify(mockErrorExtractor, atLeastOnce()).rangeNotSatisfiable(any(IOException.class));
  }

  /**
   * Test argument sanitization for GoogleCloudStorage.create(String).
   */
  @Test
  public void testCreateBucketIllegalArguments()
      throws IOException {
    assertThrows(IllegalArgumentException.class, () -> gcs.create((String) null));
    assertThrows(IllegalArgumentException.class, () -> gcs.create(""));
  }

  /**
   * Test successful operation of GoogleCloudStorage.create(String).
   */
  @Test
  public void testCreateBucketNormalOperation()
      throws IOException {
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
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
   * Test successful operation of GoogleCloudStorage.create(String, CreateBucketOptions).
   */
  @Test
  public void testCreateBucketWithOptionsNormalOperation()
      throws IOException {
    final Bucket[] bucketWithOptions = new Bucket[1];
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);

    final Storage.Buckets.Insert finalMockInsert = mockStorageBucketsInsert;
    when(mockStorageBuckets.insert(eq(PROJECT_ID), any(Bucket.class)))
        .thenAnswer(new Answer<Storage.Buckets.Insert>() {
          @Override public Storage.Buckets.Insert answer(InvocationOnMock invocation) {
            bucketWithOptions[0] = (Bucket) invocation.getArguments()[1];
            return finalMockInsert;
          }});
    gcs.create(BUCKET_NAME, new CreateBucketOptions("some-location", "storage-class"));

    assertThat(bucketWithOptions[0].getName()).isEqualTo(BUCKET_NAME);
    assertThat(bucketWithOptions[0].getLocation()).isEqualTo("some-location");
    assertThat(bucketWithOptions[0].getStorageClass()).isEqualTo("storage-class");

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
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);
    when(mockStorage.buckets()).thenReturn(mockStorageBuckets);
    when(mockStorageBuckets.insert(eq(PROJECT_ID), any(Bucket.class)))
        .thenReturn(mockStorageBucketsInsert);
    when(mockStorageBucketsInsert.execute())
        .thenThrow(new IOException("Fake exception"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(false);

    // TODO(user): Switch to testing for FileExistsException once implemented.
    assertThrows(IOException.class, () -> gcs.create(BUCKET_NAME));

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
    assertThrows(
        IllegalArgumentException.class,
        () -> gcs.deleteBuckets(Lists.<String>newArrayList((String) null)));
    assertThrows(IllegalArgumentException.class, () -> gcs.deleteBuckets(Lists.newArrayList("")));
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
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);

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
    when(mockBackOffFactory.newBackOff()).thenReturn(mockBackOff);

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
    assertThrows(IOException.class, () -> gcs.deleteBuckets(Lists.newArrayList(BUCKET_NAME)));

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
      assertThrows(
          IllegalArgumentException.class,
          () ->
              gcs.deleteObjects(
                  Lists.newArrayList(new StorageResourceId(objectPair[0], objectPair[1]))));
    }
  }

  /** Test successful operation of GoogleCloudStorage.delete(2). */
  @Test
  public void testDeleteObjectNormalOperation() throws IOException {
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

    gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    verify(mockBatchFactory).newBatchHelper(any(HttpRequestInitializer.class),
        eq(mockStorage), any(Long.class));
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects).delete(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsDelete).setIfGenerationMatch(eq(1L));
    verify(mockBatchHelper, times(1)).flush();
    verify(mockBatchHelper, times(2))
        .queue(
            Matchers.<StorageRequest<Object>>anyObject(),
            Matchers.<JsonBatchCallback<Object>>anyObject());
  }

  /** Test successful operation of GoogleCloudStorage.delete(2) with generationId. */
  @Test
  public void testDeleteObjectWithGenerationId() throws IOException {
    when(mockBatchFactory.newBatchHelper(any(HttpRequestInitializer.class),
        any(Storage.class), any(Long.class))).thenReturn(mockBatchHelper);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.delete(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsDelete);

    doAnswer(new Answer() {
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

    gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME, 222L)));

    verify(mockBatchFactory).newBatchHelper(any(HttpRequestInitializer.class),
        eq(mockStorage), any(Long.class));
    verify(mockStorage, atLeastOnce()).objects();
    verify(mockStorageObjects).delete(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsDelete).setIfGenerationMatch(eq(222L));
    verify(mockBatchHelper, times(1)).flush();
    verify(mockBatchHelper, times(1))
        .queue(
            Matchers.<StorageRequest<Object>>anyObject(),
            Matchers.<JsonBatchCallback<Object>>anyObject());
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.delete(2).
   */
  @Test
  public void testDeleteObjectApiException() throws IOException {
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
    verify(mockBatchHelper, times(2)).flush();
  }

  /**
   * Test argument sanitization for GoogleCloudStorage.copy(4).
   */
  @Test
  public void testCopyObjectsIllegalArguments()
      throws IOException {
    for (String[] objectPair : ILLEGAL_OBJECTS) {
      // Src is bad.
      assertThrows(
          IllegalArgumentException.class,
          () ->
              gcs.copy(
                  objectPair[0],
                  Lists.newArrayList(objectPair[1]),
                  BUCKET_NAME,
                  Lists.newArrayList(OBJECT_NAME)));

      // Dst is bad.
      assertThrows(
          IllegalArgumentException.class,
          () ->
              gcs.copy(
                  BUCKET_NAME,
                  Lists.newArrayList(OBJECT_NAME),
                  objectPair[0],
                  Lists.newArrayList(objectPair[1])));
    }

    // Failure if src == dst.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            gcs.copy(
                BUCKET_NAME,
                ImmutableList.of(OBJECT_NAME),
                BUCKET_NAME,
                ImmutableList.of(OBJECT_NAME)));

    // Null lists.
    assertThrows(
        IllegalArgumentException.class,
        () -> gcs.copy(BUCKET_NAME, null, BUCKET_NAME, ImmutableList.of(OBJECT_NAME)));
    assertThrows(
        IllegalArgumentException.class,
        () -> gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME), BUCKET_NAME, null));

    // Mismatched number of objects.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            gcs.copy(
                BUCKET_NAME,
                ImmutableList.of(OBJECT_NAME),
                BUCKET_NAME,
                ImmutableList.of(OBJECT_NAME + "1", OBJECT_NAME + "2")));
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
    assertThrows(
        IOException.class,
        () ->
            gcs.copy(
                BUCKET_NAME,
                ImmutableList.of(OBJECT_NAME),
                BUCKET_NAME,
                ImmutableList.of(dstObjectName)));

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
    FileNotFoundException srcFileNotFoundException =
        assertThrows(
            FileNotFoundException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(srcFileNotFoundException).hasMessageThat().contains(BUCKET_NAME);

    // 2. Src unexpected error
    IOException srcIOException =
        assertThrows(
            IOException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(srcIOException).hasMessageThat().isEqualTo(wrappedUnexpectedException1.getMessage());
    assertThat(srcIOException).hasCauseThat().isEqualTo(unexpectedException);

    // 3. Dst 404
    FileNotFoundException dstFileNotFoundException =
        assertThrows(
            FileNotFoundException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(dstFileNotFoundException).hasMessageThat().contains(dstBucketName);

    // 4. Dst unexpected error
    IOException dstIOException =
        assertThrows(
            IOException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(dstIOException).hasMessageThat().isEqualTo(wrappedUnexpectedException2.getMessage());
    assertThat(dstIOException).hasCauseThat().isEqualTo(unexpectedException);

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
    UnsupportedOperationException e1 =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(e1).hasMessageThat().contains("not supported");
    assertThat(e1).hasMessageThat().contains("storage location");

    UnsupportedOperationException e2 =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                gcs.copy(
                    BUCKET_NAME,
                    ImmutableList.of(OBJECT_NAME),
                    dstBucketName,
                    ImmutableList.of(dstObjectName)));
    assertThat(e2).hasMessageThat().contains("not supported");
    assertThat(e2).hasMessageThat().contains("storage class");

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
    assertThat(bucketNames).hasSize(3);
    assertThat(bucketNames.get(0)).isEqualTo("bucket0");
    assertThat(bucketNames.get(1)).isEqualTo("bucket1");
    assertThat(bucketNames.get(2)).isEqualTo("bucket2");

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
    assertThat(bucketInfos).hasSize(3);
    assertThat(bucketInfos.get(0).getBucketName()).isEqualTo("bucket0");
    assertThat(bucketInfos.get(1).getBucketName()).isEqualTo("bucket1");
    assertThat(bucketInfos.get(2).getBucketName()).isEqualTo("bucket2");

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
    when(mockStorageObjects.list(eq(BUCKET_NAME))).thenReturn(mockStorageObjectsList);
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
    assertThat(objectNames)
        .containsExactly(
            "foo/bar/baz/dir0/", "foo/bar/baz/dir1/", "foo/bar/baz/obj0", "foo/bar/baz/obj1")
        .inOrder();

    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.FALSE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).setPageToken("token0");
    verify(mockStorageObjectsList, times(2)).execute();
  }

  /**
   * Test GoogleCloudStorage.listObjectNames(3) with maxResults set.
   */
  @Test
  public void testListObjectNamesPrefixLimited()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    long maxResults = 3;
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

    List<String> objectNames =
        gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter, maxResults);
    assertThat(objectNames)
        .containsExactly("foo/bar/baz/dir0/", "foo/bar/baz/dir1/", "foo/bar/baz/obj0")
        .inOrder();

    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList).setMaxResults(eq(maxResults + 1));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.FALSE));
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
    List<String> objectNames =
        gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter);
    assertThat(objectNames).isEmpty();

    // Second time throws.
    assertThrows(
        IOException.class, () -> gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects, times(2)).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList, times(2)).setMaxResults(
        eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList, times(2)).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList, times(2)).setIncludeTrailingDelimiter(eq(Boolean.FALSE));
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
    assertThat(objectInfos).hasSize(2);
    assertThat(objectInfos.get(0).getObjectName()).isEqualTo(fakeObjectList.get(1).getName());
    assertThat(objectInfos.get(0).getCreationTime())
        .isEqualTo(fakeObjectList.get(1).getUpdated().getValue());
    assertThat(objectInfos.get(0).getSize()).isEqualTo(fakeObjectList.get(1).getSize().longValue());
    assertThat(objectInfos.get(1).getObjectName()).isEqualTo(fakeObjectList.get(2).getName());
    assertThat(objectInfos.get(1).getCreationTime())
        .isEqualTo(fakeObjectList.get(2).getUpdated().getValue());
    assertThat(objectInfos.get(1).getSize()).isEqualTo(fakeObjectList.get(2).getSize().longValue());

    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.TRUE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).execute();
  }

  @Test
  public void testListObjectInfoReturnPrefixes()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    StorageObject dir0 =
        new StorageObject()
            .setName(dir0Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L);
    StorageObject dir1 =
        new StorageObject()
            .setName(dir1Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(eq(BUCKET_NAME))).thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.execute())
        .thenReturn(
            new Objects()
                .setPrefixes(ImmutableList.of(dir0Name, dir1Name))
                .setItems(ImmutableList.of(dir0, dir1))
                .setNextPageToken(null));

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    assertThat(objectInfos)
        .containsExactly(
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir0Name), dir0),
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1));

    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.TRUE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).execute();
  }

  @Test
  public void testListObjectInfoReturnPrefixesNotFound()
      throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    String dir2Name = "foo/bar/baz/dir2/";
    StorageObject dir0 =
        new StorageObject()
            .setName(dir0Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(111L))
            .setGeneration(1L)
            .setMetageneration(1L);
    StorageObject dir1 =
        new StorageObject()
            .setName(dir1Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L);
    StorageObject dir2 =
        new StorageObject()
            .setName(dir2Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(33L))
            .setSize(BigInteger.valueOf(333L))
            .setGeneration(3L)
            .setMetageneration(3L);

    // Set up the initial list to return three prefixes, two of which don't exist.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(any(String.class))).thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.execute())
        .thenReturn(
            new Objects()
                .setPrefixes(ImmutableList.of(dir0Name, dir1Name, dir2Name))
                .setItems(ImmutableList.of(dir1))
                .setNextPageToken(null));

    // Set up the follow-up getItemInfos to just return a batch with "not found".
    when(mockBatchFactory.newBatchHelper(
            any(HttpRequestInitializer.class), any(Storage.class), any(Long.class)))
        .thenReturn(mockBatchHelper);
    when(mockStorageObjects.get(any(String.class), any(String.class)))
        .thenReturn(mockStorageObjectsGet);
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              @SuppressWarnings("unchecked")
              JsonBatchCallback<StorageObject> callback =
                  (JsonBatchCallback<StorageObject>) args[1];
              try {
                callback.onSuccess(dir0, new HttpHeaders());
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
                callback.onSuccess(dir2, new HttpHeaders());
              } catch (IOException ioe) {
                fail(ioe.toString());
              }
              return null;
            })
        .when(mockBatchHelper)
        .queue(eq(mockStorageObjectsGet), Matchers.anyObject());

    // Set up the "create" for auto-repair after a failed getItemInfos.
    when(mockStorageObjects.insert(
            any(String.class), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    // objects().list, objects().insert x 2, objects().get x 2
    verify(mockStorage, times(5)).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList)
        .setMaxResults(eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.TRUE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).execute();

    // Auto-repair insert.
    verify(mockStorageObjects, times(2)).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper, times(2))
        .setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert, times(2)).execute();

    // Batch get after auto-repair.
    verify(mockBatchFactory)
        .newBatchHelper(any(HttpRequestInitializer.class), eq(mockStorage), any(Long.class));
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), any(String.class));
    verify(mockBatchHelper, times(2))
        .queue(eq(mockStorageObjectsGet), Matchers.<JsonBatchCallback<StorageObject>>anyObject());
    verify(mockBatchHelper).flush();

    // Check logical contents after all the "verify" calls, otherwise the mock verifications won't
    // be executed and we'll have misleading "NoInteractionsWanted" errors.
    assertThat(objectInfos)
        .containsExactly(
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir0Name), dir0),
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1),
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir2Name), dir2));
  }

  @Test
  public void testListObjectInfoNoAutoRepairWithInferImplicit()
      throws IOException {
    runTestListObjectInfoNoAutoRepair(true);
  }

  @Test
  public void testListObjectInfoNoAutoRepairWithoutInferImplicit() throws IOException {
    runTestListObjectInfoNoAutoRepair(false);
  }

  private void runTestListObjectInfoNoAutoRepair(boolean inferImplicit) throws IOException {
    GoogleCloudStorage gcsNoAutoRepair =
        createTestInstanceWithAutoRepairWithInferImplicit(false, inferImplicit);

    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    String dir2Name = "foo/bar/baz/dir2/";
    StorageObject dir1 =
        new StorageObject()
            .setName(dir1Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L);

    // Set up the initial list to return three prefixes and one item
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(any(String.class))).thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.execute())
        .thenReturn(
            new Objects()
                .setPrefixes(ImmutableList.of(dir0Name, dir1Name, dir2Name))
                .setItems(ImmutableList.of(dir1))
                .setNextPageToken(null));

    // List the objects, without attempting any auto-repair
    List<GoogleCloudStorageItemInfo> objectInfos =
        gcsNoAutoRepair.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    // objects().list
    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList).setMaxResults(
        eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.TRUE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).execute();

    // Check logical contents after all the "verify" calls, otherwise the
    // mock verifications won't be executed and we'll have misleading
    // "NoInteractionsWanted" errors.

    if (gcsNoAutoRepair.getOptions().isInferImplicitDirectoriesEnabled()) {
      assertThat(objectInfos)
          .containsExactly(
              createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir0Name)),
              createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1),
              createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir2Name)));
    } else {
      assertThat(objectInfos)
          .containsExactly(
              createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1));
    }
  }

  @Test
  public void testListObjectInfoInferImplicit()
      throws IOException {
    GoogleCloudStorage gcsInferImplicit =
        createTestInstanceWithInferImplicit(true);

    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    String dir2Name = "foo/bar/baz/dir2/";
    StorageObject dir1 =
        new StorageObject()
            .setName(dir1Name)
            .setBucket(BUCKET_NAME)
            .setUpdated(new DateTime(22L))
            .setSize(BigInteger.valueOf(222L))
            .setGeneration(2L)
            .setMetageneration(2L);

    // Set up the initial list to return three prefixes,
    // two of which don't exist.
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(any(String.class))).thenReturn(mockStorageObjectsList);
    when(mockStorageObjectsList.execute())
        .thenReturn(
            new Objects()
                .setPrefixes(ImmutableList.of(dir0Name, dir1Name, dir2Name))
                .setItems(ImmutableList.of(dir1))
                .setNextPageToken(null));

    // List the objects, without attempting any auto-repair
    List<GoogleCloudStorageItemInfo> objectInfos =
        gcsInferImplicit.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    // objects().list
    verify(mockStorage).objects();
    verify(mockStorageObjects).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList).setMaxResults(
        eq(GoogleCloudStorageOptions.MAX_LIST_ITEMS_PER_CALL_DEFAULT));
    verify(mockStorageObjectsList).setDelimiter(eq(delimiter));
    verify(mockStorageObjectsList).setIncludeTrailingDelimiter(eq(Boolean.TRUE));
    verify(mockStorageObjectsList).setPrefix(eq(objectPrefix));
    verify(mockStorageObjectsList).execute();

    // Check logical contents after all the "verify" calls, otherwise the
    // mock verifications won't be executed and we'll have misleading
    // "NoInteractionsWanted" errors.

    // Only one of our three directory objects existed.
    assertThat(objectInfos)
        .containsExactly(
            createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir0Name)),
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1),
            createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir2Name)));
  }

  /**
   * Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent ROOT.
   */
  @Test
  public void testGetItemInfoRoot()
      throws IOException {
    GoogleCloudStorageItemInfo info = gcs.getItemInfo(StorageResourceId.ROOT);
    assertThat(info).isEqualTo(GoogleCloudStorageItemInfo.ROOT_INFO);
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
    assertThat(info).isEqualTo(expected);

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
    assertThrows(
        IllegalArgumentException.class, () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME)));

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
    assertThat(info).isEqualTo(expected);

    // Throw.
    assertThrows(IOException.class, () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME)));

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
            .setContentType("text/plain")
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
        "text/plain",
        EMPTY_METADATA,
        1L,
        1L);
    assertThat(info).isEqualTo(expected);

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

    assertThrows(
        IllegalArgumentException.class,
        () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));
    assertThrows(
        IllegalArgumentException.class,
        () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

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
        null,
        EMPTY_METADATA,
        0 /* Content Generation */,
        0 /* Meta generation */);
    assertThat(info).isEqualTo(expected);

    // Throw.
    assertThrows(
        IOException.class, () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

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
                  .setContentType("image/png")
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
        "image/png",
        EMPTY_METADATA,
        1 /* Content Generation */,
        1 /* Meta generation */);
    GoogleCloudStorageItemInfo expectedRoot = GoogleCloudStorageItemInfo.ROOT_INFO;
    GoogleCloudStorageItemInfo expectedBucket = new GoogleCloudStorageItemInfo(
        new StorageResourceId(BUCKET_NAME), 1234L, 0L, "us-west-123", "class-af4");

    assertThat(itemInfos.get(0)).isEqualTo(expectedObject);
    assertThat(itemInfos.get(1)).isEqualTo(expectedRoot);
    assertThat(itemInfos.get(2)).isEqualTo(expectedBucket);

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

    GoogleCloudStorageItemInfo expectedObject =
        GoogleCloudStorageItemInfo.createNotFound(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    GoogleCloudStorageItemInfo expectedRoot = GoogleCloudStorageItemInfo.ROOT_INFO;
    GoogleCloudStorageItemInfo expectedBucket =
        GoogleCloudStorageItemInfo.createNotFound(new StorageResourceId(BUCKET_NAME));

    assertThat(itemInfos.get(0)).isEqualTo(expectedObject);
    assertThat(itemInfos.get(1)).isEqualTo(expectedRoot);
    assertThat(itemInfos.get(2)).isEqualTo(expectedBucket);

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
    verify(mockBatchFactory)
        .newBatchHelper(any(HttpRequestInitializer.class), eq(mockStorage), any(Long.class));
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
    assertThat(executorService.isShutdown()).isTrue();
  }

  /**
   * Test for argument sanitization in GoogleCloudStorage.waitForBucketEmpty(1).
   */
  @Test
  public void testWaitForBucketEmptyIllegalArguments()
      throws IOException {
    assertThrows(IllegalArgumentException.class, () -> gcs.waitForBucketEmpty(null));
    assertThrows(IllegalArgumentException.class, () -> gcs.waitForBucketEmpty(""));
  }

  /**
   * Test for successful GoogleCloudStorage.waitForBucketEmpty(1) including a sleep/retry.
   */
  @Test
  public void testWaitForBucketEmptySuccess()
      throws IOException, InterruptedException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.list(eq(BUCKET_NAME))).thenReturn(mockStorageObjectsList);
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
    verify(mockStorageObjectsList, times(2)).setMaxResults(eq(2L));
    verify(mockStorageObjectsList, times(2)).setDelimiter(eq(GoogleCloudStorage.PATH_DELIMITER));
    verify(mockStorageObjectsList, times(2)).setIncludeTrailingDelimiter(eq(Boolean.FALSE));
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
    assertThrows(IllegalArgumentException.class, () -> gcs.waitForBucketEmpty(null));
    assertThrows(IllegalArgumentException.class, () -> gcs.waitForBucketEmpty(""));

    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    when(mockStorageObjects.list(eq(BUCKET_NAME))).thenReturn(mockStorageObjectsList);

    OngoingStubbing<Objects> stub = when(mockStorageObjectsList.execute());
    for (int i = 0; i < GoogleCloudStorageImpl.BUCKET_EMPTY_MAX_RETRIES; i++) {
      stub = stub.thenReturn(new Objects()
          .setPrefixes(ImmutableList.of("foo"))
          .setItems(ImmutableList.<StorageObject>of()));
    }

    IOException e = assertThrows(IOException.class, () -> gcs.waitForBucketEmpty(BUCKET_NAME));
    assertThat(e).hasMessageThat().contains("not empty");

    VerificationMode retryTimes = times(GoogleCloudStorageImpl.BUCKET_EMPTY_MAX_RETRIES);
    verify(mockStorage, retryTimes).objects();
    verify(mockStorageObjects, retryTimes).list(eq(BUCKET_NAME));
    verify(mockStorageObjectsList, retryTimes).setMaxResults(eq(2L));
    verify(mockStorageObjectsList, retryTimes).setDelimiter(eq(GoogleCloudStorage.PATH_DELIMITER));
    verify(mockStorageObjectsList, retryTimes).setIncludeTrailingDelimiter(eq(Boolean.FALSE));
    verify(mockStorageObjectsList, retryTimes).execute();
    verify(mockSleeper, retryTimes).sleep(
        eq((long) GoogleCloudStorageImpl.BUCKET_EMPTY_WAIT_TIME_MS));
  }

  @Test
  public void testComposeSuccess() throws Exception {
    String destination = "composedObject";
    List<String> sources = ImmutableList.of("object1", "object2");
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(BUCKET_NAME, destination)).thenReturn(mockStorageObjectsGet);
    when(mockStorageObjects.compose(eq(BUCKET_NAME), eq(destination), any(ComposeRequest.class)))
        .thenReturn(mockStorageObjectsCompose);
    when(mockStorageObjectsCompose.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(destination)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(5L))
            .setGeneration(12345L)
            .setMetageneration(1L));

    gcs.compose(BUCKET_NAME, sources, destination, CreateFileOptions.DEFAULT_CONTENT_TYPE);

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(destination));
    verify(mockStorageObjectsGet).execute();
    verify(mockStorageObjects).compose(eq(BUCKET_NAME), eq(destination), any(ComposeRequest.class));
    verify(mockStorageObjectsCompose).setIfGenerationMatch(0L);
    verify(mockStorageObjectsCompose).execute();
  }

  @Test
  public void testComposeObjectsWithGenerationId() throws Exception {
    String destination = "composedObject";
    StorageResourceId destinationId = new StorageResourceId(BUCKET_NAME, destination, 222L);
    List<StorageResourceId> sources = ImmutableList.of(
        new StorageResourceId(BUCKET_NAME, "object1"),
        new StorageResourceId(BUCKET_NAME, "object2"));
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.compose(eq(BUCKET_NAME), eq(destination), any(ComposeRequest.class)))
        .thenReturn(mockStorageObjectsCompose);
    when(mockStorageObjectsCompose.execute())
        .thenReturn(new StorageObject()
            .setBucket(BUCKET_NAME)
            .setName(destination)
            .setUpdated(new DateTime(11L))
            .setSize(BigInteger.valueOf(5L))
            .setGeneration(12345L)
            .setMetageneration(1L));

    Map<String, byte[]> rawMetadata =
        ImmutableMap.of("foo", new byte[] { 0x01 }, "bar", new byte[] { 0x02 });
    gcs.composeObjects(
        sources, destinationId, new CreateObjectOptions(false, "text-content", rawMetadata));

    ArgumentCaptor<ComposeRequest> composeCaptor = ArgumentCaptor.forClass(ComposeRequest.class);
    verify(mockStorage, times(1)).objects();
    verify(mockStorageObjects).compose(eq(BUCKET_NAME), eq(destination), composeCaptor.capture());
    verify(mockStorageObjectsCompose).setIfGenerationMatch(222L);
    verify(mockStorageObjectsCompose).execute();

    assertThat(composeCaptor.getValue().getSourceObjects()).hasSize(2);
    assertThat(composeCaptor.getValue().getSourceObjects().get(0).getName())
        .isEqualTo(sources.get(0).getObjectName());
    assertThat(composeCaptor.getValue().getSourceObjects().get(1).getName())
        .isEqualTo(sources.get(1).getObjectName());
    assertThat(composeCaptor.getValue().getDestination().getContentType())
        .isEqualTo("text-content");
    assertThat(composeCaptor.getValue().getDestination().getMetadata())
        .isEqualTo(GoogleCloudStorageImpl.encodeMetadata(rawMetadata));
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

    // Verify that projectId == null or empty does not throw.
    optionsBuilder.setProjectId(null);
    new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);

    optionsBuilder.setProjectId("");
    new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage);

    optionsBuilder.setProjectId("projectId");

    // Verify that appName == null or empty throws IllegalArgumentException.

    optionsBuilder.setAppName(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage));

    optionsBuilder.setAppName("");
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageImpl(optionsBuilder.build(), mockStorage));

    optionsBuilder.setAppName("appName");

    // Verify that gcs == null throws NullPointerException.
    assertThrows(
        NullPointerException.class,
        () -> new GoogleCloudStorageImpl(optionsBuilder.build(), (Storage) null));
  }

  /**
   * Provides coverage for default constructor. No real validation is performed.
   */
  @Test
  public void testCoverDefaultConstructor() {
    new GoogleCloudStorageImpl();
  }

  /**
   * Coverage for GoogleCloudStorageItemInfo.metadataEquals.
   */
  @Test
  public void testItemInfoMetadataEquals() {
    assertThat(getItemInfoForEmptyObjectWithMetadata(EMPTY_METADATA).metadataEquals(EMPTY_METADATA))
        .isTrue();

    // The factory method changes 'null' to the empty map, but that doesn't mean an empty
    // metadata setting equals 'null' as the parameter passed to metadataEquals.
    assertThat(getItemInfoForEmptyObjectWithMetadata(null).metadataEquals(EMPTY_METADATA)).isTrue();
    assertThat(getItemInfoForEmptyObjectWithMetadata(null).metadataEquals(null)).isFalse();

    //  Basic equality case.
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))
                .metadataEquals(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02})))
        .isTrue();

    // Equality across different map implementations.
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    new HashMap<String, byte[]>(
                        ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02})))
                .metadataEquals(
                    new TreeMap<String, byte[]>(
                        ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))))
        .isTrue();

    // Even though the keySet() is equal for the two and the set of values() is equal for the two,
    // since we inverted which key points to which value, they should not be deemed equal.
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))
                .metadataEquals(
                    ImmutableMap.of("foo", new byte[] {0x02}, "bar", new byte[] {0x01})))
        .isFalse();

    // Only a subset is equal.
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))
                .metadataEquals(ImmutableMap.of("foo", new byte[] {0x01})))
        .isFalse();
  }

  @Test
  public void testItemInfoEqualityIncludesMetadata() {
    assertThat(
            getItemInfoForEmptyObjectWithMetadata(
                    ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02}))
                .equals(getItemInfoForEmptyObjectWithMetadata(null)))
        .isFalse();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObject() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(getStorageObjectForEmptyObjectWithMetadata(
            ImmutableMap.<String, byte[]>of("foo", new byte[0])));

    gcs.createEmptyObject(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        new CreateObjectOptions(true, ImmutableMap.<String, byte[]>of("foo", new byte[0])));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectMismatchMetadata() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(getStorageObjectForEmptyObjectWithMetadata(EMPTY_METADATA));

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                gcs.createEmptyObject(
                    new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                    new CreateObjectOptions(
                        true, ImmutableMap.<String, byte[]>of("foo", new byte[0]))));
    assertThat(thrown).hasMessageThat().contains("rateLimitExceeded");

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectMismatchMetadataButOptionsHasNoMetadata()
      throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(getStorageObjectForEmptyObjectWithMetadata(
            ImmutableMap.<String, byte[]>of("foo", new byte[0])));

    // The fetch will "mismatch" with more metadata than our default EMPTY_METADATA used in the
    // default CreateObjectOptions, but we won't care because the metadata-check requirement
    // will be false, so the call will complete successfully.
    gcs.createEmptyObject(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjects() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenReturn(getStorageObjectForEmptyObjectWithMetadata(EMPTY_METADATA));

    gcs.createEmptyObjects(ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    verify(mockStorage, times(2)).objects();
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockStorageObjects).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectsNonIgnorableException() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("forbidden"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(false);
    when(mockErrorExtractor.isInternalServerError(any(IOException.class))).thenReturn(false);

    assertThrows(
        IOException.class,
        () ->
            gcs.createEmptyObjects(
                ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))));

    verify(mockStorage).objects();
    verify(mockStorageObjects)
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockErrorExtractor).isInternalServerError(any(IOException.class));
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectsErrorOnRefetch() throws IOException {
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
            eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert)
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(new IOException("rateLimitExceeded"));
    when(mockErrorExtractor.rateLimited(any(IOException.class))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet)
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenThrow(new RuntimeException("error while fetching"));

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                gcs.createEmptyObjects(
                    ImmutableList.of(
                        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                        new StorageResourceId(BUCKET_NAME, OBJECT_NAME))));
    assertThat(thrown).hasMessageThat().contains("Multiple IOExceptions");

    verify(mockStorage, times(4)).objects();
    verify(mockStorageObjects, times(2))
        .insert(eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert, times(2)).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper, times(2))
        .setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert, times(2)).execute();
    verify(mockErrorExtractor, times(2)).rateLimited(any(IOException.class));
    verify(mockStorageObjects, times(2)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(2)).execute();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectsWithMultipleRetries()
      throws IOException, InterruptedException {
    IOException notFoundException = new IOException("NotFound");
    IOException rateLimitException = new IOException("RateLimited");
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class)))
        .thenReturn(mockStorageObjectsInsert);
    when(mockStorageObjectsInsert.execute())
        .thenThrow(rateLimitException);
    when(mockErrorExtractor.rateLimited(eq(rateLimitException))).thenReturn(true);
    when(mockErrorExtractor.itemNotFound(eq(notFoundException))).thenReturn(true);
    when(mockStorageObjects.get(eq(BUCKET_NAME), eq(OBJECT_NAME)))
        .thenReturn(mockStorageObjectsGet);
    when(mockStorageObjectsGet.execute())
        .thenThrow(notFoundException)
        .thenThrow(notFoundException)
        .thenReturn(getStorageObjectForEmptyObjectWithMetadata(EMPTY_METADATA));

    gcs.createEmptyObjects(ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    verify(mockStorage, times(4)).objects(); // 1 insert, 3 gets
    verify(mockStorageObjects).insert(
        eq(BUCKET_NAME), any(StorageObject.class), any(AbstractInputStreamContent.class));
    verify(mockStorageObjectsInsert).setDisableGZipContent(eq(true));
    verify(mockClientRequestHelper).setDirectUploadEnabled(eq(mockStorageObjectsInsert), eq(true));
    verify(mockStorageObjectsInsert).execute();
    verify(mockErrorExtractor).rateLimited(any(IOException.class));
    verify(mockErrorExtractor, times(2)).itemNotFound(eq(notFoundException));
    verify(mockStorageObjects, times(3)).get(eq(BUCKET_NAME), eq(OBJECT_NAME));
    verify(mockStorageObjectsGet, times(3)).execute();
    verify(mockSleeper, times(2)).sleep(any(Long.class));
  }
}
