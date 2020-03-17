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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.createItemInfoForBucket;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl.createItemInfoForStorageObject;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo.createInferredDirectory;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.HTTP_TRANSPORT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.JSON_FACTORY;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.resumableUploadResponse;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.batchRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.composeRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.copyRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.createBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getMediaRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listBucketsRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadChunkRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.resumableUploadRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.uploadRequestString;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.dataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.emptyResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.inputStreamResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonErrorResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockBatchTransport;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.DateTime;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Buckets;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.ErrorResponses;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.SSLException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for GoogleCloudStorage class. The underlying GCS HTTP requests are mocked, in order to
 * test behavior in response to various types of unexpected exceptions/errors.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageTest {

  private static final String PROJECT_ID = "test-project";
  private static final String BUCKET_NAME = "foo-bucket";
  private static final String OBJECT_NAME = "bar-object";

  private static final ImmutableList<String[]> ILLEGAL_OBJECTS =
      ImmutableList.copyOf(
          new String[][] {
            {null, "bar-object"}, {"foo-bucket", null}, {"", "bar-object"}, {"foo-bucket", ""}
          });
  private static final ImmutableList<StorageResourceId> ILLEGAL_OBJECT_IDS =
      ImmutableList.of(StorageResourceId.ROOT, new StorageResourceId("foo-bucket"));

  private static final GoogleCloudStorageOptions GCS_OPTIONS =
      GoogleCloudStorageOptions.builder()
          .setAppName("gcsio-unit-test")
          .setProjectId(PROJECT_ID)
          .build();

  private static final ImmutableMap<String, byte[]> EMPTY_METADATA = ImmutableMap.of();

  private TrackingHttpRequestInitializer trackingHttpRequestInitializer;

  @Before
  public void setUp() {
    trackingHttpRequestInitializer =
        new TrackingHttpRequestInitializer(/* replaceRequestParams= */ false);
  }

  private static StorageObject getStorageObjectForEmptyObjectWithMetadata(
      Map<String, byte[]> metadata) {
    return newStorageObject(BUCKET_NAME, OBJECT_NAME)
        .setSize(BigInteger.ZERO)
        .setMetadata(metadata == null ? null : GoogleCloudStorageImpl.encodeMetadata(metadata));
  }

  private static GoogleCloudStorageItemInfo getItemInfoForEmptyObjectWithMetadata(
      Map<String, byte[]> metadata) {
    return createItemInfoForStorageObject(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        getStorageObjectForEmptyObjectWithMetadata(metadata));
  }

  @Test
  public void customStorageApiEndpoint() throws Exception {
    GoogleCloudStorageOptions options =
        GoogleCloudStorageOptions.builder()
            .setAppName("testAppName")
            .setProjectId("testProjectId")
            .setStorageRootUrl("https://unit-test-storage.googleapis.com/")
            .build();

    GoogleCloudStorageImpl gcsImpl = new GoogleCloudStorageImpl(options, request -> {});

    assertThat(gcsImpl.gcs.getRootUrl()).isEqualTo("https://unit-test-storage.googleapis.com/");
  }

  /** Test argument sanitization for GoogleCloudStorage.create(2). */
  @Test
  public void testCreateObjectIllegalArguments() {
    GoogleCloudStorage gcs = mockedGcs(HTTP_TRANSPORT);
    ILLEGAL_OBJECT_IDS.forEach(
        resourceId -> assertThrows(IllegalArgumentException.class, () -> gcs.create(resourceId)));
  }

  /** Test successful operation of GoogleCloudStorage.create(2). */
  @Test
  public void testCreateObjectNormalOperation() throws Exception {
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08, 0x09};

    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setSize(BigInteger.valueOf(testData.length))));

    GoogleCloudStorage gcs = mockedGcs(transport);

    try (WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))) {
      assertThat(writeChannel.isOpen()).isTrue();
      writeChannel.write(ByteBuffer.wrap(testData));
    }

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 0, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 1))
        .inOrder();

    HttpRequest chunkUploadRequest = trackingHttpRequestInitializer.getAllRequests().get(2);
    assertThat(chunkUploadRequest.getContent().getLength()).isEqualTo(testData.length);
    try (ByteArrayOutputStream writtenData = new ByteArrayOutputStream(testData.length)) {
      chunkUploadRequest.getContent().writeTo(writtenData);
      assertThat(writtenData.toByteArray()).isEqualTo(testData);
    }
  }

  /** Test successful operation of GoogleCloudStorage.create(2) with generationId. */
  @Test
  public void testCreateObjectWithGenerationId() throws Exception {
    int generationId = 13;
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08, 0x09};

    MockHttpTransport transport =
        mockTransport(
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setSize(BigInteger.valueOf(testData.length))));

    GoogleCloudStorage gcs = mockedGcs(transport);

    try (WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME, generationId))) {
      assertThat(writeChannel.isOpen()).isTrue();
      writeChannel.write(ByteBuffer.wrap(testData));
    }

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, generationId, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 1))
        .inOrder();

    HttpRequest chunkUploadRequest = trackingHttpRequestInitializer.getAllRequests().get(1);
    assertThat(chunkUploadRequest.getContent().getLength()).isEqualTo(testData.length);
    try (ByteArrayOutputStream writtenData = new ByteArrayOutputStream(testData.length)) {
      chunkUploadRequest.getContent().writeTo(writtenData);
      assertThat(writtenData.toByteArray()).isEqualTo(testData);
    }
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(2).
   */
  @Test
  public void testCreateObjectApiIOException() throws IOException {
    trackingHttpRequestInitializer = new TrackingHttpRequestInitializer();

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME)),
            jsonErrorResponse(ErrorResponses.NOT_FOUND));

    GoogleCloudStorage gcs = mockedGcs(transport);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(writeChannel.isOpen()).isTrue();

    IOException thrown = assertThrows(IOException.class, writeChannel::close);
    assertThat(thrown).hasMessageThat().isEqualTo("Upload failed");
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 1, /* replaceGenerationId= */ true))
        .inOrder();
  }

  @Test
  public void reupload_success_singleWrite_singleUploadChunk() throws Exception {
    byte[] testData = new byte[MediaHttpUploader.MINIMUM_CHUNK_SIZE];
    new Random().nextBytes(testData);
    int uploadChunkSize = testData.length * 2;
    int uploadCacheSize = testData.length * 2;

    MockHttpTransport transport =
        mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            jsonErrorResponse(ErrorResponses.GONE),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setSize(BigInteger.valueOf(testData.length))));

    AsyncWriteChannelOptions writeOptions =
        AsyncWriteChannelOptions.builder()
            .setUploadChunkSize(uploadChunkSize)
            .setUploadCacheSize(uploadCacheSize)
            .build();

    GoogleCloudStorage gcs =
        mockedGcs(GCS_OPTIONS.toBuilder().setWriteChannelOptions(writeOptions).build(), transport);

    try (WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))) {
      writeChannel.write(ByteBuffer.wrap(testData));
    }

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 0, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 1),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 0, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 2))
        .inOrder();

    HttpRequest writeRequest = trackingHttpRequestInitializer.getAllRequests().get(4);
    assertThat(writeRequest.getContent().getLength()).isEqualTo(testData.length);
    try (ByteArrayOutputStream writtenData = new ByteArrayOutputStream(testData.length)) {
      writeRequest.getContent().writeTo(writtenData);
      assertThat(writtenData.toByteArray()).isEqualTo(testData);
    }
  }

  @Test
  public void reupload_success_singleWrite_multipleUploadChunks() throws Exception {
    byte[] testData = new byte[2 * MediaHttpUploader.MINIMUM_CHUNK_SIZE];
    new Random().nextBytes(testData);
    int uploadChunkSize = testData.length / 2;
    int uploadCacheSize = testData.length * 2;

    MockHttpTransport transport =
        mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            // "308 Resume Incomplete" - successfully uploaded 1st chunk
            emptyResponse(308).addHeader("Range", "bytes=0-" + (uploadChunkSize - 1)),
            jsonErrorResponse(ErrorResponses.GONE),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            // "308 Resume Incomplete" - successfully uploaded 1st chunk
            emptyResponse(308).addHeader("Range", "bytes=0-" + (uploadChunkSize - 1)),
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setSize(BigInteger.valueOf(testData.length))));

    AsyncWriteChannelOptions writeOptions =
        AsyncWriteChannelOptions.builder()
            .setUploadChunkSize(uploadChunkSize)
            .setUploadCacheSize(uploadCacheSize)
            .build();

    GoogleCloudStorage gcs =
        mockedGcs(GCS_OPTIONS.toBuilder().setWriteChannelOptions(writeOptions).build(), transport);

    try (WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))) {
      writeChannel.write(ByteBuffer.wrap(testData));
    }

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 0, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 1),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 2),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 0, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 3),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 4))
        .inOrder();

    HttpRequest writeRequestChunk1 = trackingHttpRequestInitializer.getAllRequests().get(5);
    assertThat(writeRequestChunk1.getContent().getLength()).isEqualTo(testData.length / 2);
    HttpRequest writeRequestChunk2 = trackingHttpRequestInitializer.getAllRequests().get(6);
    assertThat(writeRequestChunk2.getContent().getLength()).isEqualTo(testData.length / 2);
    try (ByteArrayOutputStream writtenData = new ByteArrayOutputStream(testData.length)) {
      writeRequestChunk1.getContent().writeTo(writtenData);
      writeRequestChunk2.getContent().writeTo(writtenData);
      assertThat(writtenData.toByteArray()).isEqualTo(testData);
    }
  }

  @Test
  public void reupload_success_multipleWrites_singleUploadChunk() throws Exception {
    byte[] testData = new byte[MediaHttpUploader.MINIMUM_CHUNK_SIZE];
    new Random().nextBytes(testData);
    int uploadChunkSize = testData.length * 2;
    int uploadCacheSize = testData.length * 2;

    MockHttpTransport transport =
        mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            jsonErrorResponse(ErrorResponses.GONE),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setSize(BigInteger.valueOf(testData.length))));

    AsyncWriteChannelOptions writeOptions =
        AsyncWriteChannelOptions.builder()
            .setUploadChunkSize(uploadChunkSize)
            .setUploadCacheSize(uploadCacheSize)
            .build();

    GoogleCloudStorage gcs =
        mockedGcs(GCS_OPTIONS.toBuilder().setWriteChannelOptions(writeOptions).build(), transport);

    try (WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))) {
      writeChannel.write(ByteBuffer.wrap(testData));
      writeChannel.write(ByteBuffer.wrap(testData));
    }

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 0, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 1),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 0, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 2))
        .inOrder();

    HttpRequest writeRequest = trackingHttpRequestInitializer.getAllRequests().get(4);
    assertThat(writeRequest.getContent().getLength()).isEqualTo(2 * testData.length);
    try (ByteArrayOutputStream writtenData = new ByteArrayOutputStream(testData.length)) {
      writeRequest.getContent().writeTo(writtenData);
      assertThat(writtenData.toByteArray()).isEqualTo(Bytes.concat(testData, testData));
    }
  }

  @Test
  public void reupload_success_multipleWrites_multipleUploadChunks() throws Exception {
    byte[] testData = new byte[2 * MediaHttpUploader.MINIMUM_CHUNK_SIZE];
    new Random().nextBytes(testData);
    int uploadChunkSize = testData.length / 2;
    int uploadCacheSize = testData.length * 2;

    MockHttpTransport transport =
        mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            // "308 Resume Incomplete" - successfully uploaded 1st chunk
            emptyResponse(308).addHeader("Range", "bytes=0-" + (uploadChunkSize - 1)),
            jsonErrorResponse(ErrorResponses.GONE),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            // "308 Resume Incomplete" - successfully uploaded 3 chunks
            emptyResponse(308).addHeader("Range", "bytes=0-" + (uploadChunkSize - 1)),
            emptyResponse(308).addHeader("Range", "bytes=0-" + (2 * uploadChunkSize - 1)),
            emptyResponse(308).addHeader("Range", "bytes=0-" + (3 * uploadChunkSize - 1)),
            jsonDataResponse(
                newStorageObject(BUCKET_NAME, OBJECT_NAME)
                    .setSize(BigInteger.valueOf(2 * testData.length))));

    AsyncWriteChannelOptions writeOptions =
        AsyncWriteChannelOptions.builder()
            .setUploadChunkSize(uploadChunkSize)
            .setUploadCacheSize(uploadCacheSize)
            .build();

    GoogleCloudStorage gcs =
        mockedGcs(GCS_OPTIONS.toBuilder().setWriteChannelOptions(writeOptions).build(), transport);

    try (WritableByteChannel writeChannel =
        gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))) {
      writeChannel.write(ByteBuffer.wrap(testData));
      writeChannel.write(ByteBuffer.wrap(testData));
    }

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 0, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 1),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 2),
            resumableUploadRequestString(
                BUCKET_NAME, OBJECT_NAME, /* generationId= */ 0, /* replaceGenerationId= */ false),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 3),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 4),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 5),
            resumableUploadChunkRequestString(BUCKET_NAME, OBJECT_NAME, /* uploadId= */ 6))
        .inOrder();

    HttpRequest writeRequestChunk1 = trackingHttpRequestInitializer.getAllRequests().get(5);
    assertThat(writeRequestChunk1.getContent().getLength()).isEqualTo(testData.length / 2);
    HttpRequest writeRequestChunk2 = trackingHttpRequestInitializer.getAllRequests().get(6);
    assertThat(writeRequestChunk2.getContent().getLength()).isEqualTo(testData.length / 2);
    HttpRequest writeRequestChunk3 = trackingHttpRequestInitializer.getAllRequests().get(7);
    assertThat(writeRequestChunk3.getContent().getLength()).isEqualTo(testData.length / 2);
    HttpRequest writeRequestChunk4 = trackingHttpRequestInitializer.getAllRequests().get(8);
    assertThat(writeRequestChunk4.getContent().getLength()).isEqualTo(testData.length / 2);
    try (ByteArrayOutputStream writtenData = new ByteArrayOutputStream(testData.length)) {
      writeRequestChunk1.getContent().writeTo(writtenData);
      writeRequestChunk2.getContent().writeTo(writtenData);
      writeRequestChunk3.getContent().writeTo(writtenData);
      writeRequestChunk4.getContent().writeTo(writtenData);
      assertThat(writtenData.toByteArray()).isEqualTo(Bytes.concat(testData, testData));
    }
  }

  @Test
  public void reupload_failure_cacheTooSmall_singleWrite_singleChunk() throws Exception {
    byte[] testData = new byte[MediaHttpUploader.MINIMUM_CHUNK_SIZE];
    new Random().nextBytes(testData);
    int uploadChunkSize = testData.length;
    int uploadCacheSize = testData.length / 2;

    MockHttpTransport transport =
        mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
            resumableUploadResponse(BUCKET_NAME, OBJECT_NAME),
            jsonErrorResponse(ErrorResponses.GONE));

    AsyncWriteChannelOptions writeOptions =
        AsyncWriteChannelOptions.builder()
            .setUploadChunkSize(uploadChunkSize)
            .setUploadCacheSize(uploadCacheSize)
            .build();

    GoogleCloudStorage gcs =
        mockedGcs(GCS_OPTIONS.toBuilder().setWriteChannelOptions(writeOptions).build(), transport);

    WritableByteChannel writeChannel = gcs.create(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    writeChannel.write(ByteBuffer.wrap(testData));

    IOException writeException = assertThrows(IOException.class, writeChannel::close);

    assertThat(writeException).hasCauseThat().isInstanceOf(GoogleJsonResponseException.class);
    assertThat(writeException).hasCauseThat().hasMessageThat().startsWith("410");
  }

  /** Test successful operation of GoogleCloudStorage.createEmptyObject(1). */
  @Test
  public void testCreateEmptyObject() throws IOException {
    MockHttpTransport transport =
        mockTransport(jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.createEmptyObject(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(uploadRequestString(BUCKET_NAME, OBJECT_NAME, /* generationId= */ null))
        .inOrder();
  }

  /** Test argument sanitization for GoogleCloudStorage.open(2). */
  @Test
  public void testOpenObjectIllegalArguments() {
    GoogleCloudStorage gcs = mockedGcs(HTTP_TRANSPORT);
    ILLEGAL_OBJECT_IDS.forEach(
        resourceId -> assertThrows(IllegalArgumentException.class, () -> gcs.open(resourceId)));
  }

  @Test
  public void testGcsReadChannelCloseIdempotent() throws IOException {
    MockHttpTransport transport =
        mockTransport(jsonDataResponse(newStorageObject(BUCKET_NAME, OBJECT_NAME)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel channel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));

    assertThat(channel.isOpen()).isTrue();

    channel.close();

    assertThat(channel.isOpen()).isFalse();

    channel.close();

    assertThat(channel.isOpen()).isFalse();
    assertThrows(ClosedChannelException.class, channel::position);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  @Test
  public void testOpenWithSomeExceptionsDuringRead() throws Exception {
    InputStream timeoutStream = new ThrowingInputStream(new SocketTimeoutException("read timeout"));
    InputStream sslExceptionStream = new ThrowingInputStream(new SSLException("read SSLException"));
    InputStream ioExceptionStream = new ThrowingInputStream(new IOException("read IOException"));

    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            inputStreamResponse(CONTENT_LENGTH, testData.length, timeoutStream),
            inputStreamResponse(CONTENT_LENGTH, testData.length, sslExceptionStream),
            inputStreamResponse(CONTENT_LENGTH, testData.length, ioExceptionStream),
            dataResponse(testData));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setSleeper(Sleeper.DEFAULT);
    readChannel.setMaxRetries(3);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  @Test
  public void testOpenWithExceptionDuringReadAndCloseForRetry() throws Exception {
    InputStream failedStream =
        new ThrowingInputStream(
            new SSLException("read SSLException"), new SSLException("close SSLException"));
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            inputStreamResponse(CONTENT_LENGTH, testData.length, failedStream),
            dataResponse(testData));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setMaxRetries(1);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  @Test
  public void testClosesWithRuntimeExceptionDuringReadAndClose() throws IOException {
    InputStream failedStream =
        new ThrowingInputStream(
            new RuntimeException("read RuntimeException"),
            new RuntimeException("close RuntimeException"));

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject), inputStreamResponse(CONTENT_LENGTH, 1, failedStream));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> readChannel.read(ByteBuffer.allocate(1)));

    assertThat(thrown).hasMessageThat().contains("read RuntimeException");
    assertThat(readChannel.contentChannel).isNull();
    assertThat(readChannel.contentChannelPosition).isEqualTo(-1);

    assertThat(readChannel.isOpen()).isTrue();
    // TODO: modify readChannel.close() to throw underlying channel exception on close
    readChannel.close();

    assertThat(readChannel.isOpen()).isFalse();

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  @Test
  public void testCloseWithExceptionDuringClose() throws IOException {
    InputStream failedStream =
        new ThrowingInputStream(/* readException= */ null, new SSLException("close SSLException"));

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject), inputStreamResponse(CONTENT_LENGTH, 1, failedStream));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    readChannel.performLazySeek(/* bytesToRead= */ 1);
    assertThat(readChannel.contentChannel).isNotNull();

    // Should not throw exception. If it does, it will be caught by the test harness.
    // TODO: modify readChannel.close() to throw underlying channel exception on close
    readChannel.close();

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  @Test
  public void testOpenAndReadWithPrematureEndOfStreamRetriesFail() throws Exception {
    // We'll claim a Content-Length of testData.length, but then only return a stream containing
    // truncatedData. The channel should throw an exception upon detecting this premature
    // end-of-stream.
    int testLength = 5;
    byte[] truncatedData = {0x01, 0x02, 0x03};
    byte[] truncatedRetryData = {0x11};

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            // First time: Claim  we'll provide 5 bytes, but only give 3.
            inputStreamResponse(
                CONTENT_LENGTH, testLength, new ByteArrayInputStream(truncatedData)),
            // Second time: Claim we'll provide the 2 remaining bytes, but only give one byte.
            // This retry counts toward the maxRetries of the "first" attempt, but the nonzero bytes
            // returned resets the counter; when this ends prematurely we'll expect yet another
            // "retry"
            // even though we'll set maxRetries == 1.
            inputStreamResponse(CONTENT_LENGTH, 2, new ByteArrayInputStream(truncatedRetryData)),
            // Third time, we claim we'll deliver the one remaining byte, but give none. Since no
            // progress is made, the retry counter does not get reset and we've exhausted all
            // retries.
            inputStreamResponse(CONTENT_LENGTH, 1, new ByteArrayInputStream(new byte[0])));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    // Only allow one retry for this test.
    readChannel.setMaxRetries(1);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    byte[] actualData = new byte[testLength];

    assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.wrap(actualData)));

    assertThat(actualData).isEqualTo(new byte[] {0x01, 0x02, 0x03, 0x11, 0x00});

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  @Test
  public void testOpenAndReadWithPrematureEndOfStreamRetriesSucceed() throws Exception {
    // We'll claim a Content-Length of testData.length, but then only return a stream containing
    // firstReadData. The channel should throw an exception upon detecting this premature
    // end-of-stream.
    byte[] testData = {0x01, 0x02, 0x03, 0x11, 0x21};
    byte[] firstReadData = {0x01, 0x02, 0x03};
    byte[] secondReadData = {0x11};
    byte[] thirdReadData = {0x21};

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            // First time: Claim  we'll provide 5 bytes, but only give 3.
            inputStreamResponse(
                CONTENT_LENGTH, testData.length, new ByteArrayInputStream(firstReadData)),
            // Second time: Claim we'll provide the 2 remaining bytes, but only give one byte.
            //         This retry counts toward the maxRetries of the "first" attempt, but the
            // nonzero bytes
            // returned resets the counter; when this ends prematurely we'll expect yet another
            // "retry"
            // even though we'll set maxRetries == 1.
            inputStreamResponse(CONTENT_LENGTH, 2, new ByteArrayInputStream(secondReadData)),
            // Third time, we claim we'll deliver the one remaining byte, but give none. Since no
            // progress is made, the retry counter does not get reset and we've exhausted all
            // retries.
            inputStreamResponse(CONTENT_LENGTH, 1, new ByteArrayInputStream(thirdReadData)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  @Test
  public void testOpenExceptionsDuringReadTotalElapsedTimeTooGreat() throws Exception {
    IOException readException1 = new IOException("read IOException #1");
    IOException readException2 = new IOException("read IOException #2");

    NanoClock spyNanoClock = spy(NanoClock.class);

    when(spyNanoClock.nanoTime())
        .thenReturn(
            Duration.ofMillis(1).toNanos(),
            Duration.ofMillis(2).toNanos(),
            Duration.ofMillis(3).toNanos(),
            Duration.ofMillis(3).plusMillis(DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS).toNanos());

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            inputStreamResponse(CONTENT_LENGTH, 1, new ThrowingInputStream(readException1)),
            inputStreamResponse(CONTENT_LENGTH, 1, new ThrowingInputStream(readException2)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setNanoClock(spyNanoClock);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    IOException thrown =
        assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
    assertThat(thrown).hasMessageThat().isEqualTo("read IOException #2");

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  @Test
  public void testOpenExceptionsDuringReadInterruptedDuringSleep() throws Exception {
    Sleeper spySleeper = spy(Sleeper.class);

    InterruptedException sleepException = new InterruptedException("sleep InterruptedException");

    doThrow(sleepException).when(spySleeper).sleep(anyLong());

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            inputStreamResponse(
                CONTENT_LENGTH, 1, new ThrowingInputStream(new IOException("read IOException"))));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setSleeper(spySleeper);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    IOException thrown =
        assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
    assertThat(thrown).hasMessageThat().isEqualTo("read IOException");

    assertThat(thrown.getSuppressed()).isEqualTo(new Throwable[] {sleepException});
    assertThat(Thread.interrupted()).isTrue();

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  @Test
  public void testOpenTooManyExceptionsDuringRead() throws Exception {
    InputStream timeoutStream = new ThrowingInputStream(new SocketTimeoutException("read timeout"));
    InputStream sslExceptionStream = new ThrowingInputStream(new SSLException("read SSLException"));
    IOException readIOException = new IOException("read IOException");

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            inputStreamResponse(CONTENT_LENGTH, 1, timeoutStream),
            inputStreamResponse(CONTENT_LENGTH, 1, sslExceptionStream),
            inputStreamResponse(CONTENT_LENGTH, 1, new ThrowingInputStream(readIOException)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setMaxRetries(2);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    IOException thrown =
        assertThrows(IOException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
    assertThat(thrown).isSameInstanceAs(readIOException);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  @Test
  public void testOpenTwoTimeoutsWithIntermittentProgress() throws Exception {
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};
    byte[] testData2 = {0x05, 0x08};

    InputStream timeoutStream =
        new ThrowingInputStream(new SocketTimeoutException("read timeout #1"));

    InputStream intermittentProgressTimeoutStream =
        new InputStream() {
          // Return -1 value from time to time to simulate intermittent read progress
          final int[] readData = {testData[0], testData[1], -1, testData[2], -1};
          int readDataIndex = 0;

          @Override
          public int available() {
            return 1;
          }

          @Override
          public int read() throws IOException {
            assertThat(readDataIndex).isAtMost(readData.length);
            // throw SocketTimeoutException after all bytes were read
            if (readData.length == readDataIndex) {
              readDataIndex++;
              throw new SocketTimeoutException("read timeout #2");
            }
            return readData[readDataIndex++];
          }
        };

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            inputStreamResponse(CONTENT_LENGTH, testData.length, timeoutStream),
            inputStreamResponse(CONTENT_LENGTH, testData.length, intermittentProgressTimeoutStream),
            inputStreamResponse(
                CONTENT_LENGTH, testData.length, new ByteArrayInputStream(testData2)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setMaxRetries(1);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    // Should succeed even though, in total, there were more retries than maxRetries, since we
    // made progress between errors.
    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(readChannel.position()).isEqualTo(5);
    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  /** Test successful operation of GoogleCloudStorage.open(2) with Content-Encoding: gzip files. */
  @Test
  public void testOpenGzippedObjectNormalOperation() throws IOException {
    byte[] testData = new byte[1024];
    new Random().nextBytes(testData);
    byte[] compressedData = gzip(testData);

    Map<String, Object> responseHeaders =
        ImmutableMap.of(CONTENT_LENGTH, compressedData.length, "Content-Encoding", "gzip");

    StorageObject storageObject =
        newStorageObject(BUCKET_NAME, OBJECT_NAME)
            .setSize(BigInteger.valueOf(compressedData.length))
            .setContentEncoding("gzip");

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            dataResponse(responseHeaders, compressedData),
            dataResponse(responseHeaders, compressedData),
            dataResponse(responseHeaders, compressedData));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);
    assertThat(readChannel.size()).isEqualTo(Long.MAX_VALUE);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(readChannel.size()).isEqualTo(Long.MAX_VALUE);
    assertThat(readChannel.contentChannel).isNotNull();
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
    bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(partialData.length);
    assertThat(readChannel.size()).isEqualTo(Long.MAX_VALUE);
    assertThat(actualData).isEqualTo(partialData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    readChannel.position(testData.length / 2);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(testData.length / 2);

    partialData = Arrays.copyOfRange(testData, testData.length / 2, testData.length);
    actualData = new byte[partialData.length];
    bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(partialData.length);
    assertThat(readChannel.size()).isEqualTo(Long.MAX_VALUE);
    assertThat(actualData).isEqualTo(partialData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    partialData = new byte[1];
    actualData = new byte[partialData.length];
    bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(-1);
    assertThat(readChannel.size()).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(partialData);
    assertThat(readChannel.position()).isEqualTo(testData.length);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  /**
   * If we disable the supportGzipEncoding option when opening a channel, and disable failing fast
   * on nonexistent objects we should expect no extraneous metadata-GET calls at all.
   */
  @Test
  public void testOpenNoSupportGzipEncodingAndNoFailFastOnNotFound() throws Exception {
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};

    MockHttpTransport transport = mockTransport(dataResponse(testData));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setSupportGzipEncoding(false)
            .setFastFailOnNotFound(false)
            .build();

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME), readOptions);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getMediaRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  /** Test in-place forward seeks smaller than seek buffer, smaller than limit. */
  @Test
  public void testInplaceSeekSmallerThanSeekLimit() throws Exception {
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};

    MockHttpTransport transport = mockTransport(dataResponse(testData));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                GoogleCloudStorageReadOptions.builder()
                    .setFastFailOnNotFound(false)
                    .setInplaceSeekLimit(2)
                    .build());

    byte[] actualData = new byte[1];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(1);
    assertThat(actualData).isEqualTo(new byte[] {0x01});

    // Jump 2 bytes forwards; this should be done in-place without any new executeMedia() call.
    readChannel.position(3);
    assertThat(readChannel.position()).isEqualTo(3);

    actualData = new byte[2];
    bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(2);
    assertThat(actualData).isEqualTo(new byte[] {0x05, 0x08});

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getMediaRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  /** Test in-place forward seeks larger than seek buffer but smaller than limit. */
  @Test
  public void testInplaceSeekLargerThanSeekBuffer() throws Exception {
    byte[] testData = new byte[2 * GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE];
    new Random().nextBytes(testData);

    MockHttpTransport transport = mockTransport(dataResponse(testData));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setFastFailOnNotFound(false)
            .setInplaceSeekLimit(testData.length)
            .build();

    SeekableByteChannel readChannel =
        gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME), readOptions);

    byte[] actualData = new byte[1];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(1);
    assertThat(actualData).isEqualTo(new byte[] {testData[0]});

    // Jump SKIP_BUFFER_SIZE + 3 bytes forwards; this should be done in-place without any
    // new executeMedia() call.
    int jumpPosition = GoogleCloudStorageReadChannel.SKIP_BUFFER_SIZE + 3;
    readChannel.position(jumpPosition);
    assertThat(readChannel.position()).isEqualTo(jumpPosition);

    actualData = new byte[2];
    bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(2);
    assertThat(actualData)
        .isEqualTo(new byte[] {testData[jumpPosition], testData[jumpPosition + 1]});

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getMediaRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  /** Test in-place backward seek does not trigger a re-read if it isn't used */
  @Test
  public void testUnusedBackwardSeekIgnored() throws Exception {
    byte[] testData = {0x01, 0x02, 0x03, 0x05, 0x08};

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject), dataResponse(testData), dataResponse(testData));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel)
            gcs.open(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(2).build());

    byte[] actualData = new byte[1];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(readChannel.position()).isEqualTo(1);
    assertThat(bytesRead).isEqualTo(1);
    assertThat(actualData).isEqualTo(new byte[] {0x01});

    // Position back to 0, but don't read anything. Verify position returned to caller.
    readChannel.position(0);
    assertThat(readChannel.position()).isEqualTo(0);

    // Re-position forward before the next read. No new executeMedia calls.
    readChannel.position(3);
    assertThat(readChannel.position()).isEqualTo(3);

    actualData = new byte[2];
    bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(readChannel.position()).isEqualTo(5);
    assertThat(bytesRead).isEqualTo(2);
    assertThat(actualData).isEqualTo(new byte[] {0x05, 0x08});

    // Position back to 0, and read. Should lead to another executeMedia call.
    readChannel.position(0);

    actualData = new byte[2];
    bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(readChannel.position()).isEqualTo(2);
    assertThat(bytesRead).isEqualTo(2);
    assertThat(actualData).isEqualTo(new byte[] {0x01, 0x02});

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  /**
   * Test operation of GoogleCloudStorage.open(2) with Content-Encoding: gzip files when exceptions
   * occur during reading.
   */
  @Test
  public void testOpenGzippedObjectExceptionsDuringRead() throws Exception {
    byte[] testData = new byte[1024];
    new Random().nextBytes(testData);
    byte[] compressedData = gzip(testData);

    Map<String, Object> responseHeaders =
        ImmutableMap.of(CONTENT_LENGTH, compressedData.length, "Content-Encoding", "gzip");

    StorageObject storageObject =
        newStorageObject(BUCKET_NAME, OBJECT_NAME)
            .setSize(BigInteger.valueOf(compressedData.length))
            .setContentEncoding("gzip");

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject),
            inputStreamResponse(
                responseHeaders,
                partialReadTimeoutStream(
                    compressedData, /* readFraction= */ 0.25, "read timeout #1")),
            inputStreamResponse(
                responseHeaders,
                partialReadTimeoutStream(
                    compressedData, /* readFraction= */ 0.75, "read timeout #2")),
            inputStreamResponse(responseHeaders, new ByteArrayInputStream(compressedData)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    readChannel.setReadBackOff(BackOff.ZERO_BACKOFF);
    readChannel.setMaxRetries(1);
    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(readChannel.position()).isEqualTo(testData.length);
    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  /** Test successful operation of GoogleCloudStorage.open(2). */
  @Test
  public void testOpenObjectNormalOperation() throws IOException {
    byte[] testData = {0x01, 0x02, 0x11, 0x12, 0x13};
    byte[] testData2 = {0x11, 0x12, 0x13};

    StorageObject storageObject =
        newStorageObject(BUCKET_NAME, OBJECT_NAME).setSize(BigInteger.valueOf(testData.length));

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject), dataResponse(testData), dataResponse(testData2));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageReadChannel readChannel =
        (GoogleCloudStorageReadChannel) gcs.open(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));

    assertThat(readChannel.isOpen()).isTrue();
    assertThat(readChannel.position()).isEqualTo(0);

    byte[] actualData = new byte[testData.length];
    int bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(testData.length);
    assertThat(actualData).isEqualTo(testData);
    assertThat(readChannel.contentChannel).isNotNull();
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

    actualData = new byte[testData2.length];
    bytesRead = readChannel.read(ByteBuffer.wrap(actualData));

    assertThat(bytesRead).isEqualTo(testData2.length);
    assertThat(actualData).isEqualTo(testData2);

    // Note that position will be testData.length, *not* testData2.length (5, not 3).
    assertThat(readChannel.position()).isEqualTo(testData.length);

    readChannel.close();
    assertThat(readChannel.isOpen()).isFalse();

    // After closing the channel, future reads should throw a ClosedChannelException.
    assertThrows(ClosedChannelException.class, () -> readChannel.read(ByteBuffer.allocate(1)));
    assertThrows(ClosedChannelException.class, () -> readChannel.position(0));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject.getGeneration()))
        .inOrder();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.open(2).
   */
  @Test
  public void testOpenObjectApiException() throws IOException {
    StorageObject storageObject1 = newStorageObject(BUCKET_NAME, OBJECT_NAME);
    StorageObject storageObject2 = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            jsonDataResponse(storageObject1),
            jsonErrorResponse(ErrorResponses.RANGE_NOT_SATISFIABLE),
            jsonDataResponse(storageObject2),
            jsonErrorResponse(ErrorResponses.GONE));

    GoogleCloudStorage gcs = mockedGcs(transport);

    StorageResourceId objectId = new StorageResourceId(BUCKET_NAME, OBJECT_NAME);

    // First time is the notFoundException.
    assertThrows(FileNotFoundException.class, () -> gcs.open(objectId));

    // Second time is the rangeNotSatisfiableException.
    SeekableByteChannel readChannel2 = gcs.open(objectId);
    EOFException thrown2 =
        assertThrows(EOFException.class, () -> readChannel2.read(ByteBuffer.allocate(1)));

    String expectedErrorMessage = ErrorResponses.RANGE_NOT_SATISFIABLE.getErrorMessage();
    assertThat(thrown2).hasCauseThat().hasMessageThat().contains(expectedErrorMessage);

    // Third time is the unexpectedException.
    SeekableByteChannel readChannel3 = gcs.open(objectId);
    IOException thrown3 =
        assertThrows(IOException.class, () -> readChannel3.read(ByteBuffer.allocate(1)));
    assertThat(thrown3)
        .hasMessageThat()
        .isEqualTo(
            String.format("Error reading 'gs://%s/%s' at position 0", BUCKET_NAME, OBJECT_NAME));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject1.getGeneration()),
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getMediaRequestString(BUCKET_NAME, OBJECT_NAME, storageObject2.getGeneration()))
        .inOrder();
  }

  /** Test argument sanitization for GoogleCloudStorage.create(String). */
  @Test
  public void testCreateBucketIllegalArguments() {
    GoogleCloudStorage gcs = mockedGcs(HTTP_TRANSPORT);
    assertThrows(IllegalArgumentException.class, () -> gcs.create((String) null));
    assertThrows(IllegalArgumentException.class, () -> gcs.create(""));
  }

  /** Test successful operation of GoogleCloudStorage.create(String). */
  @Test
  public void testCreateBucketNormalOperation() throws IOException {
    MockHttpTransport transport = mockTransport(jsonDataResponse(newBucket(BUCKET_NAME)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.create(BUCKET_NAME);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(createBucketRequestString(PROJECT_ID))
        .inOrder();
  }

  /** Test successful operation of GoogleCloudStorage.create(String, CreateBucketOptions). */
  @Test
  public void testCreateBucketWithOptionsNormalOperation() throws IOException {
    MockHttpTransport transport = mockTransport(jsonDataResponse(newBucket(BUCKET_NAME)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    String location = "some-location";
    String storageClass = "storage-class";
    CreateBucketOptions bucketOptions =
        CreateBucketOptions.builder().setLocation(location).setStorageClass(storageClass).build();

    gcs.create(BUCKET_NAME, bucketOptions);

    assertThat(bucketOptions.getLocation()).isEqualTo(location);
    assertThat(bucketOptions.getStorageClass()).isEqualTo(storageClass);
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(createBucketRequestString(PROJECT_ID))
        .inOrder();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.create(String).
   */
  @Test
  public void testCreateBucketApiException() throws Exception {
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.GONE));

    GoogleCloudStorage gcs = mockedGcs(transport);

    // TODO(user): Switch to testing for FileExistsException once implemented.
    IOException exception = assertThrows(IOException.class, () -> gcs.create(BUCKET_NAME));
    assertThat(exception)
        .hasMessageThat()
        .contains("\"code\" : " + ErrorResponses.GONE.getErrorCode());

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(createBucketRequestString(PROJECT_ID))
        .inOrder();
  }

  /** Test handling of rate-limiting and back-off in GoogleCloudStorage.create(String). */
  @Test
  public void testCreateBucketRateLimited() throws Exception {
    Bucket bucket = newBucket(BUCKET_NAME);
    MockHttpTransport transport =
        mockTransport(jsonErrorResponse(ErrorResponses.RATE_LIMITED), jsonDataResponse(bucket));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.create(BUCKET_NAME);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            createBucketRequestString(PROJECT_ID), createBucketRequestString(PROJECT_ID))
        .inOrder();
  }

  /** Test argument sanitization for GoogleCloudStorage.delete(1). */
  @Test
  public void testDeleteBucketIllegalArguments() {
    GoogleCloudStorage gcs = mockedGcs(HTTP_TRANSPORT);
    assertThrows(
        IllegalArgumentException.class, () -> gcs.deleteBuckets(Lists.newArrayList((String) null)));
    assertThrows(IllegalArgumentException.class, () -> gcs.deleteBuckets(Lists.newArrayList("")));
  }

  /** Test successful operation of GoogleCloudStorage.delete(1). */
  @Test
  public void testDeleteBucketNormalOperation() throws IOException {
    MockHttpTransport transport =
        mockTransport(emptyResponse(HttpStatusCodes.STATUS_CODE_NO_CONTENT));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.deleteBuckets(ImmutableList.of(BUCKET_NAME));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(deleteBucketRequestString(BUCKET_NAME))
        .inOrder();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.delete(1).
   */
  @Test
  public void testDeleteBucketApiException() throws Exception {
    String bucket1 = BUCKET_NAME + 1;
    String bucket2 = BUCKET_NAME + 2;

    MockHttpTransport transport =
        mockTransport(
            emptyResponse(HttpStatusCodes.STATUS_CODE_SERVER_ERROR),
            jsonErrorResponse(ErrorResponses.NOT_FOUND));

    GoogleCloudStorage gcs = mockedGcs(transport);

    assertThrows(IOException.class, () -> gcs.deleteBuckets(ImmutableList.of(bucket1)));
    assertThrows(FileNotFoundException.class, () -> gcs.deleteBuckets(ImmutableList.of(bucket2)));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(deleteBucketRequestString(bucket1), deleteBucketRequestString(bucket2))
        .inOrder();
  }

  /** Test handling of rate-limiting and back-off in GoogleCloudStorage.delete(1). */
  @Test
  public void testDeleteBucketRateLimited() throws Exception {
    Bucket bucket = newBucket(BUCKET_NAME);
    MockHttpTransport transport =
        mockTransport(jsonErrorResponse(ErrorResponses.RATE_LIMITED), jsonDataResponse(bucket));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.deleteBuckets(ImmutableList.of(BUCKET_NAME));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            deleteBucketRequestString(BUCKET_NAME), deleteBucketRequestString(BUCKET_NAME))
        .inOrder();
  }

  /** Test argument sanitization for GoogleCloudStorage.delete(2). */
  @Test
  public void testDeleteObjectIllegalArguments() {
    GoogleCloudStorage gcs = mockedGcs(HTTP_TRANSPORT);
    ILLEGAL_OBJECT_IDS.forEach(
        resourceId ->
            assertThrows(
                IllegalArgumentException.class,
                () -> gcs.deleteObjects(ImmutableList.of(resourceId))));
  }

  /** Test successful operation of GoogleCloudStorage.delete(2). */
  @Test
  public void testDeleteObjectNormalOperation() throws IOException {
    trackingHttpRequestInitializer = new TrackingHttpRequestInitializer();

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(storageObject), emptyResponse(HttpStatusCodes.STATUS_CODE_NO_CONTENT));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.deleteObjects(Lists.newArrayList(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            deleteRequestString(BUCKET_NAME, OBJECT_NAME, /* generationId= */ 1))
        .inOrder();
  }

  /** Test successful operation of GoogleCloudStorage.delete(2) with generationId. */
  @Test
  public void testDeleteObjectWithGenerationId() throws IOException {
    int generationId = 65;

    MockHttpTransport transport =
        mockTransport(emptyResponse(HttpStatusCodes.STATUS_CODE_NO_CONTENT));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.deleteObjects(
        ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME, generationId)));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            deleteRequestString(
                BUCKET_NAME, OBJECT_NAME, generationId, /* replaceGenerationId= */ false))
        .inOrder();
  }

  /** Test argument sanitization for GoogleCloudStorage.copy(4). */
  @Test
  public void testCopyObjectsIllegalArguments() {
    String b = BUCKET_NAME;
    List<String> o = ImmutableList.of(OBJECT_NAME);

    GoogleCloudStorage gcs = mockedGcs(HTTP_TRANSPORT);

    ILLEGAL_OBJECTS.forEach(
        objectPair -> {
          String badBucket = objectPair[0];
          List<String> badObject = Collections.singletonList(objectPair[1]);
          // Src is bad.
          assertThrows(IllegalArgumentException.class, () -> gcs.copy(badBucket, badObject, b, o));
          // Dst is bad.
          assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, o, badBucket, badObject));
        });

    // Failure if src == dst.
    assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, o, b, o));

    // Null lists.
    assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, null, b, o));
    assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, o, b, null));

    // Mismatched number of objects.
    List<String> objects = ImmutableList.of(OBJECT_NAME + "1", OBJECT_NAME + "2");
    assertThrows(IllegalArgumentException.class, () -> gcs.copy(b, o, b, objects));
  }

  /**
   * Test successful operation of GoogleCloudStorage.copy(4) where srcBucketName == dstBucketName.
   */
  @Test
  public void testCopyObjectsNormalOperationSameBucket() throws IOException {
    String dstObject = OBJECT_NAME + "-copy";
    StorageObject object = newStorageObject(BUCKET_NAME, dstObject);
    MockHttpTransport transport =
        mockTransport(jsonDataResponse(new Objects().setItems(ImmutableList.of(object))));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME), BUCKET_NAME, ImmutableList.of(dstObject));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            copyRequestString(BUCKET_NAME, OBJECT_NAME, BUCKET_NAME, dstObject, "copyTo"))
        .inOrder();
  }

  /**
   * Test successful operation of GoogleCloudStorage.copy(4) where srcBucketName != dstBucketName.
   */
  @Test
  public void testCopyObjectsNormalOperationDifferentBucket() throws IOException {
    String dstBucket = BUCKET_NAME + "-copy";
    String dstObject = OBJECT_NAME + "-copy";

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(newBucket(BUCKET_NAME)),
            jsonDataResponse(newBucket(dstBucket)),
            dataResponse("{\"done\": true}".getBytes(StandardCharsets.UTF_8)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.copy(BUCKET_NAME, ImmutableList.of(OBJECT_NAME), dstBucket, ImmutableList.of(dstObject));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(dstBucket),
            copyRequestString(BUCKET_NAME, OBJECT_NAME, dstBucket, dstObject, "copyTo"))
        .inOrder();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.copy(4) where srcBucketName != dstBucketName.
   */
  @Test
  public void testCopyObjectsApiExceptionDifferentBucket() throws IOException {
    String dstObjectName = OBJECT_NAME + "-copy";
    String dstBucketName = BUCKET_NAME + "-copy";

    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            jsonErrorResponse(ErrorResponses.GONE),
            jsonDataResponse(newBucket(BUCKET_NAME)),
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            jsonDataResponse(newBucket(BUCKET_NAME)),
            jsonErrorResponse(ErrorResponses.GONE));

    GoogleCloudStorage gcs = mockedGcs(transport);

    // Order of exceptions:
    // 1. Src 404
    ImmutableList<String> srcObjectNames = ImmutableList.of(OBJECT_NAME);
    ImmutableList<String> dstObjectNames = ImmutableList.of(dstObjectName);
    FileNotFoundException thrownSrcFileNotFoundException =
        assertThrows(
            FileNotFoundException.class,
            () -> gcs.copy(BUCKET_NAME, srcObjectNames, dstBucketName, dstObjectNames));
    assertThat(thrownSrcFileNotFoundException).hasMessageThat().contains(BUCKET_NAME);

    // 2. Src unexpected error
    IOException srcIOException =
        assertThrows(
            IOException.class,
            () -> gcs.copy(BUCKET_NAME, srcObjectNames, dstBucketName, dstObjectNames));
    assertThat(srcIOException).hasMessageThat().isEqualTo("Error accessing Bucket " + BUCKET_NAME);

    // 3. Dst 404
    FileNotFoundException dstFileNotFoundException =
        assertThrows(
            FileNotFoundException.class,
            () -> gcs.copy(BUCKET_NAME, srcObjectNames, dstBucketName, dstObjectNames));
    assertThat(dstFileNotFoundException).hasMessageThat().contains(dstBucketName);

    // 4. Dst unexpected error
    IOException dstIOException =
        assertThrows(
            IOException.class,
            () -> gcs.copy(BUCKET_NAME, srcObjectNames, dstBucketName, dstObjectNames));
    assertThat(dstIOException)
        .hasMessageThat()
        .isEqualTo("Error accessing Bucket " + dstBucketName);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(dstBucketName),
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(dstBucketName))
        .inOrder();
  }

  /**
   * Test behavior of GoogleCloudStorage.copy(4) where srcBucketName != dstBucketName and the
   * retrieved bucket metadata indicates they are not compatible for copying.
   */
  @Test
  public void testCopyObjectsIncompatibleBuckets() throws IOException {
    List<String> srcObject = ImmutableList.of(OBJECT_NAME);
    String dstBucket = BUCKET_NAME + "-copy";
    List<String> dstObject = ImmutableList.of(OBJECT_NAME + "-copy");

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(newBucket(BUCKET_NAME).setLocation("us-east-incomp")),
            jsonDataResponse(newBucket(dstBucket)),
            jsonDataResponse(newBucket(BUCKET_NAME).setStorageClass("class-be2-incomp")),
            jsonDataResponse(newBucket(dstBucket)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    UnsupportedOperationException locationException =
        assertThrows(
            UnsupportedOperationException.class,
            () -> gcs.copy(BUCKET_NAME, srcObject, dstBucket, dstObject));
    assertThat(locationException).hasMessageThat().contains("not supported");
    assertThat(locationException).hasMessageThat().contains("storage location");

    UnsupportedOperationException storageClassException =
        assertThrows(
            UnsupportedOperationException.class,
            () -> gcs.copy(BUCKET_NAME, srcObject, dstBucket, dstObject));
    assertThat(storageClassException).hasMessageThat().contains("not supported");
    assertThat(storageClassException).hasMessageThat().contains("storage class");

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(dstBucket),
            getBucketRequestString(BUCKET_NAME),
            getBucketRequestString(dstBucket))
        .inOrder();
  }

  /** Test for GoogleCloudStorage.listBucketNames(0). */
  @Test
  public void testListBucketNames() throws IOException {
    List<Bucket> buckets =
        ImmutableList.of(newBucket("bucket0"), newBucket("bucket1"), newBucket("bucket2"));

    MockHttpTransport transport = mockTransport(jsonDataResponse(new Buckets().setItems(buckets)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    List<String> bucketNames = gcs.listBucketNames();

    assertThat(bucketNames).containsExactly("bucket0", "bucket1", "bucket2").inOrder();
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(listBucketsRequestString(PROJECT_ID))
        .inOrder();
  }

  /** Test for GoogleCloudStorage.listBucketInfo(0). */
  @Test
  public void testListBucketInfo() throws IOException {
    List<Bucket> buckets =
        ImmutableList.of(newBucket("bucket0"), newBucket("bucket1"), newBucket("bucket2"));

    MockHttpTransport transport = mockTransport(jsonDataResponse(new Buckets().setItems(buckets)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    List<GoogleCloudStorageItemInfo> bucketInfos = gcs.listBucketInfo();

    assertThat(bucketInfos)
        .containsExactlyElementsIn(
            buckets.stream()
                .map(b -> createItemInfoForBucket(new StorageResourceId(b.getName()), b))
                .toArray())
        .inOrder();
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(listBucketsRequestString(PROJECT_ID))
        .inOrder();
  }

  /** Test successful operation of GoogleCloudStorage.listObjectNames(3). */
  @Test
  public void testListObjectNamesPrefix() throws IOException {
    String prefix = "foo/bar/baz/";
    String pageToken = "pageToken_0";

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                new Objects()
                    .setPrefixes(ImmutableList.of("foo/bar/baz/dir0/", "foo/bar/baz/dir1/"))
                    .setNextPageToken(pageToken)),
            jsonDataResponse(
                new Objects()
                    .setItems(
                        ImmutableList.of(
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/"),
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/obj0"),
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/obj1")))
                    .setNextPageToken(null)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    List<String> objectNames = gcs.listObjectNames(BUCKET_NAME, prefix, PATH_DELIMITER);

    assertThat(objectNames)
        .containsExactly(
            "foo/bar/baz/dir0/", "foo/bar/baz/dir1/", "foo/bar/baz/obj0", "foo/bar/baz/obj1")
        .inOrder();
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(BUCKET_NAME, prefix, /* maxResults= */ 1024, /* pageToken= */ null),
            listRequestString(BUCKET_NAME, prefix, /* maxResults= */ 1024, pageToken))
        .inOrder();
  }

  /** Test GoogleCloudStorage.listObjectNames(3) with maxResults set. */
  @Test
  public void testListObjectNamesPrefixLimited() throws IOException {
    String prefix = "foo/bar/baz/";
    int maxResults = 3;

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                new Objects()
                    .setPrefixes(ImmutableList.of("foo/bar/baz/dir0/", "foo/bar/baz/dir1/"))
                    .setItems(
                        ImmutableList.of(
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/"),
                            newStorageObject(BUCKET_NAME, "foo/bar/baz/obj0")))
                    .setNextPageToken("pageToken0")));

    GoogleCloudStorage gcs = mockedGcs(transport);

    List<String> objectNames = gcs.listObjectNames(BUCKET_NAME, prefix, PATH_DELIMITER, maxResults);

    assertThat(objectNames)
        .containsExactly("foo/bar/baz/dir0/", "foo/bar/baz/dir1/", "foo/bar/baz/obj0")
        .inOrder();
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(BUCKET_NAME, prefix, maxResults + 1, /* pageToken= */ null))
        .inOrder();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.listObjectNames(3).
   */
  @Test
  public void testListObjectNamesPrefixApiException() throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";

    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND), jsonErrorResponse(ErrorResponses.GONE));

    GoogleCloudStorage gcs = mockedGcs(transport);

    // First time should just return empty list.
    List<String> objectNames = gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter);
    assertThat(objectNames).isEmpty();

    // Second time throws.
    IOException thrown =
        assertThrows(
            IOException.class, () -> gcs.listObjectNames(BUCKET_NAME, objectPrefix, delimiter));

    String expectedErrorMessage = "\"code\" : " + ErrorResponses.GONE.getErrorCode();
    assertThat(thrown).hasCauseThat().hasMessageThat().contains(expectedErrorMessage);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                BUCKET_NAME,
                /* includeTrailingDelimiter= */ false,
                objectPrefix,
                /* maxResults= */ 1024,
                /* pageToken= */ null),
            listRequestString(
                BUCKET_NAME,
                /* includeTrailingDelimiter= */ false,
                objectPrefix,
                /* maxResults= */ 1024,
                /* pageToken= */ null))
        .inOrder();
  }

  @Test
  public void testListObjectInfoBasic() throws IOException {
    String prefix = "foo/bar/baz/";

    StorageObject object1 = newStorageObject(BUCKET_NAME, "foo/bar/baz/obj0");
    StorageObject object2 = newStorageObject(BUCKET_NAME, "foo/bar/baz/obj1");

    List<StorageObject> objects =
        ImmutableList.of(newStorageObject(BUCKET_NAME, "foo/bar/baz/"), object1, object2);

    MockHttpTransport transport =
        mockTransport(jsonDataResponse(new Objects().setItems(objects).setNextPageToken(null)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, prefix, PATH_DELIMITER);

    // The item exactly matching the input prefix will be discarded.
    assertThat(objectInfos)
        .containsExactly(
            createItemInfoForStorageObject(
                new StorageResourceId(BUCKET_NAME, object1.getName()), object1),
            createItemInfoForStorageObject(
                new StorageResourceId(BUCKET_NAME, object2.getName()), object2))
        .inOrder();
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                BUCKET_NAME,
                /* includeTrailingDelimiter= */ true,
                prefix,
                /* maxResults= */ 1024,
                /* pageToken= */ null))
        .inOrder();
  }

  @Test
  public void testListObjectInfoReturnPrefixes() throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    StorageObject dir0 = newStorageObject(BUCKET_NAME, dir0Name);
    StorageObject dir1 = newStorageObject(BUCKET_NAME, dir1Name);

    List<StorageObject> objects =
        ImmutableList.of(newStorageObject(BUCKET_NAME, objectPrefix), dir0, dir1);

    MockHttpTransport transport =
        mockTransport(jsonDataResponse(new Objects().setItems(objects).setNextPageToken(null)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    trackingHttpRequestInitializer.getAllRequestStrings();
    assertThat(objectInfos)
        .containsExactly(
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir0Name), dir0),
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1))
        .inOrder();

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                BUCKET_NAME,
                /* includeTrailingDelimiter= */ true,
                objectPrefix,
                /* maxResults= */ 1024,
                /* pageToken= */ null))
        .inOrder();
  }

  @Test
  public void testListObjectInfoWithoutInferImplicit() throws IOException {
    runTestListObjectInfo(false);
  }

  private void runTestListObjectInfo(boolean inferImplicit) throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    String dir2Name = "foo/bar/baz/dir2/";
    StorageObject dir1 = newStorageObject(BUCKET_NAME, dir1Name);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                new Objects()
                    .setPrefixes(ImmutableList.of(dir0Name, dir1Name, dir2Name))
                    .setItems(ImmutableList.of(dir1))
                    .setNextPageToken(null)));

    GoogleCloudStorageOptions gcsOptions =
        GCS_OPTIONS.toBuilder().setInferImplicitDirectoriesEnabled(inferImplicit).build();
    GoogleCloudStorage gcs = mockedGcs(gcsOptions, transport);

    // List the objects
    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    if (gcs.getOptions().isInferImplicitDirectoriesEnabled()) {
      assertThat(objectInfos)
          .containsExactly(
              createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir0Name)),
              createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1),
              createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir2Name)))
          .inOrder();
    } else {
      assertThat(objectInfos)
          .containsExactly(
              createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1))
          .inOrder();
    }

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                BUCKET_NAME,
                /* includeTrailingDelimiter= */ true,
                objectPrefix,
                /* maxResults= */ 1024,
                /* pageToken= */ null))
        .inOrder();
  }

  @Test
  public void testListObjectInfoInferImplicit() throws IOException {
    String objectPrefix = "foo/bar/baz/";
    String delimiter = "/";
    String dir0Name = "foo/bar/baz/dir0/";
    String dir1Name = "foo/bar/baz/dir1/";
    String dir2Name = "foo/bar/baz/dir2/";
    StorageObject dir1 = newStorageObject(BUCKET_NAME, dir1Name);

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(
                new Objects()
                    .setPrefixes(ImmutableList.of(dir0Name, dir1Name, dir2Name))
                    .setItems(ImmutableList.of(dir1))
                    .setNextPageToken(null)));

    GoogleCloudStorageOptions gcsOptions =
        GCS_OPTIONS.toBuilder().setInferImplicitDirectoriesEnabled(true).build();

    GoogleCloudStorage gcs = mockedGcs(gcsOptions, transport);

    // List the objects
    List<GoogleCloudStorageItemInfo> objectInfos =
        gcs.listObjectInfo(BUCKET_NAME, objectPrefix, delimiter);

    // Only one of our three directory objects existed.
    assertThat(objectInfos)
        .containsExactly(
            createItemInfoForStorageObject(new StorageResourceId(BUCKET_NAME, dir1Name), dir1),
            createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir0Name)),
            createInferredDirectory(new StorageResourceId(BUCKET_NAME, dir2Name)))
        .inOrder();

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                BUCKET_NAME,
                /* includeTrailingDelimiter= */ true,
                objectPrefix,
                /* maxResults= */ 1024,
                /* pageToken= */ null))
        .inOrder();
  }

  /** Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent ROOT. */
  @Test
  public void testGetItemInfoRoot() throws IOException {
    GoogleCloudStorage gcs = mockedGcs(HTTP_TRANSPORT);
    GoogleCloudStorageItemInfo info = gcs.getItemInfo(StorageResourceId.ROOT);
    assertThat(info).isEqualTo(GoogleCloudStorageItemInfo.ROOT_INFO);
  }

  /**
   * Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent only a bucket.
   */
  @Test
  public void testGetItemInfoBucket() throws IOException {
    Bucket bucket = newBucket(BUCKET_NAME);
    StorageResourceId bucketId = new StorageResourceId(bucket.getName());

    MockHttpTransport transport = mockTransport(jsonDataResponse(bucket));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageItemInfo info = gcs.getItemInfo(bucketId);

    assertThat(info).isEqualTo(createItemInfoForBucket(bucketId, bucket));
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getBucketRequestString(BUCKET_NAME))
        .inOrder();
  }

  /** Test handling of mismatch in Bucket.getName() vs StorageResourceId.getBucketName(). */
  @Test
  public void testGetItemInfoBucketReturnMismatchedName() throws IOException {
    Bucket bucket = newBucket("wrong-bucket-name");
    StorageResourceId bucketId = new StorageResourceId(BUCKET_NAME);

    MockHttpTransport transport = mockTransport(jsonDataResponse(bucket));

    GoogleCloudStorage gcs = mockedGcs(transport);

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> gcs.getItemInfo(bucketId));

    String expectedMsg =
        String.format(
            "resourceId.getBucketName() must equal bucket.getName(): '%s' vs '%s'",
            BUCKET_NAME, bucket.getName());
    assertThat(thrown).hasMessageThat().isEqualTo(expectedMsg);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getBucketRequestString(BUCKET_NAME))
        .inOrder();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.getItemInfo(2) when arguments represent only a bucket.
   */
  @Test
  public void testGetItemInfoBucketApiException() throws IOException {
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND), jsonErrorResponse(ErrorResponses.GONE));

    GoogleCloudStorage gcs = mockedGcs(transport);

    // Not found.
    GoogleCloudStorageItemInfo info = gcs.getItemInfo(new StorageResourceId(BUCKET_NAME));
    GoogleCloudStorageItemInfo expected =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(BUCKET_NAME),
            /* creationTime= */ 0,
            /* modificationTime= */ 0,
            /* size= */ -1,
            /* location= */ null,
            /* storageClass= */ null);
    assertThat(info).isEqualTo(expected);

    // Throw.
    assertThrows(IOException.class, () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME)));
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getBucketRequestString(BUCKET_NAME), getBucketRequestString(BUCKET_NAME))
        .inOrder();
  }

  /**
   * Test GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent an object in a
   * bucket.
   */
  @Test
  public void testGetItemInfoObject() throws IOException {
    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport = mockTransport(jsonDataResponse(storageObject));

    GoogleCloudStorage gcs = mockedGcs(transport);

    GoogleCloudStorageItemInfo info =
        gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));

    GoogleCloudStorageItemInfo expected =
        GoogleCloudStorageImpl.createItemInfoForStorageObject(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME), storageObject);

    assertThat(info).isEqualTo(expected);
    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  /**
   * Test handling of mismatch in StorageObject.getBucket() and StorageObject.getName() vs
   * respective values in the queried StorageResourceId.
   */
  @Test
  public void testGetItemInfoObjectReturnMismatchedName() throws IOException {
    StorageObject wrongObjectName = newStorageObject(BUCKET_NAME, "wrong-object-name");

    MockHttpTransport transport = mockTransport(jsonDataResponse(wrongObjectName));

    GoogleCloudStorage gcs = mockedGcs(transport);

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    String expectedMsg =
        String.format(
            "resourceId.getObjectName() must equal object.getName(): '%s' vs '%s'",
            OBJECT_NAME, wrongObjectName.getName());
    assertThat(thrown).hasMessageThat().isEqualTo(expectedMsg);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(getRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  /**
   * Test handling of various types of exceptions thrown during JSON API call for
   * GoogleCloudStorage.getItemInfo(StorageResourceId) when arguments represent an object in a
   * bucket.
   */
  @Test
  public void testGetItemInfoObjectApiException() throws IOException {
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.NOT_FOUND), jsonErrorResponse(ErrorResponses.GONE));

    GoogleCloudStorage gcs = mockedGcs(transport);

    // Not found.
    GoogleCloudStorageItemInfo info =
        gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    GoogleCloudStorageItemInfo expected =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
            /* creationtTime */ 0L,
            /* modificationTIme */ 0L,
            /* size */ -1L,
            /* location */ null,
            /* storageClass */ null,
            /* contentType */ null,
            /* contentEncoding */ null,
            /* metadata */ EMPTY_METADATA,
            /* contentGeneration= */ 0,
            /* metaGeneration= */ 0);
    assertThat(info).isEqualTo(expected);

    // Throw.
    assertThrows(
        IOException.class, () -> gcs.getItemInfo(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME), getRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  @Test
  public void testGetItemInfos() throws IOException {
    Bucket bucket = newBucket(BUCKET_NAME);
    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockBatchTransport(
            /* requestsPerBatch= */ 2, jsonDataResponse(storageObject), jsonDataResponse(bucket));

    GoogleCloudStorage gcs = mockedGcs(transport);

    // Call in order of StorageObject, Bucket.
    List<GoogleCloudStorageItemInfo> itemInfos =
        gcs.getItemInfos(
            ImmutableList.of(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                new StorageResourceId(BUCKET_NAME)));

    assertThat(itemInfos)
        .containsExactly(
            GoogleCloudStorageImpl.createItemInfoForStorageObject(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME), storageObject),
            GoogleCloudStorageImpl.createItemInfoForBucket(
                new StorageResourceId(BUCKET_NAME), bucket))
        .inOrder();

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getBucketRequestString(BUCKET_NAME))
        .inOrder();
  }

  @Test
  public void testGetItemInfosNotFound() throws IOException {
    MockHttpTransport transport =
        mockBatchTransport(
            /* requestsPerBatch= */ 2,
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            jsonErrorResponse(ErrorResponses.NOT_FOUND));

    GoogleCloudStorage gcs = mockedGcs(transport);

    // Call in order of StorageObject, ROOT, Bucket.
    List<GoogleCloudStorageItemInfo> itemInfos =
        gcs.getItemInfos(
            ImmutableList.of(
                new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                StorageResourceId.ROOT,
                new StorageResourceId(BUCKET_NAME)));

    GoogleCloudStorageItemInfo expectedObject =
        GoogleCloudStorageItemInfo.createNotFound(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));
    GoogleCloudStorageItemInfo expectedRoot = GoogleCloudStorageItemInfo.ROOT_INFO;
    GoogleCloudStorageItemInfo expectedBucket =
        GoogleCloudStorageItemInfo.createNotFound(new StorageResourceId(BUCKET_NAME));

    assertThat(itemInfos).containsExactly(expectedObject, expectedRoot, expectedBucket).inOrder();

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getBucketRequestString(BUCKET_NAME))
        .inOrder();
  }

  /** Test for GoogleCloudStorage.close(0). */
  @Test
  public void testClose() {
    GoogleCloudStorage gcs = mockedGcs(HTTP_TRANSPORT);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    ((GoogleCloudStorageImpl) gcs).setBackgroundTasksThreadPool(executorService);

    gcs.close();

    assertThat(executorService.isShutdown()).isTrue();
  }

  @Test
  public void testComposeSuccess() throws Exception {
    trackingHttpRequestInitializer = new TrackingHttpRequestInitializer();

    List<String> sources = ImmutableList.of("object1", "object2");

    StorageObject storageObject = newStorageObject(BUCKET_NAME, OBJECT_NAME);

    MockHttpTransport transport =
        mockTransport(jsonDataResponse(storageObject), jsonDataResponse(storageObject));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.compose(BUCKET_NAME, sources, OBJECT_NAME, CreateFileOptions.DEFAULT_CONTENT_TYPE);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            composeRequestString(BUCKET_NAME, OBJECT_NAME, 1))
        .inOrder();
  }

  @Test
  public void testComposeObjectsWithGenerationId() throws Exception {
    String destination = "composedObject";
    int generationId = 35;
    StorageResourceId destinationId = new StorageResourceId(BUCKET_NAME, destination, generationId);

    StorageObject destinationObject = newStorageObject(BUCKET_NAME, destination);
    StorageObject object1 = newStorageObject(BUCKET_NAME, "object1");
    StorageObject object2 = newStorageObject(BUCKET_NAME, "object2");

    List<StorageResourceId> sources =
        ImmutableList.of(
            new StorageResourceId(BUCKET_NAME, "object1"),
            new StorageResourceId(BUCKET_NAME, "object2"));

    MockHttpTransport transport =
        mockTransport(
            jsonDataResponse(destinationObject),
            jsonDataResponse(object1),
            jsonDataResponse(object2));

    GoogleCloudStorage gcs = mockedGcs(transport);

    Map<String, byte[]> rawMetadata =
        ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02});
    GoogleCloudStorageItemInfo composedInfo =
        gcs.composeObjects(
            sources, destinationId, new CreateObjectOptions(false, "text-content", rawMetadata));

    assertThat(composedInfo)
        .isEqualTo(
            GoogleCloudStorageImpl.createItemInfoForStorageObject(
                new StorageResourceId(BUCKET_NAME, destination), destinationObject));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            composeRequestString(
                BUCKET_NAME, destination, generationId, /* replaceGenerationId= */ false))
        .inOrder();
  }

  /** Test validation of Storage passing constructor. */
  @Test
  public void testStoragePassedConstructor() {
    GoogleCloudStorageOptions.Builder optionsBuilder =
        GoogleCloudStorageOptions.builder().setAppName("appName").setProjectId("projectId");

    Storage storage = new Storage(HTTP_TRANSPORT, JSON_FACTORY, trackingHttpRequestInitializer);
    // Verify that fake projectId/appName and mock storage does not throw.
    new GoogleCloudStorageImpl(optionsBuilder.build(), storage);

    // Verify that projectId == null or empty does not throw.
    optionsBuilder.setProjectId(null);
    new GoogleCloudStorageImpl(optionsBuilder.build(), storage);

    optionsBuilder.setProjectId("");
    new GoogleCloudStorageImpl(optionsBuilder.build(), storage);

    optionsBuilder.setProjectId("projectId");

    // Verify that appName == null or empty throws IllegalArgumentException.

    optionsBuilder.setAppName(null);
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageImpl(optionsBuilder.build(), storage));

    optionsBuilder.setAppName("");
    assertThrows(
        IllegalArgumentException.class,
        () -> new GoogleCloudStorageImpl(optionsBuilder.build(), storage));

    optionsBuilder.setAppName("appName");

    // Verify that gcs == null throws NullPointerException.
    assertThrows(
        NullPointerException.class,
        () -> new GoogleCloudStorageImpl(optionsBuilder.build(), (Storage) null));
  }

  /** Provides coverage for default constructor. No real validation is performed. */
  @Test
  public void testCoverDefaultConstructor() {
    new GoogleCloudStorageImpl();
  }

  /** Coverage for GoogleCloudStorageItemInfo.metadataEquals. */
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
                    new HashMap<>(
                        ImmutableMap.of("foo", new byte[] {0x01}, "bar", new byte[] {0x02})))
                .metadataEquals(
                    new TreeMap<>(
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
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.RATE_LIMITED),
            jsonDataResponse(
                getStorageObjectForEmptyObjectWithMetadata(ImmutableMap.of("foo", new byte[0]))));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.createEmptyObject(
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        new CreateObjectOptions(true, ImmutableMap.of("foo", new byte[0])));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(BUCKET_NAME, OBJECT_NAME, /* generationId= */ null),
            getRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectMismatchMetadata() throws IOException {
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.RATE_LIMITED),
            jsonDataResponse(getStorageObjectForEmptyObjectWithMetadata(EMPTY_METADATA)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                gcs.createEmptyObject(
                    new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
                    new CreateObjectOptions(true, ImmutableMap.of("foo", new byte[0]))));
    assertThat(thrown).hasMessageThat().contains(ApiErrorExtractor.RATE_LIMITED_REASON);

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(BUCKET_NAME, OBJECT_NAME, /* generationId= */ null),
            getRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectMismatchMetadataButOptionsHasNoMetadata()
      throws IOException {
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.RATE_LIMITED),
            jsonDataResponse(
                getStorageObjectForEmptyObjectWithMetadata(ImmutableMap.of("foo", new byte[0]))));

    GoogleCloudStorage gcs = mockedGcs(transport);

    // The fetch will "mismatch" with more metadata than our default EMPTY_METADATA used in the
    // default CreateObjectOptions, but we won't care because the metadata-check requirement
    // will be false, so the call will complete successfully.
    gcs.createEmptyObject(new StorageResourceId(BUCKET_NAME, OBJECT_NAME));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(BUCKET_NAME, OBJECT_NAME, null),
            getRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjects() throws IOException {
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.RATE_LIMITED),
            jsonDataResponse(getStorageObjectForEmptyObjectWithMetadata(EMPTY_METADATA)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.createEmptyObjects(ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(BUCKET_NAME, OBJECT_NAME, null),
            getRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectsNonIgnorableException() throws Exception {
    MockHttpTransport transport = mockTransport(jsonErrorResponse(ErrorResponses.GONE));

    GoogleCloudStorage gcs = mockedGcs(transport);

    assertThrows(
        IOException.class,
        () ->
            gcs.createEmptyObjects(
                ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME))));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(uploadRequestString(BUCKET_NAME, OBJECT_NAME, /* generationId= */ null))
        .inOrder();
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectsErrorOnRefetch() throws IOException {
    String objectName1 = OBJECT_NAME + 1;
    String objectName2 = OBJECT_NAME + 2;

    InputStream failedStream =
        new ThrowingInputStream(new RuntimeException("read RuntimeException"));

    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.RATE_LIMITED),
            jsonDataResponse(newStorageObject(BUCKET_NAME, objectName2)),
            inputStreamResponse(CONTENT_LENGTH, 1, failedStream));

    GoogleCloudStorage gcs = mockedGcs(transport);

    List<StorageResourceId> resourceIds =
        ImmutableList.of(
            new StorageResourceId(BUCKET_NAME, objectName1),
            new StorageResourceId(BUCKET_NAME, objectName2));

    IOException thrown = assertThrows(IOException.class, () -> gcs.createEmptyObjects(resourceIds));
    assertThat(thrown).hasMessageThat().contains("Multiple IOExceptions");

    List<String> allRequestStrings = trackingHttpRequestInitializer.getAllRequestStrings();
    assertThat(allRequestStrings).hasSize(3);
    assertThat(allRequestStrings)
        .containsAtLeast(
            uploadRequestString(BUCKET_NAME, objectName1, /* generationId= */ null),
            uploadRequestString(BUCKET_NAME, objectName2, /* generationId= */ null));
    assertThat(allRequestStrings)
        .containsAnyOf(
            getRequestString(BUCKET_NAME, objectName1), getRequestString(BUCKET_NAME, objectName2));
  }

  @Test
  public void testIgnoreExceptionsOnCreateEmptyObjectsWithMultipleRetries() throws Exception {
    MockHttpTransport transport =
        mockTransport(
            jsonErrorResponse(ErrorResponses.RATE_LIMITED),
            jsonErrorResponse(ErrorResponses.NOT_FOUND),
            jsonDataResponse(getStorageObjectForEmptyObjectWithMetadata(EMPTY_METADATA)));

    GoogleCloudStorage gcs = mockedGcs(transport);

    gcs.createEmptyObjects(ImmutableList.of(new StorageResourceId(BUCKET_NAME, OBJECT_NAME)));

    assertThat(trackingHttpRequestInitializer.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(BUCKET_NAME, OBJECT_NAME, null),
            getRequestString(BUCKET_NAME, OBJECT_NAME),
            getRequestString(BUCKET_NAME, OBJECT_NAME))
        .inOrder();
  }

  private GoogleCloudStorage mockedGcs(HttpTransport transport) {
    return mockedGcs(GCS_OPTIONS, transport);
  }

  private GoogleCloudStorage mockedGcs(
      GoogleCloudStorageOptions gcsOptions, HttpTransport transport) {
    Storage storage = new Storage(transport, JSON_FACTORY, trackingHttpRequestInitializer);
    return new GoogleCloudStorageImpl(gcsOptions, storage);
  }

  static Bucket newBucket(String name) {
    return new Bucket()
        .setName(name)
        .setLocation("us-central1-a")
        .setStorageClass("class-af4")
        .setTimeCreated(new DateTime(new Date()))
        .setUpdated(new DateTime(new Date()));
  }

  static StorageObject newStorageObject(String bucketName, String objectName) {
    Random r = new Random();
    return new StorageObject()
        .setBucket(bucketName)
        .setName(objectName)
        .setSize(BigInteger.valueOf(r.nextInt(Integer.MAX_VALUE)))
        .setStorageClass("standard")
        .setGeneration((long) r.nextInt(Integer.MAX_VALUE))
        .setMetageneration((long) r.nextInt(Integer.MAX_VALUE))
        .setTimeCreated(new DateTime(new Date()))
        .setUpdated(new DateTime(new Date()));
  }

  private static byte[] gzip(byte[] testData) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (ByteArrayOutputStream outputStreamToClose = outputStream;
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStreamToClose)) {
      gzipOutputStream.write(testData);
    }
    return outputStream.toByteArray();
  }

  private static InputStream partialReadTimeoutStream(
      byte[] data, double readFraction, String timeoutMessage) {
    return new InputStream() {
      private int position = 0;
      private final int maxPos = (int) (data.length * readFraction);

      @Override
      public int read() throws IOException {
        if (position == maxPos) {
          // increment position, so read()) will return `-1` on subsequent read() calls.
          position++;
          throw new SocketTimeoutException(timeoutMessage);
        }
        if (position >= maxPos) {
          return -1;
        }
        assertThat(position).isLessThan(maxPos);
        return data[position++] & 0xff;
      }
    };
  }
}
