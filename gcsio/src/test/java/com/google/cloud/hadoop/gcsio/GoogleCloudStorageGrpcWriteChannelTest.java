package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.google.storage.v1.ChecksummedData;
import com.google.google.storage.v1.InsertObjectRequest;
import com.google.google.storage.v1.InsertObjectSpec;
import com.google.google.storage.v1.Object;
import com.google.google.storage.v1.ObjectChecksums;
import com.google.google.storage.v1.QueryWriteStatusRequest;
import com.google.google.storage.v1.QueryWriteStatusResponse;
import com.google.google.storage.v1.StartResumableWriteRequest;
import com.google.google.storage.v1.StartResumableWriteResponse;
import com.google.google.storage.v1.StorageGrpc;
import com.google.google.storage.v1.StorageGrpc.StorageImplBase;
import com.google.google.storage.v1.StorageGrpc.StorageStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public final class GoogleCloudStorageGrpcWriteChannelTest {
  private static final String BUCKET_NAME = "bucket-name";
  private static final String OBJECT_NAME = "object-name";
  private static final String UPLOAD_ID = "upload-id";
  private static final String CONTENT_TYPE = "image/jpeg";
  private static final Map<String, String> OBJECT_METADATA = new HashMap<>();
  private static final StartResumableWriteRequest START_REQUEST =
      StartResumableWriteRequest.newBuilder()
          .setInsertObjectSpec(
              InsertObjectSpec.newBuilder()
                  .setResource(
                      Object.newBuilder()
                          .setBucket(BUCKET_NAME)
                          .setName(OBJECT_NAME)
                          .setContentType(CONTENT_TYPE)))
          .build();
  private static final QueryWriteStatusRequest WRITE_STATUS_REQUEST =
      QueryWriteStatusRequest.newBuilder().setUploadId(UPLOAD_ID).build();

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private StorageStub stub;
  private FakeService fakeService;
  private ExecutorService executor = Executors.newCachedThreadPool();

  @Before
  public void setUp() throws Exception {
    fakeService = spy(new FakeService());
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(fakeService)
            .build()
            .start());
    stub =
        StorageGrpc.newStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }

  @Test
  public void writeSendsSingleInsertObjectRequest() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    ByteString data = ByteString.copyFromUtf8("test data");
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    InsertObjectRequest expectedInsertRequest =
        InsertObjectRequest.newBuilder()
            .setUploadId(UPLOAD_ID)
            .setChecksummedData(
                ChecksummedData.newBuilder()
                    .setContent(data)
                    .setCrc32C(UInt32Value.newBuilder().setValue(uInt32Value(863614154))))
            .setObjectChecksums(
                ObjectChecksums.newBuilder()
                    .setCrc32C(UInt32Value.newBuilder().setValue(uInt32Value(863614154))))
            .setFinishWrite(true)
            .build();

    verify(fakeService, times(1)).startResumableWrite(eq(START_REQUEST), any());
    verify(fakeService.insertRequestObserver, times(1)).onNext(expectedInsertRequest);
    verify(fakeService.insertRequestObserver, atLeast(1)).onCompleted();
  }

  @Test
  public void writeSendsSingleInsertObjectRequestWithoutChecksums() throws Exception {
    AsyncWriteChannelOptions options =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(false).build();
    ObjectWriteConditions writeConditions = new ObjectWriteConditions();
    GoogleCloudStorageGrpcWriteChannel writeChannel =
        newWriteChannel(options, writeConditions, Optional.absent());

    ByteString data = ByteString.copyFromUtf8("test data");
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    InsertObjectRequest expectedInsertRequest =
        InsertObjectRequest.newBuilder()
            .setUploadId(UPLOAD_ID)
            .setChecksummedData(ChecksummedData.newBuilder().setContent(data))
            .setFinishWrite(true)
            .build();

    verify(fakeService, times(1)).startResumableWrite(eq(START_REQUEST), any());
    verify(fakeService.insertRequestObserver, times(1)).onNext(expectedInsertRequest);
    verify(fakeService.insertRequestObserver, atLeast(1)).onCompleted();
  }

  @Test
  public void writeSendsMultipleInsertObjectRequests() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();
    int chunkSize = GoogleCloudStorageGrpcWriteChannel.GCS_MINIMUM_CHUNK_SIZE;
    fakeService.setQueryWriteStatusResponses(
        ImmutableList.of(
                QueryWriteStatusResponse.newBuilder().setCommittedSize(chunkSize).build(),
                QueryWriteStatusResponse.newBuilder().setCommittedSize(2 * chunkSize).build())
            .iterator());

    ByteString data = createTestData(chunkSize * 5 / 2);
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    ArgumentCaptor<InsertObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(InsertObjectRequest.class);

    verify(fakeService, times(1)).startResumableWrite(eq(START_REQUEST), any());
    verify(fakeService.insertRequestObserver, times(1)).onNext(requestCaptor.capture());
    verify(fakeService.insertRequestObserver, atLeast(1)).onCompleted();
  }

  @Test
  public void writeSendsMultipleInsertObjectRequestsWithChecksums() throws Exception {
    AsyncWriteChannelOptions options =
        AsyncWriteChannelOptions.builder().setGrpcChecksumsEnabled(true).build();
    ObjectWriteConditions writeConditions = new ObjectWriteConditions();
    GoogleCloudStorageGrpcWriteChannel writeChannel =
        newWriteChannel(options, writeConditions, Optional.absent());
    int chunkSize = GoogleCloudStorageGrpcWriteChannel.GCS_MINIMUM_CHUNK_SIZE;
    fakeService.setQueryWriteStatusResponses(
        ImmutableList.of(
                QueryWriteStatusResponse.newBuilder().setCommittedSize(chunkSize).build(),
                QueryWriteStatusResponse.newBuilder().setCommittedSize(2 * chunkSize).build())
            .iterator());

    ByteString data = createTestData(chunkSize * 5 / 2);
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    ArgumentCaptor<InsertObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(InsertObjectRequest.class);

    verify(fakeService, times(1)).startResumableWrite(eq(START_REQUEST), any());
    verify(fakeService.insertRequestObserver, times(1)).onNext(requestCaptor.capture());
    verify(fakeService.insertRequestObserver, atLeast(1)).onCompleted();
  }

  @Test
  public void writeHandlesUncommittedData() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();
    int chunkSize = GoogleCloudStorageGrpcWriteChannel.GCS_MINIMUM_CHUNK_SIZE;
    fakeService.setQueryWriteStatusResponses(
        ImmutableList.of(
                QueryWriteStatusResponse.newBuilder().setCommittedSize(chunkSize * 3 / 4).build())
            .iterator());

    ByteString data = createTestData(chunkSize * 3 / 2);
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    ArgumentCaptor<InsertObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(InsertObjectRequest.class);

    verify(fakeService, times(1)).startResumableWrite(eq(START_REQUEST), any());
    // TODO(b/150892988): Use this mock when implement resuming after a transient error.
    // verify(fakeService, times(1)).queryWriteStatus(eq(WRITE_STATUS_REQUEST), any());
    verify(fakeService.insertRequestObserver, times(1)).onNext(requestCaptor.capture());
    verify(fakeService.insertRequestObserver, atLeast(1)).onCompleted();
  }

  @Test
  public void writeUsesContentGenerationIfProvided() throws Exception {
    AsyncWriteChannelOptions options = AsyncWriteChannelOptions.builder().build();
    ObjectWriteConditions writeConditions =
        new ObjectWriteConditions(Optional.of(1L), Optional.absent());
    GoogleCloudStorageGrpcWriteChannel writeChannel =
        newWriteChannel(options, writeConditions, Optional.absent());

    ByteString data = ByteString.copyFromUtf8("test data");
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    StartResumableWriteRequest.Builder expectedRequestBuilder = START_REQUEST.toBuilder();
    expectedRequestBuilder
        .getInsertObjectSpecBuilder()
        .setIfGenerationMatch(Int64Value.newBuilder().setValue(1L));
    verify(fakeService, times(1)).startResumableWrite(eq(expectedRequestBuilder.build()), any());
  }

  @Test
  public void writeUsesMetaGenerationIfProvided() throws Exception {
    AsyncWriteChannelOptions options = AsyncWriteChannelOptions.builder().build();
    ObjectWriteConditions writeConditions =
        new ObjectWriteConditions(Optional.absent(), Optional.of(1L));
    GoogleCloudStorageGrpcWriteChannel writeChannel =
        newWriteChannel(options, writeConditions, Optional.absent());

    ByteString data = ByteString.copyFromUtf8("test data");
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    StartResumableWriteRequest.Builder expectedRequestBuilder = START_REQUEST.toBuilder();
    expectedRequestBuilder
        .getInsertObjectSpecBuilder()
        .setIfMetagenerationMatch(Int64Value.newBuilder().setValue(1L));
    verify(fakeService, times(1)).startResumableWrite(eq(expectedRequestBuilder.build()), any());
  }

  @Test
  public void writeUsesRequesterPaysProjectIfProvided() throws Exception {
    AsyncWriteChannelOptions options = AsyncWriteChannelOptions.builder().build();
    ObjectWriteConditions writeConditions = new ObjectWriteConditions();
    GoogleCloudStorageGrpcWriteChannel writeChannel =
        newWriteChannel(options, writeConditions, Optional.of("project-id"));

    ByteString data = ByteString.copyFromUtf8("test data");
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    StartResumableWriteRequest.Builder expectedRequestBuilder = START_REQUEST.toBuilder();
    expectedRequestBuilder.getInsertObjectSpecBuilder().setUserProject("project-id");
    verify(fakeService, times(1)).startResumableWrite(eq(expectedRequestBuilder.build()), any());
  }

  @Test
  public void writeHandlesErrorOnStartRequest() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    fakeService.setStartRequestException(new IOException("Error!"));
    writeChannel.initialize();
    writeChannel.write(ByteBuffer.wrap("test data".getBytes()));

    assertThrows(IOException.class, writeChannel::close);
  }

  @Test
  public void writeHandlesErrorOnInsertRequest() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();
    fakeService.setInsertRequestException(new IOException("Error!"));

    writeChannel.initialize();
    writeChannel.write(ByteBuffer.wrap("test data".getBytes()));

    assertThrows(IOException.class, writeChannel::close);
  }

  @Test
  public void writeHandlesErrorOnQueryWriteStatusRequest() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();
    int chunkSize = GoogleCloudStorageGrpcWriteChannel.GCS_MINIMUM_CHUNK_SIZE;
    fakeService.setQueryWriteStatusException(new IOException("Test error!"));
    ByteString data = createTestData(chunkSize * 2);

    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
  }

  @Test
  public void writeHandlesErrorOnInsertRequestWithUncommittedData() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();
    int chunkSize = GoogleCloudStorageGrpcWriteChannel.GCS_MINIMUM_CHUNK_SIZE;
    fakeService.setInsertRequestException(new IOException("Error!"));
    fakeService.setQueryWriteStatusResponses(
        ImmutableList.of(
                QueryWriteStatusResponse.newBuilder().setCommittedSize(chunkSize * 3 / 4).build())
            .iterator());

    ByteString data = createTestData(chunkSize * 3 / 2);
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());

    assertThrows(IOException.class, writeChannel::close);
  }

  @Test
  public void writeHandlesErrorOnInsertRequestWithoutUncommittedData() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();
    int chunkSize = GoogleCloudStorageGrpcWriteChannel.GCS_MINIMUM_CHUNK_SIZE;
    fakeService.setInsertRequestException(new IOException("Error!"));
    fakeService.setQueryWriteStatusResponses(
        ImmutableList.of(QueryWriteStatusResponse.newBuilder().setCommittedSize(chunkSize).build())
            .iterator());

    ByteString data = createTestData(chunkSize);
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());

    assertThrows(IOException.class, writeChannel::close);
  }

  @Test
  public void writeHandlesErrorOnInsertRequestWithLongUncommittedData() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();
    long chunkSize = GoogleCloudStorageGrpcWriteChannel.GCS_MINIMUM_CHUNK_SIZE * 1024L * 1024L;
    fakeService.setInsertRequestException(new IOException("Error!"));
    fakeService.setResumeFromInsertException(true);
    fakeService.setQueryWriteStatusResponses(
        ImmutableList.of(
                QueryWriteStatusResponse.newBuilder().setCommittedSize(chunkSize * 3 / 4).build())
            .iterator());

    ByteString data = createTestData(GoogleCloudStorageGrpcWriteChannel.GCS_MINIMUM_CHUNK_SIZE);
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    verify(fakeService, times(1)).startResumableWrite(eq(START_REQUEST), any());
    verify(fakeService, atLeast(1)).queryWriteStatus(eq(WRITE_STATUS_REQUEST), any());
    verify(fakeService.insertRequestObserver, atLeast(1)).onCompleted();
  }

  @Test
  public void writeFailsBeforeInitialize() {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    assertThrows(
        IllegalStateException.class,
        () -> writeChannel.write(ByteBuffer.wrap("test data".getBytes())));
  }

  @Test
  public void writeFailsAfterClose() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    writeChannel.initialize();
    writeChannel.close();

    assertThrows(
        ClosedChannelException.class,
        () -> writeChannel.write(ByteBuffer.wrap("test data".getBytes())));
  }

  @Test
  public void closeFailsBeforeInitilize() {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    assertThrows(IllegalStateException.class, writeChannel::close);
  }

  @Test
  public void getItemInfoReturnsCorrectItemInfo() throws Exception {
    fakeService.setObject(
        FakeService.DEFAULT_OBJECT.toBuilder()
            .setSize(9)
            .setGeneration(1)
            .setMetageneration(2)
            .setTimeCreated(Timestamp.newBuilder().setSeconds(1560485630).setNanos(7000000))
            .setUpdated(Timestamp.newBuilder().setSeconds(1560495630).setNanos(123000000))
            .setContentEncoding("content-encoding")
            .putMetadata("metadata-key-1", "dGVzdC1tZXRhZGF0YQ==")
            .setMd5Hash("k0K1eqORVuY208nAADpz6w==")
            .setCrc32C(UInt32Value.newBuilder().setValue(uInt32Value(863614154)))
            .build());
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    ByteString data = ByteString.copyFromUtf8("test data");
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();
    GoogleCloudStorageItemInfo itemInfo = writeChannel.getItemInfo();

    byte[] expectedMd5Hash = {
      -109, 66, -75, 122, -93, -111, 86, -26, 54, -45, -55, -64, 0, 58, 115, -21
    };
    byte[] expectedCrc32C = {51, 121, -76, -54};
    Map<String, byte[]> expectedMetadata =
        ImmutableMap.of(
            "metadata-key-1",
            new byte[] {116, 101, 115, 116, 45, 109, 101, 116, 97, 100, 97, 116, 97});
    GoogleCloudStorageItemInfo expectedItemInfo =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
            1560485630007L,
            1560495630123L,
            /* size= */ 9,
            /* location= */ null,
            /* storageClass= */ null,
            CONTENT_TYPE,
            "content-encoding",
            expectedMetadata,
            1,
            2,
            new VerificationAttributes(expectedMd5Hash, expectedCrc32C));

    assertThat(itemInfo).isEqualTo(expectedItemInfo);
  }

  @Test
  public void getItemInfoReturnsNullBeforeClose() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    ByteString data = ByteString.copyFromUtf8("test data");
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());

    assertNull(writeChannel.getItemInfo());
  }

  @Test
  public void isOpenReturnsFalseBeforeInitialize() {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    assertFalse(writeChannel.isOpen());
  }

  @Test
  public void isOpenReturnsTrueAfterInitialize() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    writeChannel.initialize();
    assertTrue(writeChannel.isOpen());
  }

  @Test
  public void isOpenReturnsFalseAfterClose() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    writeChannel.initialize();
    writeChannel.close();
    assertFalse(writeChannel.isOpen());
  }

  private GoogleCloudStorageGrpcWriteChannel newWriteChannel(
      AsyncWriteChannelOptions options,
      ObjectWriteConditions writeConditions,
      Optional<String> requesterPaysProject) {
    return new GoogleCloudStorageGrpcWriteChannel(
        executor,
        stub,
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME),
        options,
        writeConditions,
        requesterPaysProject,
        OBJECT_METADATA,
        CONTENT_TYPE);
  }

  private GoogleCloudStorageGrpcWriteChannel newWriteChannel() {
    AsyncWriteChannelOptions options = AsyncWriteChannelOptions.builder().build();
    ObjectWriteConditions writeConditions = new ObjectWriteConditions();

    return newWriteChannel(options, writeConditions, Optional.absent());
  }

  /* Returns an int with the same bytes as the uint32 representation of value. */
  private int uInt32Value(long value) {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(0, (int) value);
    return buffer.getInt();
  }

  private ByteString createTestData(int numBytes) {
    byte[] result = new byte[numBytes];
    for (int i = 0; i < numBytes; ++i) {
      // Sequential data makes it easier to compare expected vs. actual in
      // case of error. Since chunk sizes are multiples of 256, the modulo
      // ensures chunks have different data.
      result[i] = (byte) (i % 257);
    }

    return ByteString.copyFrom(result);
  }

  private static class FakeService extends StorageImplBase {
    static final Object DEFAULT_OBJECT =
        Object.newBuilder()
            .setBucket(BUCKET_NAME)
            .setName(OBJECT_NAME)
            .setGeneration(1)
            .setMetageneration(2)
            .build();

    InsertRequestObserver insertRequestObserver = spy(new InsertRequestObserver());

    private Throwable startRequestException;
    private Throwable queryWriteStatusException;
    private Iterator<QueryWriteStatusResponse> queryWriteStatusResponses;

    @Override
    public void startResumableWrite(
        StartResumableWriteRequest request,
        StreamObserver<StartResumableWriteResponse> responseObserver) {
      if (startRequestException != null) {
        responseObserver.onError(startRequestException);
      } else {
        StartResumableWriteResponse response =
            StartResumableWriteResponse.newBuilder().setUploadId(UPLOAD_ID).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    }

    @Override
    public void queryWriteStatus(
        QueryWriteStatusRequest request,
        StreamObserver<QueryWriteStatusResponse> responseObserver) {
      if (queryWriteStatusException != null) {
        responseObserver.onError(queryWriteStatusException);
      } else {
        QueryWriteStatusResponse response = queryWriteStatusResponses.next();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    }

    @Override
    public StreamObserver<InsertObjectRequest> insertObject(
        StreamObserver<Object> responseObserver) {
      insertRequestObserver.responseObserver = responseObserver;
      return insertRequestObserver;
    }

    public void setObject(Object object) {
      this.insertRequestObserver.object = object;
    }

    void setQueryWriteStatusResponses(Iterator<QueryWriteStatusResponse> responses) {
      queryWriteStatusResponses = responses;
    }

    void setQueryWriteStatusException(Throwable t) {
      queryWriteStatusException = t;
    }

    void setStartRequestException(Throwable t) {
      startRequestException = t;
    }

    void setInsertRequestException(Throwable t) {
      insertRequestObserver.insertRequestException = t;
    }

    void setResumeFromInsertException(boolean resumable) {
      insertRequestObserver.resumeFromInsertException = resumable;
    }

    private static class InsertRequestObserver implements StreamObserver<InsertObjectRequest> {
      private StreamObserver<Object> responseObserver;
      private Object object = DEFAULT_OBJECT;
      Throwable insertRequestException;
      boolean resumeFromInsertException = false;

      @Override
      public void onNext(InsertObjectRequest request) {
        if (insertRequestException != null) {
          responseObserver.onError(insertRequestException);
          if (resumeFromInsertException) {
            insertRequestException = null;
          }
        }
      }

      @Override
      public void onError(Throwable t) {
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        responseObserver.onNext(object);
        responseObserver.onCompleted();
      }
    }
  }
}
