package com.google.cloud.hadoop.gcsio;

import static org.junit.Assert.assertEquals;
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
import com.google.google.storage.v1.ChecksummedData;
import com.google.google.storage.v1.InsertObjectRequest;
import com.google.google.storage.v1.InsertObjectSpec;
import com.google.google.storage.v1.Object;
import com.google.google.storage.v1.ObjectChecksums;
import com.google.google.storage.v1.StartResumableWriteRequest;
import com.google.google.storage.v1.StartResumableWriteResponse;
import com.google.google.storage.v1.StorageObjectsGrpc;
import com.google.google.storage.v1.StorageObjectsGrpc.StorageObjectsImplBase;
import com.google.google.storage.v1.StorageObjectsGrpc.StorageObjectsStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private StorageObjectsStub stub;
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
        StorageObjectsGrpc.newStub(
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
  public void writeSendsMultipleInsertObjectRequests() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();
    int chunkSize = writeChannel.GCS_MINIMUM_CHUNK_SIZE;

    ByteString data = createTestData(chunkSize * 5 / 2);
    writeChannel.setUploadChunkSize(chunkSize);
    writeChannel.initialize();
    writeChannel.write(data.asReadOnlyByteBuffer());
    writeChannel.close();

    List<InsertObjectRequest> expectedRequests =
        Arrays.asList(
            InsertObjectRequest.newBuilder()
                .setUploadId(UPLOAD_ID)
                .setChecksummedData(
                    ChecksummedData.newBuilder()
                        .setContent(data.substring(0, chunkSize))
                        .setCrc32C(UInt32Value.newBuilder().setValue(uInt32Value(1916767651L))))
                .build(),
            InsertObjectRequest.newBuilder()
                .setUploadId(UPLOAD_ID)
                .setChecksummedData(
                    ChecksummedData.newBuilder()
                        .setContent(data.substring(chunkSize, 2 * chunkSize))
                        .setCrc32C(UInt32Value.newBuilder().setValue(uInt32Value(2842290927L))))
                .setWriteOffset(chunkSize)
                .build(),
            InsertObjectRequest.newBuilder()
                .setUploadId(UPLOAD_ID)
                .setChecksummedData(
                    ChecksummedData.newBuilder()
                        .setContent(data.substring(2 * chunkSize))
                        .setCrc32C(UInt32Value.newBuilder().setValue(uInt32Value(2513346319L))))
                .setWriteOffset(2 * chunkSize)
                .setObjectChecksums(
                    ObjectChecksums.newBuilder()
                        .setCrc32C(UInt32Value.newBuilder().setValue(uInt32Value(157031841))))
                .setFinishWrite(true)
                .build());
    ArgumentCaptor<InsertObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(InsertObjectRequest.class);

    verify(fakeService, times(1)).startResumableWrite(eq(START_REQUEST), any());
    verify(fakeService.insertRequestObserver, times(3)).onNext(requestCaptor.capture());
    assertEquals(expectedRequests, requestCaptor.getAllValues());
    verify(fakeService.insertRequestObserver, atLeast(1)).onCompleted();
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
  public void writeFailsBeforeInitialize() throws Exception {
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
  public void setUploadChunkSizeFailsOnNegative() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    assertThrows(IllegalArgumentException.class, () -> writeChannel.setUploadChunkSize(-1));
  }

  @Test
  public void setUploadChunkSizeFailsOnLessThanMinimum() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    assertThrows(
        IllegalArgumentException.class,
        () -> writeChannel.setUploadChunkSize(writeChannel.GCS_MINIMUM_CHUNK_SIZE - 1));
  }

  @Test
  public void closeFailsBeforeInitilize() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    assertThrows(IllegalStateException.class, writeChannel::close);
  }

  @Test
  public void getItemInfoReturnsNullBeforeClose() throws Exception {
    GoogleCloudStorageGrpcWriteChannel writeChannel = newWriteChannel();

    assertNull(writeChannel.getItemInfo());
  }

  @Test
  public void isOpenReturnsFalseBeforeInitialize() throws Exception {
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
      AsyncWriteChannelOptions options, ObjectWriteConditions writeConditions) {
    return new GoogleCloudStorageGrpcWriteChannel(
        executor,
        stub,
        BUCKET_NAME,
        OBJECT_NAME,
        options,
        writeConditions,
        OBJECT_METADATA,
        CONTENT_TYPE);
  }

  private GoogleCloudStorageGrpcWriteChannel newWriteChannel() {
    AsyncWriteChannelOptions options = AsyncWriteChannelOptions.newBuilder().build();
    ObjectWriteConditions writeConditions = new ObjectWriteConditions();

    return newWriteChannel(options, writeConditions);
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

  private static class FakeService extends StorageObjectsImplBase {
    private static final Object DEFAULT_OBJECT =
        Object.newBuilder().setBucket(BUCKET_NAME).setName(OBJECT_NAME).build();

    InsertRequestObserver insertRequestObserver = spy(new InsertRequestObserver());

    private Throwable startRequestException;

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
    public StreamObserver<InsertObjectRequest> insert(StreamObserver<Object> responseObserver) {
      insertRequestObserver.responseObserver = responseObserver;
      return insertRequestObserver;
    }

    public void setStartRequestException(Throwable t) {
      startRequestException = t;
    }

    public void setInsertRequestException(Throwable t) {
      insertRequestObserver.insertRequestException = t;
    }

    private static class InsertRequestObserver implements StreamObserver<InsertObjectRequest> {
      private StreamObserver<Object> responseObserver;
      Throwable insertRequestException;

      @Override
      public void onNext(InsertObjectRequest request) {
        if (insertRequestException != null) {
          responseObserver.onError(insertRequestException);
        }
      }

      @Override
      public void onError(Throwable t) {
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        responseObserver.onNext(DEFAULT_OBJECT);
        responseObserver.onCompleted();
      }
    }
  }
}
