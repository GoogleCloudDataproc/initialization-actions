package com.google.cloud.hadoop.gcsio;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.hash.Hashing;
import com.google.google.storage.v1.ChecksummedData;
import com.google.google.storage.v1.GetObjectMediaRequest;
import com.google.google.storage.v1.GetObjectMediaResponse;
import com.google.google.storage.v1.GetObjectRequest;
import com.google.google.storage.v1.Object;
import com.google.google.storage.v1.StorageGrpc;
import com.google.google.storage.v1.StorageGrpc.StorageBlockingStub;
import com.google.google.storage.v1.StorageGrpc.StorageImplBase;
import com.google.protobuf.ByteString;
import com.google.protobuf.UInt32Value;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public final class GoogleCloudStorageGrpcReadChannelTest {

  private static final String BUCKET_NAME = "bucket-name";
  private static final String OBJECT_NAME = "object-name";
  private static final long OBJECT_GENERATION = 7;
  private static final int OBJECT_SIZE = FakeService.CHUNK_SIZE * 2 + 10;
  private static final int DEFAULT_OBJECT_CRC32C = 185327488;
  private static Object DEFAULT_OBJECT =
      Object.newBuilder()
          .setBucket(BUCKET_NAME)
          .setName(OBJECT_NAME)
          .setSize(OBJECT_SIZE)
          .setCrc32C(UInt32Value.newBuilder().setValue(DEFAULT_OBJECT_CRC32C))
          .setGeneration(OBJECT_GENERATION)
          .build();
  private static GetObjectRequest GET_OBJECT_REQUEST =
      GetObjectRequest.newBuilder().setBucket(BUCKET_NAME).setObject(OBJECT_NAME).build();
  private static GetObjectMediaRequest GET_OBJECT_MEDIA_REQUEST =
      GetObjectMediaRequest.newBuilder()
          .setBucket(BUCKET_NAME)
          .setObject(OBJECT_NAME)
          .setGeneration(OBJECT_GENERATION)
          .build();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private StorageBlockingStub stub;
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
        StorageGrpc.newBlockingStub(
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }

  @Test
  public void readSingleChunkSucceeds() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    ByteBuffer buffer = ByteBuffer.allocate(100);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GET_OBJECT_MEDIA_REQUEST), any());
    assertArrayEquals(fakeService.data.substring(0, 100).toByteArray(), buffer.array());
  }

  @Test
  public void readMultipleChunksSucceeds() throws Exception {
    // Enough to require multiple chunks.
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(FakeService.CHUNK_SIZE * 2).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    ByteBuffer buffer = ByteBuffer.allocate(FakeService.CHUNK_SIZE * 2);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GET_OBJECT_MEDIA_REQUEST), any());
    assertArrayEquals(
        fakeService.data.substring(0, FakeService.CHUNK_SIZE * 2).toByteArray(), buffer.array());
  }

  @Test
  public void readAfterRepositioningAfterSkippingSucceeds() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(10).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer bufferAtBeginning = ByteBuffer.allocate(20);
    readChannel.read(bufferAtBeginning);
    readChannel.position(25);
    ByteBuffer bufferFromSkippedSection = ByteBuffer.allocate(5);
    readChannel.read(bufferFromSkippedSection);
    ByteBuffer bufferFromReposition = ByteBuffer.allocate(10);
    readChannel.position(1);
    readChannel.read(bufferFromReposition);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GET_OBJECT_MEDIA_REQUEST), any());
    assertArrayEquals(fakeService.data.substring(0, 20).toByteArray(), bufferAtBeginning.array());
    assertArrayEquals(
        fakeService.data.substring(25, 30).toByteArray(), bufferFromSkippedSection.array());
    assertArrayEquals(
        fakeService.data.substring(1, 11).toByteArray(), bufferFromReposition.array());
  }

  @Test
  public void multipleSequentialReads() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    ByteBuffer first_buffer = ByteBuffer.allocate(10);
    ByteBuffer second_buffer = ByteBuffer.allocate(20);
    readChannel.read(first_buffer);
    readChannel.read(second_buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GET_OBJECT_MEDIA_REQUEST), any());
    assertArrayEquals(fakeService.data.substring(0, 10).toByteArray(), first_buffer.array());
    assertArrayEquals(fakeService.data.substring(10, 30).toByteArray(), second_buffer.array());
  }

  @Test
  public void randomReadRequestsExactBytes() throws Exception {
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.RANDOM)
            .setGrpcChecksumsEnabled(true)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(50);
    readChannel.position(10);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());

    GetObjectMediaRequest expectedRequest =
        GetObjectMediaRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(50)
            .setReadOffset(10)
            .build();
    verify(fakeService, times(1)).getObjectMedia(eq(expectedRequest), any());
    assertArrayEquals(fakeService.data.substring(10, 60).toByteArray(), buffer.array());
  }

  @Test
  public void repeatedRandomReadsWorkAsExpected() throws Exception {
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(Fadvise.RANDOM)
            .setGrpcChecksumsEnabled(true)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(50);
    readChannel.position(10);
    readChannel.read(buffer);
    assertArrayEquals(fakeService.data.substring(10, 60).toByteArray(), buffer.array());

    buffer = ByteBuffer.allocate(25);
    readChannel.position(20);
    readChannel.read(buffer);
    assertArrayEquals(fakeService.data.substring(20, 45).toByteArray(), buffer.array());

    GetObjectMediaRequest firstExpectedRequest =
        GetObjectMediaRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(50)
            .setReadOffset(10)
            .build();
    GetObjectMediaRequest secondExpectedRequest =
        GetObjectMediaRequest.newBuilder()
            .setBucket(BUCKET_NAME)
            .setObject(OBJECT_NAME)
            .setGeneration(OBJECT_GENERATION)
            .setReadLimit(25)
            .setReadOffset(20)
            .build();

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(firstExpectedRequest), any());
    verify(fakeService, times(1)).getObjectMedia(eq(secondExpectedRequest), any());
  }

  @Test
  public void readToBufferWithArrayOffset() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    byte[] array = new byte[200];
    // `slice` generates a ByteBuffer with a non-zero `arrayOffset`.
    ByteBuffer buffer = ByteBuffer.wrap(array, 50, 150).slice();
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GET_OBJECT_MEDIA_REQUEST), any());
    byte[] expected = ByteString.copyFrom(array, 50, 100).toByteArray();
    assertArrayEquals(fakeService.data.substring(0, 100).toByteArray(), expected);
  }

  @Test
  public void readSucceedsAfterSeek() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.position(50);
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1))
        .getObjectMedia(eq(GET_OBJECT_MEDIA_REQUEST.toBuilder().setReadOffset(50).build()), any());
    assertArrayEquals(fakeService.data.substring(50, 60).toByteArray(), buffer.array());
  }

  @Test
  public void singleReadSucceedsWithValidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder()
            .setCrc32C(UInt32Value.newBuilder().setValue(DEFAULT_OBJECT_CRC32C))
            .build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setGrpcChecksumsEnabled(true)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(OBJECT_SIZE);
    readChannel.read(buffer);

    assertArrayEquals(fakeService.data.toByteArray(), buffer.array());
  }

  @Test
  public void partialReadSucceedsWithInvalidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder().setCrc32C(UInt32Value.newBuilder().setValue(0)).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setGrpcChecksumsEnabled(true)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(OBJECT_SIZE - 10);
    readChannel.read(buffer);

    assertArrayEquals(
        fakeService.data.substring(0, OBJECT_SIZE - 10).toByteArray(), buffer.array());
  }

  @Test
  public void multipleSequentialsReadsSucceedWithValidObjectChecksum() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder()
            .setCrc32C(UInt32Value.newBuilder().setValue(DEFAULT_OBJECT_CRC32C))
            .build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setGrpcChecksumsEnabled(true)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer firstBuffer = ByteBuffer.allocate(100);
    ByteBuffer secondBuffer = ByteBuffer.allocate(OBJECT_SIZE - 100);
    readChannel.read(firstBuffer);
    readChannel.read(secondBuffer);

    assertArrayEquals(fakeService.data.substring(0, 100).toByteArray(), firstBuffer.array());
    assertArrayEquals(fakeService.data.substring(100).toByteArray(), secondBuffer.array());
  }

  @Test
  public void readFailsWithInvalidMessageChecksum() throws Exception {
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    fakeService.setReturnIncorrectMessageChecksum();

    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertTrue(thrown.getMessage().contains("checksum"));
  }

  @Test
  public void readToBufferWithArrayOffsetSucceeds() throws Exception {
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    byte[] array = new byte[OBJECT_SIZE + 100];
    // `ByteBuffer.slice` generates a ByteBuffer with a non-zero `arrayOffset`.
    ByteBuffer buffer = ByteBuffer.wrap(array, 50, OBJECT_SIZE).slice();
    readChannel.read(buffer);

    byte[] expected = ByteString.copyFrom(array, 50, OBJECT_SIZE).toByteArray();
    assertArrayEquals(fakeService.data.toByteArray(), expected);
  }

  @Test
  public void readToBufferWithArrayOffsetFailsWithInvalidMessageChecksum() throws Exception {
    fakeService.setReturnIncorrectMessageChecksum();
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setGrpcChecksumsEnabled(true).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    byte[] array = new byte[OBJECT_SIZE + 100];
    // `ByteBuffer.slice` generates a ByteBuffer with a non-zero `arrayOffset`.
    ByteBuffer buffer = ByteBuffer.wrap(array, 50, OBJECT_SIZE).slice();

    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertTrue(
        thrown.getMessage() + " should have contained 'checksum'",
        thrown.getMessage().contains("checksum"));
  }

  @Test
  public void multipleReadsIgnoreObjectChecksumForLatestGenerationReads() throws Exception {
    fakeService.setObject(
        DEFAULT_OBJECT.toBuilder().setCrc32C(UInt32Value.newBuilder().setValue(0)).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .setGrpcChecksumsEnabled(true)
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer firstBuffer = ByteBuffer.allocate(100);
    ByteBuffer secondBuffer = ByteBuffer.allocate(OBJECT_SIZE - 100);
    readChannel.read(firstBuffer);
    readChannel.read(secondBuffer);

    assertArrayEquals(fakeService.data.substring(0, 100).toByteArray(), firstBuffer.array());
    assertArrayEquals(fakeService.data.substring(100).toByteArray(), secondBuffer.array());
  }

  @Test
  public void readHandlesGetError() throws Exception {
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();
    fakeService.setGetException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());
    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> newReadChannel(options));
    assertTrue(thrown.getCause().getMessage().contains("Custom error message."));
  }

  @Test
  public void readHandlesGetMediaError() throws Exception {
    fakeService.setGetMediaException(
        Status.fromCode(Status.Code.INTERNAL)
            .withDescription("Custom error message.")
            .asException());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    ByteBuffer buffer = ByteBuffer.allocate(10);
    IOException thrown = assertThrows(IOException.class, () -> readChannel.read(buffer));
    assertTrue(thrown.getCause().getMessage().contains("Custom error message."));
  }

  @Test
  public void readFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    ByteBuffer buffer = ByteBuffer.allocate(10);
    assertThrows(ClosedChannelException.class, () -> readChannel.read(buffer));
  }

  @Test
  public void readWithStrictGenerationReadConsistencySucceeds() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).setGeneration(1).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.read(buffer);
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).setGeneration(2).build());
    readChannel.position(0);
    buffer.clear();
    readChannel.read(buffer);

    List<GetObjectMediaRequest> expectedRequests =
        Arrays.asList(
            GET_OBJECT_MEDIA_REQUEST,
            GET_OBJECT_MEDIA_REQUEST.toBuilder()
                .setReadOffset(10)
                .setReadLimit(20)
                .setGeneration(1)
                .build());
    ArgumentCaptor<GetObjectMediaRequest> requestCaptor =
        ArgumentCaptor.forClass(GetObjectMediaRequest.class);
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(2)).getObjectMedia(requestCaptor.capture(), any());
  }

  @Test
  public void readWithLatestGenerationReadConsistencySucceeds() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).setGeneration(1).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder()
            .build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(10);
    readChannel.read(buffer);
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).setGeneration(2).build());
    readChannel.position(0);
    buffer.clear();
    readChannel.read(buffer);

    List<GetObjectMediaRequest> expectedRequests =
        Arrays.asList(
            GET_OBJECT_MEDIA_REQUEST,
            GET_OBJECT_MEDIA_REQUEST.toBuilder().setReadOffset(10).setReadLimit(20).build());
    ArgumentCaptor<GetObjectMediaRequest> requestCaptor =
        ArgumentCaptor.forClass(GetObjectMediaRequest.class);
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(2)).getObjectMedia(requestCaptor.capture(), any());
  }

  @Test
  public void seekUnderInplaceSeekLimitReadsCorrectBufferedData() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(10).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.read(buffer);
    readChannel.position(25);
    buffer.clear();
    readChannel.read(buffer);

    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
    verify(fakeService, times(1)).getObjectMedia(eq(GET_OBJECT_MEDIA_REQUEST), any());
    assertArrayEquals(fakeService.data.substring(25, 45).toByteArray(), buffer.array());
  }

  @Test
  public void seekUnderInplaceSeekLimitReadsCorrectDataWithSomeBuffered() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(FakeService.CHUNK_SIZE * 4).build());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setInplaceSeekLimit(10).build();
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel(options);

    ByteBuffer buffer = ByteBuffer.allocate(20);
    readChannel.read(buffer);
    readChannel.position(50);
    buffer.clear();
    buffer = ByteBuffer.allocate(FakeService.CHUNK_SIZE * 3 + 7);
    readChannel.read(buffer);

    assertArrayEquals(
        fakeService.data.substring(50, 50 + FakeService.CHUNK_SIZE * 3 + 7).toByteArray(),
        buffer.array());
  }

  @Test
  public void seekFailsOnNegative() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertThrows(IllegalArgumentException.class, () -> readChannel.position(-1));
  }

  @Test
  public void seekFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertThrows(ClosedChannelException.class, () -> readChannel.position(2));
  }

  @Test
  public void positionUpdatesOnRead() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(100).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    ByteBuffer buffer = ByteBuffer.allocate(50);
    readChannel.read(buffer);

    assertEquals(50, readChannel.position());
  }

  @Test
  public void positionUpdatesOnSeek() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.position(50);

    assertEquals(50, readChannel.position());
  }

  @Test
  public void positionFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertThrows(ClosedChannelException.class, readChannel::position);
  }

  @Test
  public void fastFailOnNotFoundFailsOnCreateWhenEnabled() throws Exception {
    fakeService.setGetException(Status.NOT_FOUND.asException());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(true).build();

    assertThrows(FileNotFoundException.class, () -> newReadChannel(options));
  }

  @Test
  public void fastFailOnNotFoundFailsByReadWhenDisabled() throws Exception {
    fakeService.setGetException(Status.NOT_FOUND.asException());
    GoogleCloudStorageReadOptions options =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();

    ByteBuffer buffer = ByteBuffer.allocate(10);

    // If the user hasn't mandated fail fast, it is permissible for either open() or read() to
    // raise this exception.
    assertThrows(FileNotFoundException.class, () -> newReadChannel(options).read(buffer));
  }

  @Test
  public void sizeReturnsObjectSize() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(1234).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertEquals(1234L, readChannel.size());
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
  }

  @Test
  public void sizeFailsOnClosedChannel() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertThrows(ClosedChannelException.class, readChannel::size);
  }

  @Test
  public void sizeIsCached() throws Exception {
    fakeService.setObject(DEFAULT_OBJECT.toBuilder().setSize(1234).build());
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertEquals(1234L, readChannel.size());
    assertEquals(1234L, readChannel.size());
    verify(fakeService, times(1)).getObject(eq(GET_OBJECT_REQUEST), any());
  }

  @Test
  public void isOpenReturnsTrueOnCreate() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    assertTrue(readChannel.isOpen());
  }

  @Test
  public void isOpenReturnsFalseAfterClose() throws Exception {
    GoogleCloudStorageGrpcReadChannel readChannel = newReadChannel();

    readChannel.close();
    assertFalse(readChannel.isOpen());
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel(GoogleCloudStorageReadOptions options)
      throws IOException {
    return GoogleCloudStorageGrpcReadChannel.open(stub, BUCKET_NAME, OBJECT_NAME, options);
  }

  private GoogleCloudStorageGrpcReadChannel newReadChannel() throws IOException {
    return newReadChannel(GoogleCloudStorageReadOptions.DEFAULT);
  }

  private static class FakeService extends StorageImplBase {

    private static final int CHUNK_SIZE = 2048;
    ByteString data;
    private Object object;
    private Throwable getException;
    private Throwable getMediaException;
    private boolean alterMessageChecksum = false;

    public FakeService() {
      setObject(DEFAULT_OBJECT);
    }

    private static ByteString createTestData(int numBytes) {
      byte[] result = new byte[numBytes];
      for (int i = 0; i < numBytes; ++i) {
        result[i] = (byte) i;
      }

      return ByteString.copyFrom(result);
    }

    @Override
    public void getObject(GetObjectRequest request, StreamObserver<Object> responseObserver) {
      if (getException != null) {
        responseObserver.onError(getException);
      } else {
        responseObserver.onNext(object);
        responseObserver.onCompleted();
      }
    }

    @Override
    public void getObjectMedia(
        GetObjectMediaRequest request, StreamObserver<GetObjectMediaResponse> responseObserver) {
      if (getMediaException != null) {
        responseObserver.onError(getMediaException);
      } else {
        int readStart = (int) request.getReadOffset();
        int readEnd =
            request.getReadLimit() > 0
                ? (int) Math.min(object.getSize(), readStart + request.getReadLimit())
                : (int) object.getSize();
        for (int position = readStart; position < readEnd; position += CHUNK_SIZE) {
          ByteString messageData =
              data.substring(position, Math.min((int) object.getSize(), position + CHUNK_SIZE));
          int crc32c = Hashing.crc32c().hashBytes(messageData.toByteArray()).asInt();
          if (alterMessageChecksum) {
            crc32c += 1;
          }
          GetObjectMediaResponse response =
              GetObjectMediaResponse.newBuilder()
                  .setChecksummedData(
                      ChecksummedData.newBuilder()
                          .setContent(messageData)
                          .setCrc32C(UInt32Value.newBuilder().setValue(crc32c)))
                  .build();
          responseObserver.onNext(response);
        }
        responseObserver.onCompleted();
      }
    }

    public void setObject(Object object) {
      this.object = object;
      data = createTestData((int) object.getSize());
    }

    void setGetException(Throwable t) {
      getException = t;
    }

    void setGetMediaException(Throwable t) {
      getMediaException = t;
    }

    void setReturnIncorrectMessageChecksum() {
      alterMessageChecksum = true;
    }
  }
}
