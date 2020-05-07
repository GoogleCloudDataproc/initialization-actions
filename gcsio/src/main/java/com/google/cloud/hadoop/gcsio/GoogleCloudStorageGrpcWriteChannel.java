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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.google.storage.v1.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;

import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.BaseAbstractGoogleAsyncWriteChannel;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.google.storage.v1.ChecksummedData;
import com.google.google.storage.v1.InsertObjectRequest;
import com.google.google.storage.v1.InsertObjectSpec;
import com.google.google.storage.v1.Object;
import com.google.google.storage.v1.ObjectChecksums;
import com.google.google.storage.v1.QueryWriteStatusRequest;
import com.google.google.storage.v1.QueryWriteStatusResponse;
import com.google.google.storage.v1.StartResumableWriteRequest;
import com.google.google.storage.v1.StartResumableWriteResponse;
import com.google.google.storage.v1.StorageGrpc.StorageStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.util.Timestamps;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/** Implements WritableByteChannel to provide write access to GCS via gRPC. */
public final class GoogleCloudStorageGrpcWriteChannel
    extends BaseAbstractGoogleAsyncWriteChannel<Object>
    implements GoogleCloudStorageItemInfo.Provider {

  // Default GCS upload granularity.
  static final int GCS_MINIMUM_CHUNK_SIZE = 256 * 1024;

  private static final Duration START_RESUMABLE_WRITE_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration QUERY_WRITE_STATUS_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration WRITE_STREAM_TIMEOUT = Duration.ofMinutes(20);
  // Maximum number of automatic retries for each data chunk
  // when writing to underlying channel raises error.
  private static final int WRITE_CHUNK_RETRIES = 10;

  private final StorageStub stub;
  private final StorageResourceId resourceId;
  private final ObjectWriteConditions writeConditions;
  private final Optional<String> requesterPaysProject;
  private final ImmutableMap<String, String> metadata;
  private final boolean checksumsEnabled;

  private GoogleCloudStorageItemInfo completedItemInfo = null;

  GoogleCloudStorageGrpcWriteChannel(
      ExecutorService threadPool,
      StorageStub stub,
      StorageResourceId resourceId,
      AsyncWriteChannelOptions options,
      ObjectWriteConditions writeConditions,
      Optional<String> requesterPaysProject,
      Map<String, String> metadata,
      String contentType) {
    super(threadPool, options);
    this.stub = stub;
    this.resourceId = resourceId;
    this.writeConditions = writeConditions;
    this.requesterPaysProject = requesterPaysProject;
    this.metadata = ImmutableMap.copyOf(metadata);
    this.contentType = contentType;
    this.checksumsEnabled = options.isGrpcChecksumsEnabled();
  }

  @Override
  protected String getResourceString() {
    return resourceId.toString();
  }

  @Override
  public void handleResponse(Object response) {
    checkArgument(
        !response.getBucket().isEmpty(),
        "Got response from service with empty/missing bucketName: %s",
        response);
    Map<String, byte[]> metadata =
        response.getMetadataMap().entrySet().stream()
            .collect(
                toMap(Map.Entry::getKey, entry -> BaseEncoding.base64().decode(entry.getValue())));

    byte[] md5Hash =
        response.getMd5Hash().length() > 0
            ? BaseEncoding.base64().decode(response.getMd5Hash())
            : null;
    byte[] crc32c =
        response.hasCrc32C()
            ? ByteBuffer.allocate(4).putInt(response.getCrc32C().getValue()).array()
            : null;

    completedItemInfo =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId(response.getBucket(), response.getName()),
            Timestamps.toMillis(response.getTimeCreated()),
            Timestamps.toMillis(response.getUpdated()),
            response.getSize(),
            /* location= */ null,
            /* storageClass= */ null,
            response.getContentType(),
            response.getContentEncoding(),
            metadata,
            response.getGeneration(),
            response.getMetageneration(),
            new VerificationAttributes(md5Hash, crc32c));
  }

  @Override
  public void startUpload(PipedInputStream pipeSource) {
    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    try {
      uploadOperation = threadPool.submit(new UploadOperation(pipeSource));
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to start upload for '%s'", resourceId), e);
    }
  }

  private class UploadOperation implements Callable<Object> {

    // Read end of the pipe.
    private final BufferedInputStream pipeSource;
    private final int MAX_BYTES_PER_MESSAGE = MAX_WRITE_CHUNK_BYTES.getNumber();

    UploadOperation(PipedInputStream pipeSource) {
      this.pipeSource = new BufferedInputStream(pipeSource, MAX_BYTES_PER_MESSAGE);
    }

    @Override
    public Object call() throws IOException, InterruptedException {
      // Try-with-resource will close this end of the pipe so that
      // the writer at the other end will not hang indefinitely.
      try (InputStream toClose = pipeSource) {
        return doResumableUpload();
      } catch (Exception e) {
        throw new IOException(
            String.format("Caught exception during upload for '%s'", resourceId), e);
      }
    }

    private Object doResumableUpload() throws IOException, InterruptedException {
      // Send the initial StartResumableWrite request to get an uploadId.
      String uploadId = startResumableUpload();
      InsertChunkResponseObserver responseObserver;

      long writeOffset = 0;
      int retriesAttempted = 0;
      Hasher objectHasher = Hashing.crc32c().newHasher();
      do {
        ByteString chunkData = ByteString.EMPTY;
        responseObserver =
            new InsertChunkResponseObserver(uploadId, chunkData, writeOffset, objectHasher);
        // TODO(b/151184800): Implement per-message timeout, in addition to stream timeout.
        stub.withDeadlineAfter(WRITE_STREAM_TIMEOUT.toMillis(), MILLISECONDS)
            .insertObject(responseObserver);
        responseObserver.done.await();

        if (responseObserver.hasError()) {
          long committedSize = getCommittedWriteSize(uploadId);
          // If the last upload completely failed then reset to where it's marked before last
          // insert. Otherwise, uploaded data chunk needs to be skipped before reading from buffered
          // input stream.
          pipeSource.reset();
          if (committedSize > writeOffset) {
            int uploadedDataChunkSize = Math.toIntExact(committedSize - writeOffset);
            pipeSource.skip(uploadedDataChunkSize);
            writeOffset += uploadedDataChunkSize;
          }
          ++retriesAttempted;
        } else {
          writeOffset += responseObserver.bytesWritten();
          retriesAttempted = 0;
        }

        if (retriesAttempted >= WRITE_CHUNK_RETRIES) {
          throw new IOException(
              String.format("Insert failed for '%s'", resourceId), responseObserver.getError());
        }
      } while (!responseObserver.isFinished());

      return responseObserver.getResponse();
    }

    /** Handler for responses from the Insert streaming RPC. */
    private class InsertChunkResponseObserver
        implements ClientResponseObserver<InsertObjectRequest, Object> {

      private final long writeOffset;
      private final String uploadId;
      private volatile boolean objectFinalized = false;
      // The last error to occur during the streaming RPC. Present only on error.
      private Throwable error;
      // The response from the server, populated at the end of a successful streaming RPC.
      private Object response;
      private ByteString chunkData;
      private Hasher objectHasher;

      // CountDownLatch tracking completion of the streaming RPC. Set on error, or once the request
      // stream is closed.
      final CountDownLatch done = new CountDownLatch(1);

      InsertChunkResponseObserver(
          String uploadId, ByteString chunkData, long writeOffset, Hasher objectHasher) {
        this.uploadId = uploadId;
        this.chunkData = chunkData;
        this.writeOffset = writeOffset;
        this.objectHasher = objectHasher;
      }

      @Override
      public void beforeStart(ClientCallStreamObserver<InsertObjectRequest> requestObserver) {
        requestObserver.setOnReadyHandler(
            new Runnable() {
              @Override
              public void run() {
                if (objectFinalized) {
                  // onReadyHandler may be called after we've closed the request half of the stream.
                  return;
                }

                try {
                  chunkData = readRequestData();
                } catch (IOException e) {
                  error =
                      new IOException(
                          String.format("Failed to read chunk for '%s'", resourceId), e);
                  return;
                }

                InsertObjectRequest.Builder requestBuilder =
                    InsertObjectRequest.newBuilder()
                        .setUploadId(uploadId)
                        .setWriteOffset(writeOffset);

                if (chunkData.size() > 0) {
                  ChecksummedData.Builder requestDataBuilder =
                      ChecksummedData.newBuilder().setContent(chunkData);
                  if (checksumsEnabled) {
                    Hasher chunkHasher = Hashing.crc32c().newHasher();
                    for (ByteBuffer buffer : chunkData.asReadOnlyByteBufferList()) {
                      chunkHasher.putBytes(buffer.duplicate());
                      // TODO(b/7502351): Switch to "concatenating" the chunk-level crc32c values
                      //  if/when the hashing library supports that, to avoid re-scanning all data
                      //  bytes when computing the object-level crc32c.
                      objectHasher.putBytes(buffer.duplicate());
                    }
                    requestDataBuilder.setCrc32C(
                        UInt32Value.newBuilder().setValue(chunkHasher.hash().asInt()));
                  }
                  requestBuilder.setChecksummedData(requestDataBuilder);
                }

                if (objectFinalized) {
                  requestBuilder.setFinishWrite(true);
                  if (checksumsEnabled) {
                    requestBuilder.setObjectChecksums(
                        ObjectChecksums.newBuilder()
                            .setCrc32C(
                                UInt32Value.newBuilder().setValue(objectHasher.hash().asInt())));
                  }
                }
                requestObserver.onNext(requestBuilder.build());

                if (objectFinalized) {
                  // Close the request half of the streaming RPC.
                  requestObserver.onCompleted();
                }
              }

              private ByteString readRequestData() throws IOException {
                // Mark the input stream in case this request fails so that read can be recovered
                // from where it's marked.
                pipeSource.mark(MAX_BYTES_PER_MESSAGE);
                ByteString data =
                    ByteString.readFrom(ByteStreams.limit(pipeSource, MAX_BYTES_PER_MESSAGE));

                objectFinalized = data.size() < MAX_BYTES_PER_MESSAGE || pipeSource.available() > 0;
                return data;
              }
            });
      }

      public Object getResponse() {
        return checkNotNull(response, "Response not present for '%s'", resourceId);
      }

      boolean hasError() {
        return error != null || response == null;
      }

      int bytesWritten() {
        return chunkData.size();
      }

      public Throwable getError() {
        return checkNotNull(error, "Error not present for '%s'", resourceId);
      }

      boolean isFinished() {
        return objectFinalized || hasError();
      }

      @Override
      public void onNext(Object response) {
        this.response = response;
      }

      @Override
      public void onError(Throwable t) {
        error =
            new IOException(
                String.format(
                    "Caught exception for '%s', while uploading to uploadId %s at writeOffset %d",
                    resourceId, uploadId, writeOffset),
                t);
        done.countDown();
      }

      @Override
      public void onCompleted() {
        done.countDown();
      }
    }

    /** Send a StartResumableWriteRequest and return the uploadId of the resumable write. */
    private String startResumableUpload() throws InterruptedException, IOException {
      InsertObjectSpec.Builder insertObjectSpecBuilder =
          InsertObjectSpec.newBuilder()
              .setResource(
                  Object.newBuilder()
                      .setBucket(resourceId.getBucketName())
                      .setName(resourceId.getObjectName())
                      .setContentType(contentType)
                      .putAllMetadata(metadata)
                      .build());
      if (writeConditions.hasContentGenerationMatch()) {
        insertObjectSpecBuilder.setIfGenerationMatch(
            Int64Value.newBuilder().setValue(writeConditions.getContentGenerationMatch()));
      }
      if (writeConditions.hasMetaGenerationMatch()) {
        insertObjectSpecBuilder.setIfMetagenerationMatch(
            Int64Value.newBuilder().setValue(writeConditions.getMetaGenerationMatch()));
      }
      if (requesterPaysProject.isPresent()) {
        insertObjectSpecBuilder.setUserProject(requesterPaysProject.get());
      }
      StartResumableWriteRequest request =
          StartResumableWriteRequest.newBuilder()
              .setInsertObjectSpec(insertObjectSpecBuilder)
              .build();

      SimpleResponseObserver<StartResumableWriteResponse> responseObserver =
          new SimpleResponseObserver<>();

      stub.withDeadlineAfter(START_RESUMABLE_WRITE_TIMEOUT.toMillis(), MILLISECONDS)
          .startResumableWrite(request, responseObserver);
      responseObserver.done.await();

      if (responseObserver.hasError()) {
        throw new IOException(
            String.format("Failed to start resumable upload for '%s'", resourceId),
            responseObserver.getError());
      }

      return responseObserver.getResponse().getUploadId();
    }

    // TODO(b/150892988): Call this to find resume point after a transient error.
    private long getCommittedWriteSize(String uploadId) throws InterruptedException, IOException {
      QueryWriteStatusRequest request =
          QueryWriteStatusRequest.newBuilder().setUploadId(uploadId).build();

      SimpleResponseObserver<QueryWriteStatusResponse> responseObserver =
          new SimpleResponseObserver<>();

      stub.withDeadlineAfter(QUERY_WRITE_STATUS_TIMEOUT.toMillis(), MILLISECONDS)
          .queryWriteStatus(request, responseObserver);
      responseObserver.done.await();

      if (responseObserver.hasError()) {
        throw new IOException(
            String.format("Failed to get committed write size '%s'", resourceId),
            responseObserver.getError());
      }

      return responseObserver.getResponse().getCommittedSize();
    }

    /** Stream observer for single response RPCs. */
    private class SimpleResponseObserver<T> implements StreamObserver<T> {

      // The response from the server, populated at the end of a successful RPC.
      private T response;

      // The last error to occur during the RPC. Present only on error.
      private Throwable error;

      // CountDownLatch tracking completion of the RPC.
      final CountDownLatch done = new CountDownLatch(1);

      public T getResponse() {
        return checkNotNull(response, "Response not present for '%s'", resourceId);
      }

      boolean hasError() {
        return error != null || response == null;
      }

      public Throwable getError() {
        return checkNotNull(error, "Error not present for '%s'", resourceId);
      }

      @Override
      public void onNext(T response) {
        this.response = response;
      }

      @Override
      public void onError(Throwable t) {
        error = new IOException(String.format("Caught exception for '%s'", resourceId), t);
        done.countDown();
      }

      @Override
      public void onCompleted() {
        done.countDown();
      }
    }
  }

  /**
   * Returns non-null only if close() has been called and the underlying object has been
   * successfully committed.
   */
  @Override
  public GoogleCloudStorageItemInfo getItemInfo() {
    return this.completedItemInfo;
  }
}
