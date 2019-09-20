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

import static com.google.google.storage.v1.ServiceConstants.Values.MAX_WRITE_CHUNK_BYTES;

import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.BaseAbstractGoogleAsyncWriteChannel;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/** Implements WritableByteChannel to provide write access to GCS via gRPC. */
public final class GoogleCloudStorageGrpcWriteChannel
    extends BaseAbstractGoogleAsyncWriteChannel<Object>
    implements GoogleCloudStorageItemInfo.Provider {

  private final Object object;
  private final ObjectWriteConditions writeConditions;
  private final Optional<String> requesterPaysProject;
  private final StorageStub stub;
  private final boolean checksumsEnabled;

  private GoogleCloudStorageItemInfo completedItemInfo = null;

  public GoogleCloudStorageGrpcWriteChannel(
      ExecutorService threadPool,
      StorageStub stub,
      String bucketName,
      String objectName,
      AsyncWriteChannelOptions options,
      ObjectWriteConditions writeConditions,
      Optional<String> requesterPaysProject,
      Map<String, String> objectMetadata,
      String contentType) {
    super(threadPool, options);
    this.stub = stub;
    this.writeConditions = writeConditions;
    this.requesterPaysProject = requesterPaysProject;
    this.checksumsEnabled = options.isGrpcChecksumsEnabled();
    this.object =
        Object.newBuilder()
            .setName(objectName)
            .setBucket(bucketName)
            .setContentType(contentType)
            .putAllMetadata(objectMetadata)
            .build();
  }

  @Override
  public void handleResponse(Object response) {
    Map<String, byte[]> metadata =
        Maps.transformValues(
            response.getMetadataMap(),
            value -> {
              try {
                return BaseEncoding.base64().decode(value);
              } catch (IllegalArgumentException iae) {
                logger.atSevere().withCause(iae).log(
                    "Failed to parse base64 encoded attribute value %s - %s", value, iae);
                return null;
              }
            });

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
            Timestamps.toMillis(response.getUpdated()),
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
  public void startUpload(PipedInputStream pipeSource) throws IOException {
    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    uploadOperation = threadPool.submit(new UploadOperation(pipeSource));
  }

  private class UploadOperation implements Callable<Object> {
    // Read end of the pipe.
    private final BufferedInputStream pipeSource;

    public UploadOperation(InputStream pipeSource) {
      // Buffer the input stream by 1 byte so we can peek ahead for the end of the stream.
      this.pipeSource = new BufferedInputStream(pipeSource, 1);
    }

    @Override
    public Object call() throws IOException, InterruptedException {
      // Try-with-resource will close this end of the pipe so that
      // the writer at the other end will not hang indefinitely.
      try (InputStream uploadPipeSource = pipeSource) {
        return doResumableUpload();
      }
    }

    private Object doResumableUpload() throws IOException, InterruptedException {
      // Send the initial StartResumableWrite request to get an uploadId.
      String uploadId = getUploadId();
      // Protect against chunkSize changing mid RPC.
      int chunkSize = uploadChunkSize;
      InsertChunkResponseObserver responseObserver;
      // This ByteString holds all the data for the current streaming RPC that hasn't
      // yet been confirmed committed. This should never be larger than `chunkSize`
      // defined above.
      ByteString chunkData = ByteString.EMPTY;
      long writeOffset = 0;
      Hasher objectHasher = Hashing.crc32c().newHasher();
      do {
        responseObserver =
            new InsertChunkResponseObserver(
                uploadId, chunkData, chunkSize, writeOffset, objectHasher);
        stub.insertObject(responseObserver);
        responseObserver.done.await();

        chunkData = responseObserver.getChunkData();
        // We can assume all bytes were written on a successful final request.
        long bytesCommitted =
            responseObserver.isFinished()
                ? writeOffset + chunkData.size()
                : getCommittedWriteSize(uploadId);

        // This should never fail since uploadChunkSize is an int.
        int chunkBytesCommitted = Math.toIntExact(bytesCommitted - writeOffset);
        if (checksumsEnabled && !responseObserver.isFinished()) {
          for (ByteBuffer buffer : chunkData.asReadOnlyByteBufferList()) {
            objectHasher.putBytes(buffer);
          }
        }
        chunkData = chunkData.substring(chunkBytesCommitted);
        writeOffset = bytesCommitted;
      } while (!responseObserver.isFinished());

      if (responseObserver.hasError()) {
        logger.atSevere().withCause(responseObserver.getError()).log("Error:");
        throw new IOException("Insert failed", responseObserver.getError());
      }

      return responseObserver.getResponse();
    }

    /** Handler for responses from the Insert streaming RPC. */
    private class InsertChunkResponseObserver
        implements ClientResponseObserver<InsertObjectRequest, Object> {
      private final int MAX_BYTES_PER_REQUEST = MAX_WRITE_CHUNK_BYTES.getNumber();

      private final long chunkStart;
      private final int chunkSize;
      private final String uploadId;
      private volatile boolean objectFinalized = false;
      // The last error to occur during the streaming RPC. Present only on error.
      private Throwable error;
      // The response from the server, populated at the end of a successful streaming RPC.
      private Object response;
      private ByteString chunkData;
      private Hasher objectHasher;
      // The number of bytes written in this streaming RPC.
      private int chunkBytesWritten = 0;

      // CountDownLatch tracking completion of the streaming RPC. Set on error, or once the request
      // stream is closed.
      final CountDownLatch done = new CountDownLatch(1);

      InsertChunkResponseObserver(
          String uploadId,
          ByteString chunkData,
          int chunkSize,
          long chunkStart,
          Hasher objectHasher) {
        this.uploadId = uploadId;
        this.chunkSize = chunkSize;
        this.chunkStart = chunkStart;
        this.objectHasher = objectHasher;
        this.chunkData = chunkData;
      }

      @Override
      public void beforeStart(final ClientCallStreamObserver<InsertObjectRequest> requestObserver) {
        requestObserver.setOnReadyHandler(
            new Runnable() {
              @Override
              public void run() {
                if (chunkFinished()) {
                  // onReadyHandler may be called after we've closed the request half of the stream.
                  return;
                }

                ByteString data;
                try {
                  data = getRequestData();
                } catch (IOException e) {
                  error = e;
                  return;
                }

                InsertObjectRequest.Builder requestBuilder =
                    InsertObjectRequest.newBuilder()
                        .setUploadId(uploadId)
                        .setWriteOffset(chunkStart + chunkBytesWritten);

                if (data.size() > 0) {
                  ChecksummedData.Builder requestDataBuilder =
                      ChecksummedData.newBuilder().setContent(data);
                  if (checksumsEnabled) {
                    Hasher hasher = Hashing.crc32c().newHasher();
                    for (ByteBuffer buffer : data.asReadOnlyByteBufferList()) {
                      hasher.putBytes(buffer);
                    }
                    int checksum = hasher.hash().asInt();
                    requestDataBuilder.setCrc32C(UInt32Value.newBuilder().setValue(checksum));
                  }
                  requestBuilder.setChecksummedData(requestDataBuilder);
                  chunkBytesWritten += data.size();
                }

                if (objectFinalized) {
                  requestBuilder.setFinishWrite(objectFinalized);
                  if (checksumsEnabled) {
                    // For the final request, we need to update the hash now so we can add the
                    // final object checksum to the request.
                    for (ByteBuffer buffer : data.asReadOnlyByteBufferList()) {
                      objectHasher.putBytes(buffer);
                    }
                    requestBuilder.setObjectChecksums(
                        ObjectChecksums.newBuilder()
                            .setCrc32C(
                                UInt32Value.newBuilder().setValue(objectHasher.hash().asInt())));
                  }
                }
                requestObserver.onNext(requestBuilder.build());

                if (chunkFinished()) {
                  // Close the request half of the streaming RPC.
                  requestObserver.onCompleted();
                }
              }

              private ByteString getRequestData() throws IOException {
                // Read into chunkData until it has MAX_BYTES_PER_REQUEST or the InputStream is
                // finished.
                int initialBytes = Math.max(chunkData.size() - chunkBytesWritten, 0);
                int maxBytesToRead = Math.max(MAX_BYTES_PER_REQUEST - initialBytes, 0);
                int bytesToRead =
                    Math.min(Math.max(chunkSize - chunkData.size(), 0), maxBytesToRead);
                int bytesRead = readChunkData(bytesToRead);

                return chunkData.substring(
                    chunkBytesWritten, chunkBytesWritten + initialBytes + bytesRead);
              }

              private int readChunkData(int bytesToRead) throws IOException {
                InputStream limitedStream = ByteStreams.limit(pipeSource, bytesToRead);
                ByteString newData = ByteString.readFrom(limitedStream);
                int bytesRead = newData.size();
                chunkData = chunkData.concat(newData);

                // Verify that chunkData never gets larger than the amount of data we're sending in
                // the current RPC (so we don't have a memory leak).
                Preconditions.checkState(chunkData.size() <= chunkSize);

                // Set objectFinalized if there is no more data, looking ahead 1 byte in the buffer
                // if neccessary. This lets us avoid sending an extra request with no data just to
                // set the finish_write flag.
                pipeSource.mark(1);
                objectFinalized = bytesRead < bytesToRead || pipeSource.read() == -1;
                pipeSource.reset();

                return bytesRead;
              }

              private boolean chunkFinished() {
                return objectFinalized || chunkData.size() == chunkSize;
              }
            });
      }

      public boolean hasResponse() {
        return response != null;
      }

      public Object getResponse() {
        if (response == null) {
          throw new IllegalStateException("Response not present.");
        }
        return response;
      }

      public boolean hasError() {
        return error != null;
      }

      public Throwable getError() {
        if (error == null) {
          throw new IllegalStateException("Error not present.");
        }
        return error;
      }

      public boolean isFinished() {
        return objectFinalized || hasError();
      }

      public ByteString getChunkData() {
        return chunkData;
      }

      @Override
      public void onNext(Object response) {
        this.response = response;
      }

      @Override
      public void onError(Throwable t) {
        error = t;
        done.countDown();
      }

      @Override
      public void onCompleted() {
        done.countDown();
      }
    }

    /** Send a StartResumableWriteRequest and return the uploadId of the resumable write. */
    private String getUploadId() throws InterruptedException, IOException {
      InsertObjectSpec.Builder insertObjectSpecBuilder =
          InsertObjectSpec.newBuilder().setResource(object);
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

      stub.startResumableWrite(request, responseObserver);
      responseObserver.done.await();

      if (responseObserver.hasError()) {
        throw new IOException(
            "StartResumableWriteRequest failed", responseObserver.getError().getCause());
      }

      return responseObserver.getResponse().getUploadId();
    }

    private long getCommittedWriteSize(String uploadId) throws InterruptedException, IOException {
      QueryWriteStatusRequest request =
          QueryWriteStatusRequest.newBuilder().setUploadId(uploadId).build();

      SimpleResponseObserver<QueryWriteStatusResponse> responseObserver =
          new SimpleResponseObserver<>();

      stub.queryWriteStatus(request, responseObserver);
      responseObserver.done.await();

      if (responseObserver.hasError()) {
        throw new IOException(
            "QueryWriteStatusRequest failed", responseObserver.getError().getCause());
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

      public boolean hasResponse() {
        return response != null;
      }

      public T getResponse() {
        if (response == null) {
          throw new IllegalStateException("Response not present.");
        }
        return response;
      }

      public boolean hasError() {
        return error != null;
      }

      public Throwable getError() {
        if (error == null) {
          throw new IllegalStateException("Error not present.");
        }
        return error;
      }

      @Override
      public void onNext(T response) {
        this.response = response;
      }

      @Override
      public void onError(Throwable t) {
        error = t;
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
