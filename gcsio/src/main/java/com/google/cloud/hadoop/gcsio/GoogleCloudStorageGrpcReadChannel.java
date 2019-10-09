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

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.flogger.GoogleLogger;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.google.storage.v1.GetObjectMediaRequest;
import com.google.google.storage.v1.GetObjectMediaResponse;
import com.google.google.storage.v1.GetObjectRequest;
import com.google.google.storage.v1.Object;
import com.google.google.storage.v1.StorageGrpc.StorageStub;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.EOFException;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;

/** Provides seekable read access to GCS via gRPC. */
public class GoogleCloudStorageGrpcReadChannel implements SeekableByteChannel {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Size of buffer to allocate for incoming data.
  // TODO(b/135137108): Figure out what an appropriate default is, and how this impacts
  // performance.
  private static final int DEFAULT_BUFFER_SIZE = 8192;

  // GCS gRPC stub.
  private final StorageStub stub;

  // Name of the bucket containing the object being read.
  private final String bucketName;

  // Name of the object being read.
  private final String objectName;

  // GCS resource/object path, used for logging.
  private final String resourceIdString;

  // True if this channel is open, false otherwise.
  private boolean channelIsOpen = true;

  // Current position in this channel.
  private long currentPosition = 0;

  // Whether to use bounded range requests or streaming requests.
  private boolean isRandomAccess = false;

  // Fine-grained options.
  private final GoogleCloudStorageReadOptions readOptions;

  // Lazy Supplier for object metadata fetched from the server.
  private final MetadataSupplier objectMetadata;

  // Internal content channel for maintaining an open connection across reads.
  private ContentChannel contentChannel;

  // Whether to use bounded range requests or streaming requests.
  private boolean randomAccess;

  /**
   * Constructs an instance of GoogleCloudStorageGrpcReadChannel.
   *
   * @param stub gRPC StorageObjects stub
   * @param bucketName name of the bucket containing the object to read
   * @param objectName name of the object to read
   * @throws IOException on IO error
   */
  public GoogleCloudStorageGrpcReadChannel(StorageStub stub, String bucketName, String objectName)
      throws IOException {
    this(stub, bucketName, objectName, GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Constructs an instance of GoogleCloudStorageGrpcReadChannel.
   *
   * @param stub gRPC StorageObjects stub
   * @param bucketName name of the bucket containing the object to read
   * @param objectName name of the object to read
   * @param readOptions fine-grained options specifying things like retry settings, buffering, etc.
   *     Cannot not be null.
   * @throws IOException on IO error
   */
  public GoogleCloudStorageGrpcReadChannel(
      StorageStub stub,
      String bucketName,
      String objectName,
      @Nonnull GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    this.stub = stub;
    this.bucketName = bucketName;
    this.objectName = objectName;
    this.readOptions = readOptions;
    this.resourceIdString = StringPaths.fromComponents(bucketName, objectName);
    this.objectMetadata = new MetadataSupplier();
    this.randomAccess = readOptions.getFadvise() == Fadvise.RANDOM;
    if (readOptions.getFastFailOnNotFound()) {
      // Fetch the object metadata now so we can fail fast.
      objectMetadata.get();
    }
  }

  public void setMaxRetries(int maxRetries) {
    // TODO(b/137190996): Figure out if creating the stub/channel in this
    // class to support this is worthwhile.
    logger.atWarning().log("setMaxRetries not supported for gRPC writes.");
  }

  @Override
  public int read(ByteBuffer buffer) throws IOException {
    // TODO(b/136088557): Implement footer prefetch?
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    if (currentPosition >= size()) {
      return -1;
    }
    if (buffer.remaining() == 0) {
      return 0;
    }
    getContentChannel(buffer.remaining());

    int bytesRead;
    try {
      bytesRead = contentChannel.read(buffer);
    } catch (IOException e) {
      contentChannel.close();
      throw e;
    }
    currentPosition += bytesRead == -1 ? 0 : bytesRead;
    return bytesRead;
  }

  private void getContentChannel(int readLimit) throws IOException {
    // Seek if viable, or else close the channel.
    if (contentChannel != null && contentChannel.isOpen()) {
      if (randomAccess) {
        contentChannel.close();
      }
      long seekDistance = currentPosition - contentChannel.position;
      boolean shouldReadSequentially =
          readOptions.getFadvise() == Fadvise.SEQUENTIAL
              || seekDistance <= readOptions.getInplaceSeekLimit();
      if (seekDistance > 0 && shouldReadSequentially) {
        try {
          contentChannel.skip(seekDistance);
        } catch (IOException e) {
          logger.atInfo().withCause(e).log("Got an IO exception on contentChannel.skip()");
          contentChannel.close();
        }
      }
      if (currentPosition != contentChannel.position) {
        if (!randomAccess && readOptions.getFadvise() == Fadvise.AUTO) {
          logger.atFine().log(
              "Read from %s to %s (%s threshold). Switching to random IO for '%s'",
              contentChannel.position,
              currentPosition,
              readOptions.getInplaceSeekLimit(),
              resourceIdString);
          randomAccess = true;
        }
        contentChannel.close();
      }
    }

    // Open a new channel if necessary.
    if (contentChannel == null || !contentChannel.isOpen()) {
      contentChannel =
          new ContentChannel(
              currentPosition, randomAccess ? Optional.of(readLimit) : Optional.empty());
    }

    Preconditions.checkState(currentPosition == contentChannel.position);
  }

  @Override
  public boolean isOpen() {
    return channelIsOpen;
  }

  @Override
  public void close() throws IOException {
    channelIsOpen = false;
    if (!channelIsOpen) {
      logger.atFine().log("Ignoring close: channel for '%s' is not open.", resourceIdString);
    } else {
      logger.atFine().log("Closing channel for '%s'", resourceIdString);
      contentChannel.close();
    }
  }

  @Override
  public long position() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return currentPosition;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    Preconditions.checkArgument(
        newPosition >= 0,
        String.format(
            "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
            newPosition, resourceIdString));
    if (newPosition != currentPosition) {
      logger.atFine().log(
          "Seek from %s to %s position for '%s'", currentPosition, newPosition, resourceIdString);
    }
    currentPosition = newPosition;

    return this;
  }

  @Override
  public long size() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return objectMetadata.get().getSize();
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  /** Fetch object metadata from the server. */
  private Object getMetadata() throws IOException {
    GetObjectRequest request =
        GetObjectRequest.newBuilder().setBucket(bucketName).setObject(objectName).build();

    GetMetadataObserver responseObserver = new GetMetadataObserver();

    stub.getObject(request, responseObserver);
    try {
      responseObserver.done.await();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    if (responseObserver.hasError()) {
      Throwable error = responseObserver.getError();
      throw Status.fromThrowable(error).getCode() == Status.Code.NOT_FOUND
          ? GoogleCloudStorageExceptions.createFileNotFoundException(
              bucketName, objectName, new IOException(error))
          : new IOException(error);
    }

    return responseObserver.getResponse();
  }

  /** Handler for responses from the metadata Get RPC. */
  private static class GetMetadataObserver implements StreamObserver<Object> {
    // The response from the server, populated at the end of a successful RPC.
    private Object response;

    // The last error to occur during the RPC. Present only on error.
    private Throwable error;

    // CountDownLatch tracking completion of the RPC.
    final CountDownLatch done = new CountDownLatch(1);

    public boolean hasError() {
      return error != null;
    }

    public Throwable getError() {
      if (error == null) {
        throw new IllegalStateException("error not present.");
      }
      return error;
    }

    public Object getResponse() {
      if (response == null) {
        throw new IllegalStateException("response not present.");
      }
      return response;
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

  private class MetadataSupplier {
    // Singleton cache for the metadata result.
    private final LoadingCache<Singleton, Object> metadataCache;

    public MetadataSupplier() {
      metadataCache =
          CacheBuilder.newBuilder()
              .build(
                  new CacheLoader<Singleton, Object>() {
                    @Override
                    public Object load(Singleton key) throws IOException {
                      return getMetadata();
                    }
                  });
    }

    public Object get() throws IOException {
      try {
        return metadataCache.get(Singleton.INSTANCE);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        } else {
          throw new IOException(e.getCause());
        }
      }
    }

    public void reset() {
      metadataCache.invalidateAll();
    }
  }

  private static enum Singleton {
    INSTANCE;
  }

  /** Internal channel wrapper around data from getMedia requests. */
  private class ContentChannel implements ReadableByteChannel {
    // TODO(b/135138893): Implement BEST_EFFORT generationReadConsistency.
    //                      Make sure to invalidate the objectHasher on generation change.
    // TODO(b/135138159): Implement minRangeRequest
    public long position;

    private PipedInputStream pipeSource;
    private PipedOutputStream pipeSink;
    private boolean contentChannelIsOpen = true;
    private ResponseObserver responseObserver;
    private Optional<Hasher> objectHasher;

    public ContentChannel(long readOffset, Optional<Integer> readLimit) throws IOException {
      int pipeBufferSize =
          readOptions.getBufferSize() > 0 ? readOptions.getBufferSize() : DEFAULT_BUFFER_SIZE;
      pipeSource = new PipedInputStream(pipeBufferSize);
      pipeSink = new PipedOutputStream(pipeSource);
      responseObserver = new ResponseObserver();
      stub.getObjectMedia(buildRequest(readOffset, readLimit), responseObserver);
      position = readOffset;
      boolean enableChecksums = readOptions.getGrpcChecksumsEnabled() && position == 0;
      objectHasher = enableChecksums ? Optional.of(Hashing.crc32c().newHasher()) : Optional.empty();
    }

    @Override
    public int read(ByteBuffer buffer) throws IOException {
      if (!isOpen()) {
        throw new ClosedChannelException();
      }
      if (responseObserver.hasError()) {
        Throwable error = responseObserver.getError();
        Status.Code statusCode = Status.fromThrowable(error).getCode();
        String msg = String.format("Error reading '%s'", resourceIdString);
        if (statusCode == Status.Code.NOT_FOUND) {
          throw GoogleCloudStorageExceptions.createFileNotFoundException(
              bucketName, objectName, new IOException(msg, error));
        } else if (statusCode == Status.Code.OUT_OF_RANGE) {
          throw new EOFException(msg);
        } else {
          throw new IOException(msg, error);
        }
      }

      int totalBytesRead = 0;
      int lastBytesRead = 0;
      byte[] data;
      int arrayOffset;
      if (buffer.hasArray()) {
        data = buffer.array();
        arrayOffset = buffer.arrayOffset();
      } else {
        data = new byte[buffer.remaining()];
        arrayOffset = 0;
      }
      long maxReadBytes = size() - position;

      while (buffer.hasRemaining() && totalBytesRead < maxReadBytes && lastBytesRead != -1) {
        lastBytesRead = pipeSource.read(data, arrayOffset + totalBytesRead, buffer.remaining());
        if (buffer.hasArray()) {
          if (lastBytesRead > 0) {
            buffer.position(buffer.position() + lastBytesRead);
          }
        } else {
          buffer.put(data);
        }
        if (lastBytesRead > 0) {
          totalBytesRead += lastBytesRead;
        }
      }

      position += totalBytesRead;

      if (objectHasher.isPresent()) {
        if (totalBytesRead > 0) {
          objectHasher.get().putBytes(data, arrayOffset, totalBytesRead);
        }
        if (position >= objectMetadata.get().getSize()) {
          int checksum = objectHasher.get().hash().asInt();
          int expectedChecksum = objectMetadata.get().getCrc32C().getValue();
          if (checksum != expectedChecksum) {
            throw new IOException(
                String.format(
                    "Object checksum didn't match. Expected %s, got %s.",
                    expectedChecksum, checksum));
          }
        }
      }
      return totalBytesRead;
    }

    @Override
    public void close() throws IOException {
      responseObserver.onCompleted();
      contentChannelIsOpen = false;
    }

    @Override
    public boolean isOpen() {
      return contentChannelIsOpen;
    }

    public long skip(long seekDistance) throws IOException {
      Preconditions.checkArgument(seekDistance > 0, "seekDistance must be > 0");
      if (!isOpen()) {
        throw new ClosedChannelException();
      }
      logger.atFine().log(
          "Seeking forward %s bytes (inplaceSeekLimit: %s) in-place to position %s for '%s'",
          seekDistance, readOptions.getInplaceSeekLimit(), currentPosition, resourceIdString);
      long totalBytesSkipped = 0;
      long bytesSkipped = 0;
      while (bytesSkipped != -1 && totalBytesSkipped < seekDistance) {
        bytesSkipped = pipeSource.skip(seekDistance - totalBytesSkipped);
        if (bytesSkipped > 0) {
          totalBytesSkipped += bytesSkipped;
        }
      }
      if (totalBytesSkipped > 0) {
        // Invalidate the object hasher since we're not reading sequentially.
        objectHasher = Optional.empty();
      }
      position += totalBytesSkipped;
      return totalBytesSkipped;
    }

    private GetObjectMediaRequest buildRequest(long readOffset, Optional<Integer> readLimit)
        throws IOException {
      GetObjectMediaRequest.Builder requestBuilder =
          GetObjectMediaRequest.newBuilder()
              .setBucket(bucketName)
              .setObject(objectName)
              .setReadOffset(readOffset)
              .setGeneration(objectMetadata.get().getGeneration());

      if (readLimit.isPresent()) {
        requestBuilder.setReadLimit(readLimit.get());
      }

      return requestBuilder.build();
    }

    /** Handler for responses from the GetObjectMedia streaming RPC. */
    private class ResponseObserver implements StreamObserver<GetObjectMediaResponse> {
      // The last error to occur during the RPC. Present only on error.
      private Throwable error;

      public boolean hasError() {
        return error != null;
      }

      public Throwable getError() {
        if (error == null) {
          throw new IllegalStateException("error not present.");
        }
        return error;
      }

      @Override
      public void onNext(GetObjectMediaResponse response) {
        // TODO(b/134521856): Calculate and verify message checksums.
        try {
          response.getChecksummedData().getContent().writeTo(pipeSink);
        } catch (IOException e) {
          error = e;
          onCompleted();
        }
      }

      @Override
      public void onError(Throwable t) {
        error = t;
      }

      @Override
      public void onCompleted() {}
    }
  }
}
