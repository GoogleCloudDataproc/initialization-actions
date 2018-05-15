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

package com.google.cloud.hadoop.util;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.InputStreamContent;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for streaming uploads to Google Cloud Services.
 *
 * <p>As per WriteableByteChannel semantics, only a single writer may be in write() at a time.
 * Close(), initalize(), etc have no thread-safety guarantees and their use should be coordinated
 * by callers to be not called concurrent to any write operation.
 * @param <T> API request type that we will be uploading.
 * @param <S> API response type that will be the result of the upload.
 */
public abstract class AbstractGoogleAsyncWriteChannel
    <T extends AbstractGoogleClientRequest<S>, S>
    implements WritableByteChannel {

  // Default size of upload buffer.
  public static final int UPLOAD_PIPE_BUFFER_SIZE_DEFAULT = 1 * 1024 * 1024;

  // Default GCS upload granularity.
  public static final int GCS_UPLOAD_GRANULARITY = 8 * 1024 * 1024;

  // Chunk size to use.
  public static final int UPLOAD_CHUNK_SIZE_DEFAULT =
      Runtime.getRuntime().maxMemory() < 512 * 1024 * 1024
           ? GCS_UPLOAD_GRANULARITY : 8 * GCS_UPLOAD_GRANULARITY;

  /**
   * Sets the ClientRequestHelper to be used instead of calling final methods in client requests.
   */
  @VisibleForTesting
  public void setClientRequestHelper(ClientRequestHelper<S> helper) {
    clientRequestHelper = helper;
  }

  // Content type
  private String contentType;


  // Upper limit on object size.
  // We use less than 250GB limit to avoid potential boundary errors
  private static final long UPLOAD_MAX_SIZE = 249 * 1024 * 1024 * 1024L;

  // ClientRequestHelper to be used instead of calling final methods in client requests.
  private ClientRequestHelper<S> clientRequestHelper = new ClientRequestHelper<>();

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractGoogleAsyncWriteChannel.class);

  // Buffering used in the upload path:
  // There are a series of buffers used along the upload path. It is important to understand their
  // function before tweaking their values.
  //
  // Note: Most values are already tweaked based on performance measurements. If you want to change
  // buffer sizes, you should change only 1 buffer size at a time to make sure you understand
  // the correlation between various buffers and their characteristics.
  //
  // Upload path:
  // Uploading a file involves the following steps:
  // -- caller creates a write stream. It involves creating a pipe between data writer (controlled
  // by the caller) and data uploader.
  // The writer and the uploader are on separate threads. That is, pipe operation is asynchronous
  // between its
  // two ends.
  // -- caller puts data in a ByteBuffer and calls write(ByteBuffer). The write() method starts
  // writing into sink end of the pipe. It blocks if pipe buffer is full till the other end
  // reads data to make space.
  // -- MediaHttpUploader code keeps on reading from the source end of the pipe till it has
  // uploadBufferSize amount of data.
  //
  // The following buffers are involved along the above path:
  // -- ByteBuffer passed by caller. We have no control over its size.
  //
  // -- Pipe buffer.
  // size = UPLOAD_PIPE_BUFFER_SIZE_DEFAULT (1 MB)
  // Increasing size does not have noticeable difference on performance.
  //
  // code.
  // size = UPLOAD_CHUNK_SIZE_DEFAULT (64 MB)

  // A pipe that connects write channel used by caller to the input stream used by GCS uploader.
  // The uploader reads from input stream, which blocks till a caller writes some data to the
  // write channel (pipeSinkChannel below). The pipe is formed by connecting pipeSink to pipeSource
  private PipedOutputStream pipeSink;
  private PipedInputStream pipeSource;
  private ExecutorService threadPool;
  private boolean isInitialized = false;

  // Chunk size to use.
  @VisibleForTesting
  private int uploadBufferSize = UPLOAD_CHUNK_SIZE_DEFAULT;

  // Size of buffer used by upload pipe.
  private int pipeBufferSize = UPLOAD_PIPE_BUFFER_SIZE_DEFAULT;

  // A channel wrapper over pipeSink.
  private WritableByteChannel pipeSinkChannel;

  // Upload operation that takes place on a separate thread.
  private Future<S> uploadOperation;

  // Previously this allowed faster writes. This is no longer true. Therefore default to false.
  // TODO: Remove this flag and all related code.
  @VisibleForTesting
  private boolean limitFileSizeTo250Gb = false;

  // When enabled, we get higher throughput for writing small files.
  private boolean directUploadEnabled = false;

  /**
   * Construct a new channel using the given ExecutorService to run background uploads.
   * @param threadPool
   * @param options
   */
  public AbstractGoogleAsyncWriteChannel(
      ExecutorService threadPool,
      AsyncWriteChannelOptions options) {
    this.threadPool = threadPool;
    enableFileSizeLimit250Gb(options.isFileSizeLimitedTo250Gb());
    setUploadBufferSize(options.getUploadBufferSize());
    setDirectUploadEnabled(options.isDirectUploadEnabled());
    setContentType("application/octet-stream");
  }

  /**
   * Create an API request to upload the given InputStreamContent.
   * @param inputStream
   * @return An initialized request.
   * @throws IOException
   */
  public abstract T createRequest(InputStreamContent inputStream) throws IOException;

  /**
   * Handle the API response.
   *
   * <p>This method is invoked after the upload has completed on the same thread that
   * invokes close().
   * @param response The API response object.
   */
  public void handleResponse(S response) throws IOException {}

  /**
   * Derived classes may optionally intercept an IOException thrown from the execute() method of
   * a prepared request that came from {@link #createRequest}, and return a reconstituted
   * "response" object if the IOException can be handled as a success; for example, if the caller
   * already has an identifier for an object, and the response is used solely for obtaining the
   * same identifier, and the IOException is a handled "409 Already Exists" type of exception,
   * then the derived class may override this method to return the expected "identifier" response.
   * Return null to let the exception propagate through correctly.
   */
  public S createResponseFromException(IOException ioe) {
    return null;
  }

  /**
   * Sets size of upload buffer used.
   */
  public void setUploadBufferSize(int bufferSize) {
    Preconditions.checkArgument(bufferSize > 0,
        "Upload buffer size must be great than 0.");
    Preconditions.checkArgument(bufferSize % MediaHttpUploader.MINIMUM_CHUNK_SIZE == 0,
        "Upload buffer size must be a multiple of MediaHttpUploader.MINIMUM_CHUNK_SIZE");
    if ((bufferSize > GCS_UPLOAD_GRANULARITY) && (bufferSize % GCS_UPLOAD_GRANULARITY != 0)) {
      LOG.warn("Upload buffer size should be a multiple of {} for best performance, got {}",
          GCS_UPLOAD_GRANULARITY, bufferSize);
    }
    uploadBufferSize = bufferSize;
  }

  /**
   * Enables or disables hard limit of 250GB on size of uploaded files.
   *
   * If enabled, we get very high write throughput but writing files larger than UPLOAD_MAX_SIZE
   * will not succeed. Set it to false to allow larger files at lower throughput.
   */
  public void enableFileSizeLimit250Gb(boolean enableLimit) {
    limitFileSizeTo250Gb = enableLimit;
  }

  /**
   * Enables or disables direct uploads.
   *
   * @see MediaHttpUploader#setDirectUploadEnabled(boolean)
   */
  public void setDirectUploadEnabled(boolean enableDirectUpload) {
    directUploadEnabled = enableDirectUpload;
  }

  /**
   * Returns true if direct media uploads are enabled.
   */
  public boolean isDirectUploadEnabled() {
    return directUploadEnabled;
  }

  /**
   * Writes contents of the given buffer to this channel.
   *
   * Note: The data that one writes gets written to a pipe which must not block
   * if the pipe has sufficient buffer space. A success code returned from this method
   * does not mean that the specific data was successfully written to the underlying
   * storage. It simply means that there is no error at present. The data upload
   * may encounter an error on a separate thread. Such error is not ignored;
   * it shows up as an exception during a subsequent call to write() or close().
   * The only way to be sure of successful upload is when the close() method
   * returns successfully.
   *
   * @param buffer buffer to write
   * @throws IOException on IO error
   */
  @Override
  public synchronized int write(ByteBuffer buffer)
      throws IOException {
    // We haven't set up properly
    throwIfNotInitialized();

    throwIfNotOpen();

    // No point in writing further if upload failed on another thread.
    if (uploadOperation.isDone()) {
      waitForCompletionAndThrowIfUploadFailed();
    }

    return pipeSinkChannel.write(buffer);
  }

  /**
   * Tells whether this channel is open.
   *
   * @return a value indicating whether this channel is open
   */
  @Override
  public boolean isOpen() {
    return (pipeSinkChannel != null) && pipeSinkChannel.isOpen();
  }

  /**
   * Closes this channel.
   *
   * Note:
   * The method returns only after all data has been successfully written to GCS
   * or if there is a non-retry-able error.
   *
   * @throws IOException on IO error
   */
  @Override
  public void close() throws IOException {
    throwIfNotInitialized();
    if (isOpen()) {
      try {
        pipeSinkChannel.close();
        handleResponse(waitForCompletionAndThrowIfUploadFailed());
      } finally {
        pipeSinkChannel = null;
        pipeSink = null;
        pipeSource = null;
        uploadOperation = null;
      }
    }
  }

  /**
   * Initialize this channel object for writing.
   *
   * @throws IOException
   */
  public void initialize() throws IOException {
    // Create a pipe such that its one end is connected to the input stream used by
    // the uploader and the other end is the write channel used by the caller.
    pipeSource = new PipedInputStream(pipeBufferSize);
    pipeSink = new PipedOutputStream(pipeSource);
    pipeSinkChannel = Channels.newChannel(pipeSink);

    // Connect pipe-source to the stream used by uploader.
    InputStreamContent objectContentStream =
        new InputStreamContent(contentType, pipeSource);
    // Indicate that we do not know length of file in advance.
    objectContentStream.setLength(-1);
    objectContentStream.setCloseInputStream(false);

    T request = createRequest(objectContentStream);
    request.setDisableGZipContent(true);

    // Set a larger backend upload-chunk granularity than system defaults.
    HttpHeaders headers = clientRequestHelper.getRequestHeaders(request);
    headers.set("X-Goog-Upload-Desired-Chunk-Granularity",
        Math.min(GCS_UPLOAD_GRANULARITY, uploadBufferSize));

    // Legacy check. Will be phased out.
    if (limitFileSizeTo250Gb) {
      headers.set("X-Goog-Upload-Max-Raw-Size", UPLOAD_MAX_SIZE);
    }

    // Change chunk size from default value (10MB) to one that yields higher performance.
    clientRequestHelper.setChunkSize(request, uploadBufferSize);

    // Given that the two ends of the pipe must operate asynchronous relative
    // to each other, we need to start the upload operation on a separate thread.
    uploadOperation = threadPool.submit(new UploadOperation(request, pipeSource));

    isInitialized = true;
  }

  class UploadOperation implements Callable<S> {
    // Object to be uploaded. This object declared final for safe object publishing.
    private final T uploadObject;

    // Read end of the pipe. This object declared final for safe object publishing.
    private final InputStream pipeSource;

    /**
     * Constructs an instance of UploadOperation.
     */
    public UploadOperation(T uploadObject, InputStream pipeSource) {
      this.uploadObject = uploadObject;
      this.pipeSource = pipeSource;
    }

    @Override
    public S call() throws Exception {
      Exception exception = null;
      try {
        return uploadObject.execute();
      } catch (IOException ioe) {
        exception = ioe;
        S response = createResponseFromException(ioe);
        if (response != null) {
          LOG.warn(String.format(
              "Received IOException, but successfully converted to response '%s'.", response),
              ioe);
          return response;
        }
        LOG.error("Exception not convertible into handled response", ioe);
      } catch (Exception e) {
        exception = e;
        LOG.error("Exception uploading. ", e);
      } finally {
        try {
          // Close this end of the pipe so that the writer at the other end
          // will not hang indefinitely.
          pipeSource.close();
        } catch (IOException ioe) {
          LOG.error("Error trying to close pipe.source()", ioe);

          if (exception != null) {
            exception.addSuppressed(ioe);
          } else {
            exception = ioe;
          }
        }
      }
      throw exception;
    }
  }

  /**
   * Sets the contentType. This must be called before initialize()
   * for any effect.
   *
   */
  protected void setContentType(String contentType) {
    this.contentType = contentType;
  }


  /**
   * Throws if this channel is not currently open.
   *
   * @throws IOException on IO error
   */
  private void throwIfNotOpen()
      throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  /**
   * Throws if upload operation failed. Propagates any errors.
   *
   * @throws IOException on IO error
   */
  private S waitForCompletionAndThrowIfUploadFailed()
      throws IOException {
    try {
      return uploadOperation.get();
    } catch (InterruptedException e) {
      // If we were interrupted, we need to cancel the upload operation.
      uploadOperation.cancel(true);
      IOException exception = new ClosedByInterruptException();
      exception.addSuppressed(e);
      throw exception;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  /**
   * Throws if initialize has not been called.
   */
  private void throwIfNotInitialized() {
    if (!isInitialized) {
      throw new IllegalStateException("initialize() must be invoked before use.");
    }
  }

}
