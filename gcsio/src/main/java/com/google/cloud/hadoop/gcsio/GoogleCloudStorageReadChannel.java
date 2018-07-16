/*
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.Fadvise;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.ResilientOperation.CheckedCallable;
import com.google.cloud.hadoop.util.RetryBoundedBackOff;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.ByteStreams;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides seekable read access to GCS. */
public class GoogleCloudStorageReadChannel implements SeekableByteChannel {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageReadChannel.class);

  // Size of buffer to allocate for skipping bytes in-place when performing in-place seeks.
  @VisibleForTesting static final int SKIP_BUFFER_SIZE = 8192;

  // GCS access instance.
  private final Storage gcs;

  // Name of the bucket containing the object being read.
  private final String bucketName;

  // Name of the object being read.
  private final String objectName;

  // GCS resource/object path, used for logging.
  private final String resourceIdString;

  // GCS object content channel.
  @VisibleForTesting ReadableByteChannel contentChannel;

  // True if this channel is open, false otherwise.
  private boolean channelIsOpen = true;

  // Current position in this channel, it could be different from contentChannelPosition if
  // position(long) method calls were made without calls to read(ByteBuffer) method.
  @VisibleForTesting protected long currentPosition = 0;

  // Current read position in the contentChannel.
  //
  // When a caller calls position(long) to set stream position, we record the target position
  // and defer the actual seek operation until the caller tries to read from the channel.
  // This allows us to avoid an unnecessary seek to position 0 that would take place on creation
  // of this instance in cases where caller intends to start reading at some other offset.
  // If contentChannelPosition is not the same as currentPosition, it indicates that a target
  // position has been set but the actual seek operation is still pending.
  @VisibleForTesting long contentChannelPosition = -1;

  // Size of the object being read.
  private long size = -1;

  // Size of the contentChannel.
  private long contentChannelEnd = -1;

  // Whether to use bounded range requests or streaming requests.
  @VisibleForTesting boolean randomAccess;

  // Maximum number of automatic retries when reading from the underlying channel without making
  // progress; each time at least one byte is successfully read, the counter of attempted retries
  // is reset.
  // TODO(user): Wire this setting out to GHFS; it should correspond to adding the wiring for
  // setting the equivalent value inside HttpRequest.java that determines the low-level retries
  // during "execute()" calls. The default in HttpRequest.java is also 10.
  private int maxRetries = 10;

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private final ApiErrorExtractor errorExtractor;

  // Request helper to use to set extra headers
  private final ClientRequestHelper<StorageObject> clientRequestHelper;

  // Fine-grained options.
  private final GoogleCloudStorageReadOptions readOptions;

  // Sleeper used for waiting between retries.
  private Sleeper sleeper = Sleeper.DEFAULT;

  // The clock used by ExponentialBackOff to determine when the maximum total elapsed time has
  // passed doing a series of retries.
  private NanoClock clock = NanoClock.SYSTEM;

  // Lazily initialized BackOff for sleeping between retries; only ever initialized if a retry is
  // necessary.
  private Supplier<BackOff> backOff = Suppliers.memoize(this::createBackOff);

  // read operation gets its own Exponential Backoff Strategy,
  // to avoid interference with other operations in nested retries.
  private Supplier<BackOff> readBackOff = Suppliers.memoize(this::createBackOff);

  // Used as scratch space when reading bytes just to discard them when trying to perform small
  // in-place seeks.
  private byte[] skipBuffer = null;

  // Whether object content is gzip-encoded.
  private boolean gzipEncoded = false;

  // Prefetched footer content.
  // TODO(b/110832992):
  // 1. Test showing footer prefetch avoids another request to GCS.
  // 2. Test showing shorter footer prefetch does not cause any problems.
  // 3. Test that footer prefetch always disabled for gzipped files.
  private byte[] footerContent;

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   *
   * @param gcs storage object instance
   * @param bucketName name of the bucket containing the object to read
   * @param objectName name of the object to read
   * @param requestHelper a ClientRequestHelper used to set any extra headers
   * @throws java.io.FileNotFoundException if the given object does not exist
   * @throws IOException on IO error
   */
  public GoogleCloudStorageReadChannel(
      Storage gcs,
      String bucketName,
      String objectName,
      ApiErrorExtractor errorExtractor,
      ClientRequestHelper<StorageObject> requestHelper)
      throws IOException {
    this(
        gcs,
        bucketName,
        objectName,
        errorExtractor,
        requestHelper,
        GoogleCloudStorageReadOptions.DEFAULT);
  }

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   *
   * @param gcs storage object instance
   * @param bucketName name of the bucket containing the object to read
   * @param objectName name of the object to read
   * @param requestHelper a ClientRequestHelper used to set any extra headers
   * @param readOptions fine-grained options specifying things like retry settings, buffering, etc.
   *     Could not be null.
   * @throws java.io.FileNotFoundException if the given object does not exist
   * @throws IOException on IO error
   */
  public GoogleCloudStorageReadChannel(
      Storage gcs,
      String bucketName,
      String objectName,
      ApiErrorExtractor errorExtractor,
      ClientRequestHelper<StorageObject> requestHelper,
      @Nonnull GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    this.gcs = gcs;
    this.clientRequestHelper = requestHelper;
    this.bucketName = bucketName;
    this.objectName = objectName;
    this.errorExtractor = errorExtractor;
    this.readOptions = readOptions;
    this.resourceIdString = StorageResourceId.createReadableString(bucketName, objectName);
    if (readOptions.getFadvise() == Fadvise.SEQUENTIAL
        || readOptions.getFooterPrefetchSize() == 0) {
      initEncodingAndSize();
    } else {
      initEncodingAndSizeAndPrefetchFooter();
    }
    this.randomAccess = !gzipEncoded && readOptions.getFadvise() == Fadvise.RANDOM;
    checkEncodingAndAccess();
    int prefetchedFooterSize = this.footerContent == null ? 0 : this.footerContent.length;
    LOG.debug(
        "Created and initialized new channel"
            + " (encoding={}, size={}, randomAccess={}, prefetchedFooterSize={}) for '{}'",
        gzipEncoded ? "gzip" : "other", size, randomAccess, prefetchedFooterSize, resourceIdString);
  }

  /**
   * Used for unit testing only. Do not use elsewhere.
   *
   * <p>Constructs an instance of GoogleCloudStorageReadChannel.
   *
   * @param readOptions fine-grained options specifying things like retry settings, buffering, etc.
   *     Could not be null.
   * @throws IOException on IO error
   */
  @VisibleForTesting
  protected GoogleCloudStorageReadChannel(@Nonnull GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    this(
        /* gcs= */ null,
        /* bucketName= */ null,
        /* objectName= */ null,
        /* errorExtractor= */ null,
        /* requestHelper= */ null,
        readOptions);
  }

  /** Sets the Sleeper used for sleeping between retries. */
  @VisibleForTesting
  void setSleeper(Sleeper sleeper) {
    Preconditions.checkArgument(sleeper != null, "sleeper must not be null!");
    this.sleeper = sleeper;
  }

  /** Sets the clock to be used for determining when max total time has elapsed doing retries. */
  @VisibleForTesting
  void setNanoClock(NanoClock clock) {
    Preconditions.checkArgument(clock != null, "clock must not be null!");
    this.clock = clock;
  }

  /**
   * Sets the back-off for determining sleep duration between retries.
   *
   * @param backOff {@link BackOff} to use for retries, could not be null.
   */
  @VisibleForTesting
  void setBackOff(BackOff backOff) {
    this.backOff = Suppliers.ofInstance(checkNotNull(backOff, "backOff could not be null"));
  }

  /**
   * Sets the back-off for determining sleep duration between read retries.
   *
   * @param backOff {@link BackOff} to use for read retries, could not be null.
   */
  void setReadBackOff(BackOff backOff) {
    this.readBackOff = Suppliers.ofInstance(checkNotNull(backOff, "backOff could not be null"));
  }

  /** Gets the back-off used for determining sleep duration between retries. */
  @VisibleForTesting
  BackOff getBackOff() {
    return backOff.get();
  }

  /** Gets the back-off used for determining sleep duration between read retries. */
  @VisibleForTesting
  BackOff getReadBackOff() {
    return readBackOff.get();
  }

  /** Creates new generic BackOff used for retries. */
  @VisibleForTesting
  ExponentialBackOff createBackOff() {
    return new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(readOptions.getBackoffInitialIntervalMillis())
        .setRandomizationFactor(readOptions.getBackoffRandomizationFactor())
        .setMultiplier(readOptions.getBackoffMultiplier())
        .setMaxIntervalMillis(readOptions.getBackoffMaxIntervalMillis())
        .setMaxElapsedTimeMillis(readOptions.getBackoffMaxElapsedTimeMillis())
        .setNanoClock(clock)
        .build();
  }

  /**
   * Sets the number of times to automatically retry by re-opening the underlying contentChannel
   * whenever an exception occurs while reading from it. The count of attempted retries is reset
   * whenever at least one byte is successfully read, so this number of retries refers to retries
   * made without achieving any forward progress.
   */
  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  /**
   * Reads from this channel and stores read data in the given buffer.
   *
   * <p>On unexpected failure, will attempt to close the channel and clean up state.
   *
   * @param buffer buffer to read data into
   * @return number of bytes read or -1 on end-of-stream
   * @throws IOException on IO error
   */
  @Override
  public int read(ByteBuffer buffer) throws IOException {
    throwIfNotOpen();

    checkState(
        size >= 0,
        "size should be initialized already, but was %s for '%s'", size, resourceIdString);

    // Don't try to read if the buffer has no space.
    if (buffer.remaining() == 0) {
      return 0;
    }

    LOG.debug(
        "Reading {} bytes at {} position from '{}'",
        buffer.remaining(), currentPosition, resourceIdString);

    // Do not perform any further reads if we already read everything from this channel.
    if (currentPosition == size) {
      return -1;
    }

    int totalBytesRead = 0;
    int retriesAttempted = 0;

    // We read from a streaming source. We may not get all the bytes we asked for
    // in the first read. Therefore, loop till we either read the required number of
    // bytes or we reach end-of-stream.
    do {
      int remainingBeforeRead = buffer.remaining();
      performLazySeek(remainingBeforeRead);
      checkState(
          contentChannelPosition == currentPosition,
          "contentChannelPosition (%s) should be equal to currentPosition (%s) after lazy seek",
          contentChannelPosition, currentPosition);

      try {
        int numBytesRead = contentChannel.read(buffer);
        checkIOPrecondition(numBytesRead != 0, "Read 0 bytes without blocking");
        if (numBytesRead < 0) {
          // Because we don't know decompressed object size for gzip-encoded objects,
          // assume that this is an object end.
          if (gzipEncoded) {
            size = currentPosition;
            contentChannelEnd = currentPosition;
          }
          // Check that we didn't get a premature End of Stream signal by checking the number of
          // bytes read against the stream size. Unfortunately we don't have information about the
          // actual size of the data stream when stream compression is used, so we can only ignore
          // this case here.
          checkIOPrecondition(
              currentPosition == contentChannelEnd || currentPosition == size,
              String.format(
                  "Received end of stream result before all the file data has been received; "
                      + "totalBytesRead: %d, currentPosition: %d,"
                      + " contentChannelEnd %d, size: %d, object: '%s'",
                  totalBytesRead, currentPosition, contentChannelEnd, size, resourceIdString));

          // If we have reached an end of a contentChannel but not an end of an object
          // then close contentChannel and continue reading an object if necessary.
          if (contentChannelEnd != size && currentPosition == contentChannelEnd) {
            closeContentChannel();
          } else {
            break;
          }
        }

        if (numBytesRead > 0) {
          totalBytesRead += numBytesRead;
          currentPosition += numBytesRead;
          contentChannelPosition += numBytesRead;
          checkState(
              contentChannelPosition == currentPosition,
              "contentChannelPosition (%s) should be equal to currentPosition (%s)"
                  + " after successful read",
              contentChannelPosition, currentPosition);
        }

        if (retriesAttempted != 0) {
          LOG.info("Success after {} retries on reading '{}'", retriesAttempted, resourceIdString);
        }
        // The count of retriesAttempted is per low-level contentChannel.read call;
        // each time we make progress we reset the retry counter.
        retriesAttempted = 0;
      } catch (IOException ioe) {
        // TODO(user): Refactor any reusable logic for retries into a separate RetryHelper class.
        if (retriesAttempted == maxRetries) {
          LOG.error(
              "Already attempted max of {} retries while reading '{}'; throwing exception.",
              maxRetries, resourceIdString);
          closeContentChannel();
          throw ioe;
        } else {
          if (retriesAttempted == 0) {
            // If this is the first of a series of retries, we also want to reset the readBackOff
            // to have fresh initial values.
            readBackOff.get().reset();
          }

          ++retriesAttempted;
          LOG.warn(
              "Got exception: {} while reading '{}'; retry #{}. Sleeping...",
              ioe.getMessage(), resourceIdString, retriesAttempted);
          try {
            boolean backOffSuccessful = BackOffUtils.next(sleeper, readBackOff.get());
            if (!backOffSuccessful) {
              LOG.error(
                  "BackOff returned false; maximum total elapsed time exhausted."
                      + " Giving up after {} retries for '{}'",
                  retriesAttempted, resourceIdString);
              closeContentChannel();
              throw ioe;
            }
          } catch (InterruptedException ie) {
            LOG.error(
                "Interrupted while sleeping before retry. Giving up after {} retries for '{}'",
                retriesAttempted, resourceIdString);
            ioe.addSuppressed(ie);
            closeContentChannel();
            throw ioe;
          }
          LOG.info(
              "Done sleeping before retry for '{}'; retry #{}", resourceIdString, retriesAttempted);

          if (buffer.remaining() != remainingBeforeRead) {
            int partialRead = remainingBeforeRead - buffer.remaining();
            LOG.info(
                "Despite exception, had partial read of {} bytes from '{}'; resetting retry count.",
                partialRead, resourceIdString);
            retriesAttempted = 0;
            totalBytesRead += partialRead;
            currentPosition += partialRead;
          }

          // Close the contentChannel.
          closeContentChannel();
        }
      } catch (RuntimeException r) {
        closeContentChannel();
        throw r;
      }
    } while (buffer.remaining() > 0);

    // If this method was called when the stream was already at EOF
    // (indicated by totalBytesRead == 0) then return EOF else,
    // return the number of bytes read.
    boolean isEndOfStream = (totalBytesRead == 0);
    if (isEndOfStream) {
      // Check that we didn't get a premature End of Stream signal by checking the number of bytes
      // read against the stream size. Unfortunately we don't have information about the actual size
      // of the data stream when stream compression is used, so we can only ignore this case here.
      checkIOPrecondition(
          currentPosition == size,
          String.format(
              "Failed to read any data before all the file data has been received;"
                  + " currentPosition: %d, size: %d, object '%s'",
              currentPosition, size, resourceIdString));
      return -1;
    }
    return totalBytesRead;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  /**
   * Tells whether this channel is open.
   *
   * @return a value indicating whether this channel is open
   */
  @Override
  public boolean isOpen() {
    return channelIsOpen;
  }

  /**
   * Closes the underlying {@link ReadableByteChannel}.
   *
   * <p>Catches and ignores all exceptions as there is not a lot the user can do to fix errors here
   * and a new connection will be needed. Especially SSLExceptions since the there's a high
   * probability that SSL connections would be broken in a way that causes {@link
   * java.nio.channels.Channel#close()} itself to throw an exception, even though underlying sockets
   * have already been cleaned up; close() on an SSLSocketImpl requires a shutdown handshake in
   * order to shutdown cleanly, and if the connection has been broken already, then this is not
   * possible, and the SSLSocketImpl was already responsible for performing local cleanup at the
   * time the exception was raised.
   */
  protected void closeContentChannel() {
    if (contentChannel != null) {
      LOG.debug("Closing internal contentChannel for '{}'", resourceIdString);
      try {
        contentChannel.close();
      } catch (Exception e) {
        LOG.debug(
            "Got an exception on contentChannel.close() for '{}'; ignoring it.",
            resourceIdString, e);
      } finally {
        contentChannel = null;
        contentChannelPosition = -1;
        contentChannelEnd = -1;
      }
    }
  }

  /**
   * Closes this channel.
   *
   * @throws IOException on IO error
   */
  @Override
  public void close() throws IOException {
    if (!channelIsOpen) {
      LOG.warn("Channel for '{}' is not open.", resourceIdString);
      return;
    }
    LOG.debug("Closing channel for '{}'", resourceIdString);
    channelIsOpen = false;
    closeContentChannel();
  }

  /**
   * Returns this channel's current position.
   *
   * @return this channel's current position
   */
  @Override
  public long position() throws IOException {
    throwIfNotOpen();
    return currentPosition;
  }

  /**
   * Sets this channel's position.
   *
   * <p>This method will throw an exception if {@code newPosition} is greater than object size,
   * which contradicts {@link SeekableByteChannel#position(long) SeekableByteChannel} contract.
   * TODO(user): decide if this needs to be fixed.
   *
   * @param newPosition the new position, counting the number of bytes from the beginning.
   * @return this channel instance
   * @throws java.io.FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    throwIfNotOpen();

    if (newPosition == currentPosition) {
      return this;
    }

    validatePosition(newPosition);
    LOG.debug(
        "Seek from {} to {} position for '{}'", currentPosition, newPosition, resourceIdString);
    currentPosition = newPosition;
    return this;
  }

  private boolean isRandomAccessPattern(long oldPosition) {
    if (!shouldDetectRandomAccess()) {
      return false;
    }
    if (currentPosition < oldPosition) {
      LOG.debug(
          "Detected backward read from {} to {} position, switching to random IO for '{}'",
          oldPosition, currentPosition, resourceIdString);
      return true;
    }
    if (oldPosition >= 0 && oldPosition + readOptions.getInplaceSeekLimit() < currentPosition) {
      LOG.debug(
          "Detected forward read from {} to {} position over {} threshold,"
              + " switching to random IO for '{}'",
          oldPosition, currentPosition, readOptions.getInplaceSeekLimit(), resourceIdString);
      return true;
    }
    return false;
  }

  private boolean shouldDetectRandomAccess() {
    return !gzipEncoded && !randomAccess && readOptions.getFadvise() == Fadvise.AUTO;
  }

  private void setRandomAccess() {
    randomAccess = true;
    checkEncodingAndAccess();
  }

  private void skipInPlace(long seekDistance) {
    if (skipBuffer == null) {
      skipBuffer = new byte[SKIP_BUFFER_SIZE];
    }
    while (seekDistance > 0 && contentChannel != null) {
      try {
        int bufferSize = Math.toIntExact(Math.min(skipBuffer.length, seekDistance));
        int bytesRead = contentChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
        if (bytesRead < 0) {
          // Shouldn't happen since we called validatePosition prior to this loop.
          LOG.info(
              "Somehow read {} bytes trying to skip {} bytes to seek to position {}, size: {}",
              bytesRead, seekDistance, currentPosition, size);
          closeContentChannel();
        } else {
          seekDistance -= bytesRead;
          contentChannelPosition += bytesRead;
        }
      } catch (IOException e) {
        LOG.info(
            "Got an IO exception on contentChannel.read(), a lazy-seek will be pending for '{}'",
            resourceIdString, e);
        closeContentChannel();
      }
    }
    checkState(
        contentChannel == null || contentChannelPosition == currentPosition,
        "contentChannelPosition (%s) should be equal to currentPosition (%s)"
            + " after successful in-place skip",
        contentChannelPosition, currentPosition);
  }

  /**
   * Returns size of the object to which this channel is connected.
   *
   * @return size of the object to which this channel is connected
   * @throws IOException on IO error
   */
  @Override
  public long size() throws IOException {
    throwIfNotOpen();
    return size;
  }

  /** Sets size of this channel to the given value. */
  @VisibleForTesting
  protected void setSize(long size) {
    this.size = size;
  }

  private void checkEncodingAndAccess() {
    checkState(
        !(gzipEncoded && randomAccess),
        "gzipEncoded and randomAccess should not be true at the same time for '%s'",
        resourceIdString);
  }

  /** Validates that the given position is valid for this channel. */
  protected void validatePosition(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'",
              position, resourceIdString));
    }

    if (position >= size) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d for '%s'",
              position, size, resourceIdString));
    }
  }

  /**
   * Seeks to the given position in the underlying stream.
   *
   * <p>Note: Seek is an expensive operation because a new stream is opened each time.
   *
   * @throws java.io.FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  @VisibleForTesting
  void performLazySeek(long limit) throws IOException {
    throwIfNotOpen();

    // Return quickly if there is no pending seek operation, i.e. position didn't change.
    if (currentPosition == contentChannelPosition && contentChannel != null) {
      return;
    }

    LOG.debug(
        "Performing lazySeek from {} to {} position with {} limit for '{}'",
        contentChannelPosition, currentPosition, limit, resourceIdString);

    // used to auto-detect random access
    long oldPosition = contentChannelPosition;

    long seekDistance = currentPosition - contentChannelPosition;
    if (contentChannel != null
        && seekDistance > 0
        && seekDistance <= readOptions.getInplaceSeekLimit()
        && currentPosition < contentChannelEnd) {
      LOG.debug(
          "Seeking forward {} bytes (inplaceSeekLimit: {}) in-place to position {} for '{}'",
          seekDistance, readOptions.getInplaceSeekLimit(), currentPosition, resourceIdString);
      skipInPlace(seekDistance);
    } else {
      closeContentChannel();
    }

    if (contentChannel == null) {
      if (isRandomAccessPattern(oldPosition)) {
        setRandomAccess();
      }
      openContentChannel(limit);
    }
  }

  private void openContentChannel(long limit) throws IOException {
    checkState(contentChannel == null, "contentChannel should be null, before opening new");
    InputStream objectContentStream =
        footerContent != null && currentPosition >= size - footerContent.length
            ? openFooterStream(limit)
            : openStream(limit);
    contentChannel = Channels.newChannel(objectContentStream);
    contentChannelPosition = currentPosition;
  }

  /**
   * Initializes {@link #gzipEncoded} and {@link #size} from object's GCS metadata.
   *
   * @throws IOException on IO error.
   */
  @VisibleForTesting
  protected void initEncodingAndSize() throws IOException {
    checkState(
        size < 0,
        "size should be not initialized yet, but was %s for '%s'", size, resourceIdString);
    StorageObject metadata = getMetadata();
    gzipEncoded = nullToEmpty(metadata.getContentEncoding()).contains("gzip");
    size = gzipEncoded ? Long.MAX_VALUE : metadata.getSize().longValue();
  }

  /**
   * Retrieve the object's metadata from GCS.
   *
   * @throws IOException on IO error.
   */
  protected StorageObject getMetadata() throws IOException {
    Storage.Objects.Get getObject = createRequest();
    return retry(ResilientOperation.getGoogleRequestCallable(getObject));
  }

  /**
   * Prefetches footer and retrieves object's metadata from HTTP headers.
   *
   * @throws IOException on IO error.
   */
  protected void initEncodingAndSizeAndPrefetchFooter() throws IOException {
    Get getObject = createDataRequest("bytes=-" + readOptions.getFooterPrefetchSize());

    HttpResponse response =
        retry(
            new CheckedCallable<HttpResponse, IOException>() {
              @Override
              public HttpResponse call() throws IOException {
                return getObject.executeMedia();
              }
            });

    HttpHeaders responseHeaders = response.getHeaders();

    gzipEncoded = nullToEmpty(responseHeaders.getContentEncoding()).contains("gzip");
    if (gzipEncoded) {
      size = Long.MAX_VALUE;
      return;
    }

    String range = responseHeaders.getContentRange();
    size = Long.parseLong(range.substring(range.lastIndexOf('/') + 1));

    int contentSize = Math.toIntExact(responseHeaders.getContentLength());
    if (size > 0) {
      try (InputStream footerStream = new BufferedInputStream(response.getContent(), contentSize)) {
        footerContent = ByteStreams.toByteArray(footerStream);
      } catch (IOException e) {
        LOG.info("Failed to prefetch footer for '{}'", resourceIdString, e);
        footerContent = null;
      }
    }
    checkState(
        footerContent == null || footerContent.length == contentSize,
        "prefetched footer size (%s) should equal to content length header (%s)",
        footerContent == null ? null : footerContent.length, contentSize);
  }

  private <T> T retry(CheckedCallable<T, IOException> callableToRetry) throws IOException {
    backOff.get().reset();
    try {
      return ResilientOperation.retry(
          callableToRetry,
          new RetryBoundedBackOff(maxRetries, backOff.get()),
          RetryDeterminer.SOCKET_ERRORS,
          IOException.class,
          sleeper);
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        throw GoogleCloudStorageExceptions.getFileNotFoundException(bucketName, objectName);
      }
      throw new IOException("Error reading " + resourceIdString, e);
    } catch (InterruptedException e) { // From the sleep
      throw new IOException("Thread interrupt received.", e);
    }
  }

  private InputStream openFooterStream(long limit) {
    int offset = Math.toIntExact(currentPosition - (size - footerContent.length));
    int length = footerContent.length - offset;
    LOG.debug(
        "Opened stream (prefetched footer) from {} position and {} limit for '{}'",
        currentPosition, limit, resourceIdString);
    return new ByteArrayInputStream(footerContent, offset, length);
  }

  /**
   * Opens the underlying stream, sets its position to the given value.
   *
   * <p>If the file encoding in GCS is gzip (and therefore the HTTP client will attempt to
   * decompress it), the entire file is always requested and we seek to the position requested. If
   * the file encoding is not gzip, only the remaining bytes to be read are requested from GCS.
   *
   * @param limit number of bytes to read from new stream. Ignored if {@link
   *     GoogleCloudStorageReadOptions#getFadvise()} is equal to {@link Fadvise#SEQUENTIAL}.
   * @throws IOException on IO error
   */
  protected InputStream openStream(long limit) throws IOException {
    checkArgument(limit > 0, "limit should be greater than 0, but was %s", limit);

    checkState(
        contentChannel == null && contentChannelEnd < 0,
        "contentChannel and contentChannelEnd should be not initialized yet for '%s'",
        resourceIdString);

    if (size == 0) {
      return new ByteArrayInputStream(new byte[0]);
    }

    int bufferSize = readOptions.getBufferSize();

    String rangeHeader = null;

    // Do not set range for gzip-encoded files - it's not supported.
    if (!gzipEncoded) {
      rangeHeader = "bytes=" + currentPosition + "-";

      if (randomAccess) {
        long rangeSize = Math.max(bufferSize, limit);
        // When bufferSize is 0 and limit passed by client is very small,
        // we still don't want to send too small range request to GCS.
        rangeSize = Math.max(rangeSize, readOptions.getMinRangeRequestSize());
        // limit rangeSize to the object end
        rangeSize = Math.min(rangeSize, size - currentPosition);
        long rangeEndInclusive = currentPosition + rangeSize - 1;
        rangeHeader += rangeEndInclusive;

        // set contentChannelEnd to range end
        contentChannelEnd = currentPosition + rangeSize;
      }
    }

    Storage.Objects.Get getObject = createDataRequest(rangeHeader);

    if (contentChannelEnd < 0) {
      checkState(
          !randomAccess,
          "contentChannelEnd should be initialized already if fadvise is RANDOM for '%s'",
          resourceIdString);

      // we are reading object till the end
      contentChannelEnd = size;
    }

    // limit buffer size to the object end
    bufferSize = Math.toIntExact(Math.min(bufferSize, contentChannelEnd - currentPosition));

    checkState(
        contentChannelEnd >= 0,
        "contentChannelEnd should be initialized already for '%s'", resourceIdString);

    HttpResponse response;
    try {
      response = getObject.executeMedia();
      // TODO(b/110832992): validate response range header against expected/request range
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        throw GoogleCloudStorageExceptions.getFileNotFoundException(bucketName, objectName);
      }
      String msg =
          String.format("Error reading '%s' at position %d", resourceIdString, currentPosition);
      if (errorExtractor.rangeNotSatisfiable(e)) {
        throw (EOFException) new EOFException(msg).initCause(e);
      }
      throw new IOException(msg, e);
    }

    try {
      InputStream contentStream = response.getContent();

      if (bufferSize > 0) {
        LOG.debug(
            "Opened stream from {} position with {} range, {} limit and {} bytes buffer for '{}'",
            currentPosition, rangeHeader, limit, bufferSize, resourceIdString);
        contentStream = new BufferedInputStream(contentStream, bufferSize);
      } else {
        LOG.debug(
            "Opened stream from {} position with {} range and {} limit for '{}'",
            currentPosition, rangeHeader, limit, resourceIdString);
      }

      if (gzipEncoded) {
        contentStream.skip(currentPosition);
      }

      return contentStream;
    } catch (IOException e) {
      try {
        response.disconnect();
      } catch (IOException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  private Get createDataRequest(String rangeHeader) throws IOException {
    Get getObject = createRequest();

    // Set the headers on the existing request headers that may have
    // been initialized with things like user-agent already.
    HttpHeaders requestHeaders = clientRequestHelper.getRequestHeaders(getObject);

    // Disable GCS decompressive transcoding.
    requestHeaders.setAcceptEncoding("gzip");

    requestHeaders.setRange(rangeHeader);

    return getObject;
  }

  protected Storage.Objects.Get createRequest() throws IOException {
    return gcs.objects().get(bucketName, objectName);
  }

  /** Throws if this channel is not currently open. */
  private void throwIfNotOpen() throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  /**
   * Throws an IOException if precondition is false.
   *
   * <p>This method should be used in place of Preconditions.checkState in cases where the
   * precondition is derived from the status of the IO operation. That makes it possible to retry
   * the operation by catching IOException.
   */
  private void checkIOPrecondition(boolean precondition, String errorMessage) throws IOException {
    if (!precondition) {
      throw new IOException(errorMessage);
    }
  }

  /**
   * @deprecated use {@link GoogleCloudStorageReadOptions#DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS}
   */
  @Deprecated
  public static final int DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS;
  /** @deprecated use {@link GoogleCloudStorageReadOptions#DEFAULT_BACKOFF_RANDOMIZATION_FACTOR} */
  @Deprecated
  public static final double DEFAULT_BACKOFF_RANDOMIZATION_FACTOR =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_RANDOMIZATION_FACTOR;
  /** @deprecated use {@link GoogleCloudStorageReadOptions#DEFAULT_BACKOFF_MULTIPLIER} instead */
  @Deprecated
  public static final double DEFAULT_BACKOFF_MULTIPLIER =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_MULTIPLIER;
  /** @deprecated use {@link GoogleCloudStorageReadOptions#DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS} */
  @Deprecated
  public static final int DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS;
  /**
   * @deprecated use {@link GoogleCloudStorageReadOptions#DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS}
   */
  @Deprecated
  public static final int DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS =
      GoogleCloudStorageReadOptions.DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS;
}
