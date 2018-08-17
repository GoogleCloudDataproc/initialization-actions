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

import static com.google.cloud.hadoop.gcsio.StorageResourceId.createReadableString;
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
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions.GenerationReadConsistency;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
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
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides seekable read access to GCS. */
public class GoogleCloudStorageReadChannel implements SeekableByteChannel {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageReadChannel.class);

  // Size of buffer to allocate for skipping bytes in-place when performing in-place seeks.
  @VisibleForTesting static final int SKIP_BUFFER_SIZE = 8192;

  private static final String GZIP_ENCODING = "gzip";

  // GCS access instance.
  private final Storage gcs;

  // Name of the bucket containing the object being read.
  private final String bucketName;

  // Name of the object being read.
  private final String objectName;

  // GCS resource/object path, used for logging.
  private final Object resourceIdString;

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
  @VisibleForTesting protected long contentChannelPosition = -1;

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

  private Long generation = null;

  private boolean metadataInitialized = false;

  /**
   * Constructs an instance of GoogleCloudStorageReadChannel.
   *
   * @param gcs storage object instance
   * @param bucketName name of the bucket containing the object to read
   * @param objectName name of the object to read
   * @param requestHelper a ClientRequestHelper used to set any extra headers
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
    // TODO: micro benchmark if this necessary
    this.resourceIdString = lazyToString(() -> createReadableString(bucketName, objectName));

    // Initialize metadata if available.
    GoogleCloudStorageItemInfo info = getInitialMetadata();
    if (info != null) {
      String generationString = String.valueOf(info.getContentGeneration());
      initMetadata(info.getContentEncoding(), info.getSize(), generationString);
    }
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
   * Sets the back-off for determining sleep duration between read retries.
   *
   * @param backOff {@link BackOff} to use for read retries, could not be null.
   */
  void setReadBackOff(BackOff backOff) {
    this.readBackOff = Suppliers.ofInstance(checkNotNull(backOff, "backOff could not be null"));
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
   * Returns {@link GoogleCloudStorageItemInfo} used to initialize metadata in constructor. By
   * default returns {@code null} which means that metadata will be lazily initialized during first
   * read.
   */
  @Nullable
  protected GoogleCloudStorageItemInfo getInitialMetadata() {
    return null;
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
              "Throwing exception after reaching max read retries ({}) for '{}'.",
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
              "Failed read retry #{}/{} for '{}'. Sleeping...",
              retriesAttempted, maxRetries, resourceIdString, ioe);
          try {
            boolean backOffSuccessful = BackOffUtils.next(sleeper, readBackOff.get());
            if (!backOffSuccessful) {
              LOG.error(
                  "BackOff returned false; maximum total elapsed time exhausted."
                      + " Giving up after {}/{} retries for '{}'",
                  retriesAttempted, maxRetries, resourceIdString);
              closeContentChannel();
              throw ioe;
            }
          } catch (InterruptedException ie) {
            LOG.error(
                "Interrupted while sleeping before retry. Giving up after {}/{} retries for '{}'",
                retriesAttempted, maxRetries, resourceIdString);
            ioe.addSuppressed(ie);
            closeContentChannel();
            throw ioe;
          }
          LOG.info(
              "Done sleeping before retry #{}/{} for '{}'",
              retriesAttempted, maxRetries, resourceIdString);

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
        resetContentChannel();
      }
    }
  }

  private void resetContentChannel() {
    checkState(contentChannel == null, "contentChannel should be null for '%s'", resourceIdString);
    contentChannelPosition = -1;
    contentChannelEnd = -1;
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
   * <p>Note: this method will return -1 until metadata will be lazily initialized during first
   * {@link #read} method call.
   *
   * @return size of the object to which this channel is connected after metadata was initialized
   *     (during first read) or {@code -1} otherwise.
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

    if (size >= 0 && position >= size) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be between 0 and %d for '%s'",
              position, size, resourceIdString));
    }
  }

  /**
   * Seeks to the {@link #currentPosition} in the underlying stream or opens new stream at {@link
   * #currentPosition}.
   *
   * <p>Note: Seek could be an expensive operation if a new stream is opened.
   *
   * @param bytesToRead number of bytes to read, used only if new stream is opened.
   * @throws java.io.FileNotFoundException if the underlying object does not exist.
   * @throws IOException on IO error
   */
  @VisibleForTesting
  void performLazySeek(long bytesToRead) throws IOException {
    throwIfNotOpen();

    // Return quickly if there is no pending seek operation, i.e. position didn't change.
    if (currentPosition == contentChannelPosition && contentChannel != null) {
      return;
    }

    LOG.debug(
        "Performing lazySeek from {} to {} position with {} bytesToRead for '{}'",
        contentChannelPosition, currentPosition, bytesToRead, resourceIdString);

    // used to auto-detect random access
    long oldPosition = contentChannelPosition;

    long seekDistance = currentPosition - contentChannelPosition;
    if (contentChannel != null
        && seekDistance > 0
        // Always skip in place gzip-encoded files, because they do not support range reads.
        && (gzipEncoded || seekDistance <= readOptions.getInplaceSeekLimit())
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
      openContentChannel(bytesToRead);
    }
  }

  private void openContentChannel(long bytesToRead) throws IOException {
    checkState(contentChannel == null, "contentChannel should be null, before opening new");
    InputStream objectContentStream =
        footerContent != null && currentPosition >= size - footerContent.length
            ? openFooterStream()
            : openStream(bytesToRead);
    contentChannel = Channels.newChannel(objectContentStream);
    checkState(
        contentChannelPosition == currentPosition,
        "contentChannelPosition (%s) should be equal to currentPosition (%s) for '%s'",
        contentChannelPosition, currentPosition, resourceIdString);
  }

  /** Initializes metadata (size, encoding, etc) from HTTP {@code headers}. */
  @VisibleForTesting
  protected void initMetadata(HttpHeaders headers) throws IOException {
    checkState(
        !metadataInitialized,
        "can not initialize metadata, it already initialized for '%s'", resourceIdString);

    long sizeFromMetadata;
    String range = headers.getContentRange();
    if (range != null) {
      sizeFromMetadata = Long.parseLong(range.substring(range.lastIndexOf('/') + 1));
    } else {
      sizeFromMetadata = headers.getContentLength();
    }

    String generation = headers.getFirstHeaderStringValue("x-goog-generation");

    initMetadata(headers.getContentEncoding(), sizeFromMetadata, generation);
  }

  /** Initializes metadata (size, encoding, etc) from passed parameters. */
  @VisibleForTesting
  protected void initMetadata(
      @Nullable String encoding, long sizeFromMetadata, @Nullable String generation)
      throws IOException {
    checkState(
        !metadataInitialized,
        "can not initialize metadata, it already initialized for '%s'", resourceIdString);
    gzipEncoded = nullToEmpty(encoding).contains(GZIP_ENCODING);
    if (gzipEncoded) {
      size = Long.MAX_VALUE;
    } else {
      size = sizeFromMetadata;
    }
    randomAccess = !gzipEncoded && readOptions.getFadvise() == Fadvise.RANDOM;
    checkEncodingAndAccess();

    initGeneration(generation);

    metadataInitialized = true;

    LOG.debug(
        "Initialized metadata (gzipEncoded={}, size={}, randomAccess={}, generation={}) for '{}'",
        gzipEncoded, size, randomAccess, generation, resourceIdString);
  }

  private void initGeneration(@Nullable String generationString) throws IOException {
    if (readOptions.getGenerationReadConsistency().equals(GenerationReadConsistency.LATEST)) {
      generation = null;
    } else {
      if (generationString == null) {
        throw new IOException(
            String.format(
                "Generation Read Consistency is '%s', but failed to retrieve generation for '%s'.",
                readOptions.getGenerationReadConsistency(), resourceIdString));
      }
      generation = Long.parseLong(generationString);
    }
  }

  private void cacheFooter(HttpResponse response) throws IOException {
    checkState(size > 0, "size should be greater than 0 for '%s'", resourceIdString);
    int footerSize = Math.toIntExact(response.getHeaders().getContentLength());
    footerContent = new byte[footerSize];
    try (InputStream footerStream = response.getContent()) {
      int totalBytesRead = 0;
      int bytesRead = 0;
      do {
        totalBytesRead += bytesRead;
        bytesRead = footerStream.read(footerContent, totalBytesRead, footerSize - totalBytesRead);
      } while (bytesRead >= 0 && totalBytesRead <= footerSize);
      checkState(
          footerStream.read() < 0,
          "footerStream should be empty after reading %s bytes from %s bytes for '%s'",
          totalBytesRead, footerSize, resourceIdString);
      checkState(
          totalBytesRead == footerSize,
          "totalBytesRead (%s) should equal footerSize (%s) for '%s'",
          totalBytesRead, footerSize, resourceIdString);
    } catch (IOException e) {
      footerContent = null;
      throw e;
    }
    LOG.debug("Prefetched {} bytes footer for '{}'", footerContent.length, resourceIdString);
  }

  /**
   * Opens the underlying stream from {@link #footerContent}, sets its position to the {@link
   * #currentPosition}.
   */
  private InputStream openFooterStream() {
    contentChannelPosition = currentPosition;
    int offset = Math.toIntExact(currentPosition - (size - footerContent.length));
    int length = footerContent.length - offset;
    LOG.debug(
        "Opened stream (prefetched footer) from {} position for '{}'",
        currentPosition, resourceIdString);
    return new ByteArrayInputStream(footerContent, offset, length);
  }

  /**
   * Opens the underlying stream, sets its position to the {@link #currentPosition}.
   *
   * <p>If the file encoding in GCS is gzip (and therefore the HTTP client will decompress it), the
   * entire file is always requested and we seek to the position requested. If the file encoding is
   * not gzip, only the remaining bytes to be read are requested from GCS.
   *
   * @param bytesToRead number of bytes to read from new stream. Ignored if {@link
   *     GoogleCloudStorageReadOptions#getFadvise()} is equal to {@link Fadvise#SEQUENTIAL}.
   * @throws IOException on IO error
   */
  protected InputStream openStream(long bytesToRead) throws IOException {
    checkArgument(bytesToRead > 0, "bytesToRead should be greater than 0, but was %s", bytesToRead);

    checkState(
        contentChannel == null && contentChannelEnd < 0,
        "contentChannel and contentChannelEnd should be not initialized yet for '%s'",
        resourceIdString);

    if (size == 0) {
      return new ByteArrayInputStream(new byte[0]);
    }

    String rangeHeader;
    if (!metadataInitialized) {
      contentChannelPosition = getContentChannelPositionForFirstRead(bytesToRead);
      rangeHeader = "bytes=" + contentChannelPosition + "-";
      if (readOptions.getFadvise() == Fadvise.RANDOM) {
        long maxBytesToRead = Math.max(readOptions.getMinRangeRequestSize(), bytesToRead);
        rangeHeader += (contentChannelPosition + maxBytesToRead - 1);
      }
    } else if (gzipEncoded) {
      // Do not set range for gzip-encoded files - it's not supported.
      rangeHeader = null;
      // Always read gzip-encoded files till the end - they do not support range reads.
      contentChannelPosition = 0;
      contentChannelEnd = size;
    } else {
      if (readOptions.getFadvise() != Fadvise.SEQUENTIAL && isFooterRead()) {
        // Pre-fetch footer if reading end of file.
        contentChannelPosition = Math.max(0, size - readOptions.getMinRangeRequestSize());
      } else {
        contentChannelPosition = currentPosition;
      }

      // Set rangeSize to the size of the file reminder from currentPosition.
      long rangeSize = size - contentChannelPosition;
      if (randomAccess) {
        long randomRangeSize = Math.max(bytesToRead, readOptions.getMinRangeRequestSize());
        // Limit rangeSize to the randomRangeSize.
        rangeSize = Math.min(randomRangeSize, rangeSize);
      }

      contentChannelEnd = contentChannelPosition + rangeSize;
      // Do not read footer again, if it was already pre-fetched.
      if (footerContent != null) {
        contentChannelEnd = Math.min(contentChannelEnd, size - footerContent.length);
      }

      checkState(
          currentPosition < contentChannelEnd,
          "currentPosition (%s) should be less than contentChannelEnd (%s) for '%s'",
          currentPosition, contentChannelEnd, resourceIdString);
      checkState(
          contentChannelPosition <= currentPosition,
          "contentChannelPosition (%s) should be less or equal to currentPosition (%s) for '%s'",
          contentChannelPosition, currentPosition, resourceIdString);

      rangeHeader = "bytes=" + contentChannelPosition + "-";
      if (randomAccess || contentChannelEnd != size) {
        rangeHeader += (contentChannelEnd - 1);
      }
    }
    checkState(
        !metadataInitialized || contentChannelEnd > 0,
        "contentChannelEnd should be initialized already for '%s'", resourceIdString);

    Get getObject = createDataRequest(rangeHeader);
    HttpResponse response;
    try {
      response = getObject.executeMedia();
      // TODO(b/110832992): validate response range header against expected/request range
    } catch (IOException e) {
      if (!metadataInitialized && errorExtractor.rangeNotSatisfiable(e) && currentPosition == 0) {
        // We don't know the size yet (metadataInitialized == false) and we're seeking to byte 0,
        // but got 'range not satisfiable'; the object must be empty.
        LOG.info(
            "Got 'range not satisfiable' for reading '{}' at position 0; assuming empty.",
            resourceIdString);
        size = 0;
        return new ByteArrayInputStream(new byte[0]);
      }
      response = handleExecuteMediaException(e, getObject, shouldRetryWithLiveVersion());
    }

    if (contentChannelEnd < 0) {
      String contentRange = response.getHeaders().getContentRange();
      if (contentRange != null) {
        String contentEnd =
            contentRange.substring(
                contentRange.lastIndexOf('-') + 1, contentRange.lastIndexOf('/'));
        contentChannelEnd = Long.parseLong(contentEnd) + 1;
      } else {
        contentChannelEnd = response.getHeaders().getContentLength();
      }
    }

    if (!metadataInitialized) {
      initMetadata(response.getHeaders());
      checkState(
          metadataInitialized, "metadata should be initialized already for '%s'", resourceIdString);
      if (size == 0) {
        resetContentChannel();
        return new ByteArrayInputStream(new byte[0]);
      }
      if (gzipEncoded && currentPosition != 0) {
        resetContentChannel();
        return openStream(bytesToRead);
      }
    }
    checkState(
        contentChannelEnd > 0,
        "contentChannelEnd should be initialized already for '%s'", resourceIdString);

    if (!gzipEncoded
        && readOptions.getFadvise() != Fadvise.SEQUENTIAL
        && contentChannelEnd == size
        && contentChannelEnd - contentChannelPosition <= readOptions.getMinRangeRequestSize()) {
      for (int retriesCount = 0; retriesCount < maxRetries; retriesCount++) {
        try {
          cacheFooter(response);
          if (retriesCount != 0) {
            LOG.info(
                "Successfully cached footer after {} retries for '{}'",
                retriesCount, resourceIdString);
          }
          break;
        } catch (IOException e) {
          LOG.info(
              "Failed to prefetch footer (retry #{}/{}) for '{}'",
              retriesCount + 1, maxRetries, resourceIdString, e);
          if (retriesCount == 0) {
            readBackOff.get().reset();
          }
          if (retriesCount == maxRetries) {
            resetContentChannel();
            throw e;
          }
          try {
            response = getObject.executeMedia();
            // TODO(b/110832992): validate response range header against expected/request range.
          } catch (IOException e1) {
            response = handleExecuteMediaException(e1, getObject, shouldRetryWithLiveVersion());
          }
        }
      }
      checkState(
          footerContent != null,
          "footerContent should not be null after successful footer prefetch for '%s'",
          resourceIdString);
      resetContentChannel();
      return openFooterStream();
    }

    try {
      InputStream contentStream = response.getContent();
      if (readOptions.getBufferSize() > 0) {
        int bufferSize = readOptions.getBufferSize();
        // limit buffer size to the channel end
        bufferSize =
            Math.toIntExact(Math.min(bufferSize, contentChannelEnd - contentChannelPosition));
        LOG.debug(
            "Opened stream from {} position with {} range, {} bytesToRead"
                + " and {} bytes buffer for '{}'",
            currentPosition, rangeHeader, bytesToRead, bufferSize, resourceIdString);
        contentStream = new BufferedInputStream(contentStream, bufferSize);
      } else {
        LOG.debug(
            "Opened stream from {} position with {} range and {} bytesToRead for '{}'",
            currentPosition, rangeHeader, bytesToRead, resourceIdString);
      }

      if (contentChannelPosition < currentPosition) {
        contentStream.skip(currentPosition - contentChannelPosition);
        contentChannelPosition = currentPosition;
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

  private boolean isFooterRead() {
    return size - currentPosition <= readOptions.getMinRangeRequestSize();
  }

  private long getContentChannelPositionForFirstRead(long bytesToRead) {
    if (readOptions.getFadvise() == Fadvise.SEQUENTIAL
        || bytesToRead >= readOptions.getMinRangeRequestSize()) {
      return currentPosition;
    }
    // Prefetch footer (bytes before 'currentPosition' in case of last byte read) lazily.
    // Max prefetch size is (minRangeRequestSize / 2) bytes.
    if (bytesToRead <= readOptions.getMinRangeRequestSize() / 2) {
      return Math.max(0, currentPosition - readOptions.getMinRangeRequestSize() / 2);
    }
    return Math.max(0, currentPosition - (readOptions.getMinRangeRequestSize() - bytesToRead));
  }

  private boolean shouldRetryWithLiveVersion() {
    return generation != null
        && readOptions.getGenerationReadConsistency().equals(GenerationReadConsistency.BEST_EFFORT);
  }

  /**
   * When an IOException is thrown, depending on if the exception is caused by non-existent object
   * generation, and depending on the generation read consistency setting, either retry the read (of
   * the latest generation), or handle the exception directly.
   *
   * @param e IOException thrown while reading from GCS.
   * @param getObject the Get request to GCS.
   * @param retryWithLiveVersion flag indicating whether we should strip the generation (thus read
   *     from the latest generation) and retry.
   * @return the HttpResponse of reading from GCS from possible retry.
   * @throws IOException either error on retry, or thrown because the original read encounters
   *     error.
   */
  private HttpResponse handleExecuteMediaException(
      IOException e, Get getObject, boolean retryWithLiveVersion) throws IOException {
    if (errorExtractor.itemNotFound(e)) {
      if (retryWithLiveVersion) {
        generation = null;
        footerContent = null;
        getObject.setGeneration(null);
        try {
          return getObject.executeMedia();
        } catch (IOException e1) {
          return handleExecuteMediaException(e1, getObject, /* retryWithLiveVersion= */ false);
        }
      }
      throw GoogleCloudStorageExceptions.getFileNotFoundException(bucketName, objectName);
    }
    String msg =
        String.format("Error reading '%s' at position %d", resourceIdString, currentPosition);
    if (errorExtractor.rangeNotSatisfiable(e)) {
      throw (EOFException) new EOFException(msg).initCause(e);
    }
    throw new IOException(msg, e);
  }

  private Get createDataRequest(String rangeHeader) throws IOException {
    Get getObject = createRequest();
    getObject.setGeneration(generation);

    // Set the headers on the existing request headers that may have
    // been initialized with things like user-agent already.
    HttpHeaders requestHeaders = clientRequestHelper.getRequestHeaders(getObject);
    // Disable GCS decompressive transcoding.
    requestHeaders.setAcceptEncoding("gzip");
    requestHeaders.setRange(rangeHeader);

    return getObject;
  }

  protected Get createRequest() throws IOException {
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

  private static Object lazyToString(Supplier<String> stringSupplier) {
    return new Object() {
      private final Supplier<String> toString = Suppliers.memoize(stringSupplier);

      @Override
      public String toString() {
        return toString.get();
      }
    };
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
