/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;

/**
 * Advanced options for reading GoogleCloudStorage objects. Immutable; callers must use the inner
 * {@link Builder} class to construct instances.
 */
@AutoValue
public abstract class GoogleCloudStorageReadOptions {

  /** Operational modes of fadvise feature. */
  public static enum Fadvise {
    AUTO,
    RANDOM,
    SEQUENTIAL
  }

  /** Options of read consistency on generations. */
  public enum GenerationReadConsistency {
    LATEST,
    BEST_EFFORT,
    STRICT
  }

  public static final int DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS = 200;
  public static final double DEFAULT_BACKOFF_RANDOMIZATION_FACTOR = 0.5;
  public static final double DEFAULT_BACKOFF_MULTIPLIER = 1.5;
  public static final int DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS = 10 * 1000;
  public static final int DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS = 2 * 60 * 1000;
  public static final boolean DEFAULT_SUPPORT_CONTENT_ENCODING = true;
  public static final int DEFAULT_BUFFER_SIZE = 0;
  public static final long DEFAULT_INPLACE_SEEK_LIMIT = 0L;
  public static final Fadvise DEFAULT_FADVISE = Fadvise.SEQUENTIAL;
  public static final int DEFAULT_MIN_RANGE_REQUEST_SIZE = 512 * 1024;
  public static final int DEFAULT_FOOTER_PREFETCH_SIZE = 0;
  public static final GenerationReadConsistency DEFAULT_GENERATION_READ_CONSISTENCY =
      GenerationReadConsistency.LATEST;

  // Default builder should be initialized after default values,
  // otherwise it will access not initialized default values.
  public static final GoogleCloudStorageReadOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_GoogleCloudStorageReadOptions.Builder()
        .setBackoffInitialIntervalMillis(DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS)
        .setBackoffRandomizationFactor(DEFAULT_BACKOFF_RANDOMIZATION_FACTOR)
        .setBackoffMultiplier(DEFAULT_BACKOFF_MULTIPLIER)
        .setBackoffMaxIntervalMillis(DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS)
        .setBackoffMaxElapsedTimeMillis(DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS)
        .setSupportContentEncoding(DEFAULT_SUPPORT_CONTENT_ENCODING)
        .setBufferSize(DEFAULT_BUFFER_SIZE)
        .setInplaceSeekLimit(DEFAULT_INPLACE_SEEK_LIMIT)
        .setFadvise(DEFAULT_FADVISE)
        .setMinRangeRequestSize(DEFAULT_MIN_RANGE_REQUEST_SIZE)
        .setFooterPrefetchSize(DEFAULT_FOOTER_PREFETCH_SIZE)
        .setGenerationReadConsistency(DEFAULT_GENERATION_READ_CONSISTENCY);
  }

  /** See {@link Builder#setBackoffInitialIntervalMillis}. */
  public abstract int getBackoffInitialIntervalMillis();

  /** See {@link Builder#setBackoffRandomizationFactor}. */
  public abstract double getBackoffRandomizationFactor();

  /** See {@link Builder#setBackoffMultiplier}. */
  public abstract double getBackoffMultiplier();

  /** See {@link Builder#setBackoffMaxIntervalMillis}. */
  public abstract int getBackoffMaxIntervalMillis();

  /** See {@link Builder#setBackoffMaxElapsedTimeMillis}. */
  public abstract int getBackoffMaxElapsedTimeMillis();

  /** See {@link Builder#setSupportContentEncoding}. */
  public abstract boolean getSupportContentEncoding();

  /** See {@link Builder#setBufferSize}. */
  public abstract int getBufferSize();

  /** See {@link Builder#setInplaceSeekLimit}. */
  public abstract long getInplaceSeekLimit();

  /** See {@link Builder#setFadvise}. */
  public abstract Fadvise getFadvise();

  /** See {@link Builder#setMinRangeRequestSize}. */
  public abstract int getMinRangeRequestSize();

  /** See {@link Builder#setFooterPrefetchSize}. */
  public abstract int getFooterPrefetchSize();

  /** See {@link Builder#setGenerationReadConsistency}. */
  public abstract GenerationReadConsistency getGenerationReadConsistency();

  public abstract Builder toBuilder();

  /** Mutable builder for GoogleCloudStorageReadOptions. */
  @AutoValue.Builder
  public abstract static class Builder {
    /**
     * On exponential back-off, the initial delay before the first retry; subsequent retries then
     * grow as an exponential function of the current delay interval.
     */
    public abstract Builder setBackoffInitialIntervalMillis(int backoffInitialIntervalMillis);

    /**
     * The amount of jitter introduced when computing the next retry sleep interval so that when
     * many clients are retrying, they don't all retry at the same time.
     */
    public abstract Builder setBackoffRandomizationFactor(double backoffRandomizationFactor);

    /**
     * The base of the exponent used for exponential back-off; each subsequent sleep interval is
     * roughly this many times the previous interval.
     */
    public abstract Builder setBackoffMultiplier(double backoffMultiplier);

    /**
     * The maximum amount of sleep between retries; at this point, there will be no further
     * exponential back-off. This prevents intervals from growing unreasonably large.
     */
    public abstract Builder setBackoffMaxIntervalMillis(int backoffMaxIntervalMillis);

    /**
     * The maximum total time elapsed since the first retry over the course of a series of retries.
     * This makes it easier to bound the maximum time it takes to respond to a permanent failure
     * without having to calculate the summation of a series of exponentiated intervals while
     * accounting for the randomization of back-off intervals.
     */
    public abstract Builder setBackoffMaxElapsedTimeMillis(int backoffMaxElapsedTimeMillis);

    /**
     * True if the channel must take special precautions for dealing with "content-encoding" headers
     * where reported object sizes don't match actual bytes being read due to the stream being
     * decoded in-flight. This is not the same as "content-type", and most use cases shouldn't have
     * to worry about this; performance will be improved if this is set to false.
     */
    public abstract Builder setSupportContentEncoding(boolean supportContentEncoding);

    /**
     * If set to a positive value, low-level streams will be wrapped inside a BufferedInputStream of
     * this size. Otherwise no buffer will be created to wrap the low-level streams. Note that the
     * low-level streams may or may not have their own additional buffering layers independent of
     * this setting.
     */
    public abstract Builder setBufferSize(int bufferSize);

    /**
     * If seeking to a new position which is within this number of bytes in front of the current
     * position, then we will skip forward by reading and discarding the necessary amount of bytes
     * rather than trying to open a brand-new underlying stream.
     */
    public abstract Builder setInplaceSeekLimit(long inplaceSeekLimit);

    /**
     * Sets fadvise mode that tunes behavior to optimize HTTP GET requests for various use cases.
     *
     * <p>Supported modes:
     *
     * <ul>
     *   <li>{@code AUTO} - automatically switches to {@code RANDOM} mode if backward read or
     *       forward read for more than {@link #setInplaceSeekLimit} bytes is detected.
     *   <li>{@code RANDOM} - sends HTTP requests with {@code Range} header set to greater of
     *       provided reade buffer by user or {@link #setBufferSize}.
     *   <li>{@code SEQUENTIAL} - sends HTTP requests with unbounded {@code Range} header.
     * </ul>
     */
    public abstract Builder setFadvise(Fadvise fadvise);

    /**
     * Sets size of the footer that will be prefetched when {@link GoogleCloudStorageReadChannel} is
     * created.
     *
     * <p>This allows to reads objects in columnar formats (like Parquet and ORC) that store
     * metadata at the end of the file in the footer faster.
     *
     * <p>To disable footer prefetching set this option to 0.
     */
    public abstract Builder setFooterPrefetchSize(int prefetchSize);

    /**
     * Sets the minimum size of the HTTP Range header that could be set in GCS request when opening
     * new stream to read an object.
     */
    public abstract Builder setMinRangeRequestSize(int size);

    /**
     * Sets the generation read consistency model.
     *
     * <p>Supported modes:
     *
     * <ul>
     *   <li>{@code LATEST}: always read the latest generation.
     *   <li>{@code BEST_EFFORT}: will try to read a certain generation (when the read-channel was
     *       opened), but when that generation is deleted/overwritten, fall back to the latest
     *       generation.
     *   <li>{@code STRICT}: will try to read a certain generation (when the read-channel was
     *       opened), but when that generation is deleted/overwritten, throw an exception.
     * </ul>
     */
    public abstract Builder setGenerationReadConsistency(GenerationReadConsistency consistency);

    abstract GoogleCloudStorageReadOptions autoBuild();

    public GoogleCloudStorageReadOptions build() {
      GoogleCloudStorageReadOptions options = autoBuild();
      checkState(
          options.getInplaceSeekLimit() >= 0,
          "inplaceSeekLimit must be non-negative! Got %s", options.getInplaceSeekLimit());
      return options;
    }
  }
}
