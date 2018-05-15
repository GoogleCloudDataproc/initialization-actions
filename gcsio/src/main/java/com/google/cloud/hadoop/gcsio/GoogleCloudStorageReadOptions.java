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

import com.google.common.base.Preconditions;

/**
 * Advanced options for reading GoogleCloudStorage objects. Immutable; callers must use the
 * inner Builder class to construct instances.
 */
public class GoogleCloudStorageReadOptions {
  public static final GoogleCloudStorageReadOptions DEFAULT = new Builder().build();

  public static final int DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS = 200;
  public static final double DEFAULT_BACKOFF_RANDOMIZATION_FACTOR = 0.5;
  public static final double DEFAULT_BACKOFF_MULTIPLIER = 1.5;
  public static final int DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS = 10 * 1000;
  public static final int DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS = 2 * 60 * 1000;
  public static final boolean DEFAULT_SUPPORT_CONTENT_ENCODING = true;
  public static final boolean DEFAULT_FAST_FAIL_ON_NOT_FOUND = true;
  public static final int DEFAULT_BUFFER_SIZE = 0;
  public static final long DEFAULT_INPLACE_SEEK_LIMIT = 0L;

  /**
   * Mutable builder for GoogleCloudStorageReadOptions.
   */
  public static class Builder {
    private int backoffInitialIntervalMillis = DEFAULT_BACKOFF_INITIAL_INTERVAL_MILLIS;
    private double backoffRandomizationFactor = DEFAULT_BACKOFF_RANDOMIZATION_FACTOR;
    private double backoffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
    private int backoffMaxIntervalMillis = DEFAULT_BACKOFF_MAX_INTERVAL_MILLIS;
    private int backoffMaxElapsedTimeMillis = DEFAULT_BACKOFF_MAX_ELAPSED_TIME_MILLIS;
    private boolean supportContentEncoding = DEFAULT_SUPPORT_CONTENT_ENCODING;
    private boolean fastFailOnNotFound = DEFAULT_FAST_FAIL_ON_NOT_FOUND;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private long inplaceSeekLimit = DEFAULT_INPLACE_SEEK_LIMIT;

    /**
     * On exponential backoff, the initial delay before the first retry; subsequent retries then
     * grow as an exponential function of the current delay interval.
     */
    public Builder setBackoffInitialIntervalMillis(
        int backoffInitialIntervalMillis) {
      this.backoffInitialIntervalMillis = backoffInitialIntervalMillis;
      return this;
    }

    /**
     * The amount of jitter introduced when computing the next retry sleep interval so that when
     * many clients are retrying, they don't all retry at the same time.
     */
    public Builder setBackoffRandomizationFactor(
        double backoffRandomizationFactor) {
      this.backoffRandomizationFactor = backoffRandomizationFactor;
      return this;
    }

    /**
     * The base of the exponent used for exponential backoff; each subsequent sleep interval is
     * roughly this many times the previous interval.
     */
    public Builder setBackoffMultiplier(double backoffMultiplier) {
      this.backoffMultiplier = backoffMultiplier;
      return this;
    }

    /**
     * The maximum amount of sleep between retries; at this point, there will be no further
     * exponential backoff. This prevents intervals from growing unreasonably large.
     */
    public Builder setBackoffMaxIntervalMillis(int backoffMaxIntervalMillis) {
      this.backoffMaxIntervalMillis = backoffMaxIntervalMillis;
      return this;
    }

    /**
     * The maximum total time elapsed since the first retry over the course of a series of retries.
     * This makes it easier to bound the maximum time it takes to respond to a permanent failure
     * without having to calculate the summation of a series of exponentiated intervals while
     * accounting for the randomization of backoff intervals.
     */
    public Builder setBackoffMaxElapsedTimeMillis(
        int backoffMaxElapsedTimeMillis) {
      this.backoffMaxElapsedTimeMillis = backoffMaxElapsedTimeMillis;
      return this;
    }

    /**
     * True if the channel must take special precautions for deailing with "content-encoding"
     * headers where reported object sizes don't match actual bytes being read due to the stream
     * being decoded in-flight. This is not the same as "content-type", and most use cases
     * shouldn't have to worry about this; performance will be improved if this is set to false.
     */
    public Builder setSupportContentEncoding(boolean supportContentEncoding) {
      this.supportContentEncoding = supportContentEncoding;
      return this;
    }

    /**
     * True if attempts to open a new channel on a nonexistent object are required to immediately
     * throw an IOException. If false, then channels may not throw exceptions for such cases
     * until attempting to call read(). Performance can be improved if this is set to false and
     * the caller is equipped to deal with delayed failures for not-found objects. Or if the caller
     * is already sure the object being opened exists, it is recommended to set this to false to
     * aGoogleCloudStorageReadOptions doing extraneous checks on open().
     */
    public Builder setFastFailOnNotFound(boolean fastFailOnNotFound) {
      this.fastFailOnNotFound = fastFailOnNotFound;
      return this;
    }

    /**
     * If set to a positive value, low-level streams will be wrapped inside a BufferedInputStream
     * of this size. Otherwise no buffer will be created to wrap the low-level streams. Note that
     * the low-level streams may or may not have their own additional buffering layers independent
     * of this setting.
     */
    public Builder setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    /**
     * If seeking to a new position which is within this number of bytes in front of the current
     * position, then we will skip forward by reading and discarding the necessary amount of bytes
     * rather than trying to open a brand-new underlying stream.
     */
    public Builder setInplaceSeekLimit(long inplaceSeekLimit) {
      Preconditions.checkArgument(inplaceSeekLimit >= 0,
          "inplaceSeekLimit must be non-negative! Got %s", inplaceSeekLimit);
      this.inplaceSeekLimit = inplaceSeekLimit;
      return this;
    }

    public GoogleCloudStorageReadOptions build() {
      return new GoogleCloudStorageReadOptions(
          backoffInitialIntervalMillis,
          backoffRandomizationFactor,
          backoffMultiplier,
          backoffMaxIntervalMillis,
          backoffMaxElapsedTimeMillis,
          supportContentEncoding,
          fastFailOnNotFound,
          bufferSize,
          inplaceSeekLimit);
    }
  }

  private final int backoffInitialIntervalMillis;
  private final double backoffRandomizationFactor;
  private final double backoffMultiplier;
  private final int backoffMaxIntervalMillis;
  private final int backoffMaxElapsedTimeMillis;
  private final boolean supportContentEncoding;
  private final boolean fastFailOnNotFound;
  private final int bufferSize;
  private final long inplaceSeekLimit;

  /**
   * Should only be used by Builder to protect callers from changing constructor signatures when
   * new options are added.
   */
  private GoogleCloudStorageReadOptions(
      int backoffInitialIntervalMillis,
      double backoffRandomizationFactor,
      double backoffMultiplier,
      int backoffMaxIntervalMillis,
      int backoffMaxElapsedTimeMillis,
      boolean supportContentEncoding,
      boolean fastFailOnNotFound,
      int bufferSize,
      long inplaceSeekLimit) {
    this.backoffInitialIntervalMillis = backoffInitialIntervalMillis;
    this.backoffRandomizationFactor = backoffRandomizationFactor;
    this.backoffMultiplier = backoffMultiplier;
    this.backoffMaxIntervalMillis = backoffMaxIntervalMillis;
    this.backoffMaxElapsedTimeMillis = backoffMaxElapsedTimeMillis;
    this.supportContentEncoding = supportContentEncoding;
    this.fastFailOnNotFound = fastFailOnNotFound;
    this.bufferSize = bufferSize;
    this.inplaceSeekLimit = inplaceSeekLimit;
  }

  /**
   * See {@link #Builder.setBackoffInitialIntervalMillis}.
   */
  public int getBackoffInitialIntervalMillis() {
    return backoffInitialIntervalMillis;
  }

  /**
   * See {@link #Builder.setBackoffRandomizationFactor}.
   */
  public double getBackoffRandomizationFactor() {
    return backoffRandomizationFactor;
  }

  /**
   * See {@link #Builder.setBackoffMultiplier}.
   */
  public double getBackoffMultiplier() {
    return backoffMultiplier;
  }

  /**
   * See {@link #Builder.setBackoffMaxIntervalMillis}.
   */
  public int getBackoffMaxIntervalMillis() {
    return backoffMaxIntervalMillis;
  }

  /**
   * See {@link #Builder.setBackoffMaxElapsedTimeMillis}.
   */
  public int getBackoffMaxElapsedTimeMillis() {
    return backoffMaxElapsedTimeMillis;
  }

  /**
   * See {@link #Builder.setSupportContentEncoding}.
   */
  public boolean getSupportContentEncoding() {
    return supportContentEncoding;
  }

  /**
   * See {@link #Builder.setFastFailOnNotFound}.
   */
  public boolean getFastFailOnNotFound() {
    return fastFailOnNotFound;
  }

  /**
   * See {@link #Builder.setBufferSize}.
   */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * See {@link #Builder.setInplaceSeekLimit}.
   */
  public long getInplaceSeekLimit() {
    return inplaceSeekLimit;
  }

  /**
   * Summary of options.
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("backoffInitialIntervalMillis=" + backoffInitialIntervalMillis + " ");
    sb.append("backoffRandomizationFactor=" + backoffRandomizationFactor + " ");
    sb.append("backoffMultiplier=" + backoffMultiplier + " ");
    sb.append("backoffMaxIntervalMillis=" + backoffMaxIntervalMillis + " ");
    sb.append("backoffMaxElapsedTimeMillis=" + backoffMaxElapsedTimeMillis + " ");
    sb.append("supportContentEncoding=" + supportContentEncoding + " ");
    sb.append("fastFailOnNotFound=" + fastFailOnNotFound + " ");
    sb.append("bufferSize=" + bufferSize + " ");
    sb.append("inplaceSeekLimit=" + inplaceSeekLimit + " ");
    return sb.toString();
  }
}
