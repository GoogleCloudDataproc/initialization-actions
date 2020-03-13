/*
 * Copyright 2019 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.gcsio.cooplock;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.time.Duration;

/** Options for the Cooperative Locking feature. */
@AutoValue
public abstract class CooperativeLockingOptions {

  /** Default value for {@link #getLockExpirationTimeoutMilli}. */
  public static final long LOCK_EXPIRATION_TIMEOUT_MS_DEFAULT = Duration.ofMinutes(2).toMillis();

  /** Default value for {@link #getMaxConcurrentOperations}. */
  public static final int MAX_CONCURRENT_OPERATIONS_DEFAULT = 20;

  public static final CooperativeLockingOptions DEFAULT = builder().build();

  private static final long MIN_LOCK_EXPIRATION_TIMEOUT_MS = Duration.ofSeconds(30).toMillis();

  public static Builder builder() {
    return new AutoValue_CooperativeLockingOptions.Builder()
        .setLockExpirationTimeoutMilli(LOCK_EXPIRATION_TIMEOUT_MS_DEFAULT)
        .setMaxConcurrentOperations(MAX_CONCURRENT_OPERATIONS_DEFAULT);
  }

  public abstract Builder toBuilder();

  public abstract long getLockExpirationTimeoutMilli();

  public abstract int getMaxConcurrentOperations();

  /** Builder for {@link CooperativeLockingOptions} */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setLockExpirationTimeoutMilli(long timeoutMilli);

    public abstract Builder setMaxConcurrentOperations(int operationsCount);

    abstract CooperativeLockingOptions autoBuild();

    public CooperativeLockingOptions build() {
      CooperativeLockingOptions options = autoBuild();
      checkArgument(
          options.getLockExpirationTimeoutMilli() >= MIN_LOCK_EXPIRATION_TIMEOUT_MS,
          "lockExpirationTimeoutMs should be greater or equal to %s ms, but was %s",
          MIN_LOCK_EXPIRATION_TIMEOUT_MS,
          options.getLockExpirationTimeoutMilli());
      checkArgument(
          options.getMaxConcurrentOperations() > 0,
          "maxConcurrentOperations should be greater than 0, but was %s",
          options.getMaxConcurrentOperations());
      return options;
    }
  }
}
