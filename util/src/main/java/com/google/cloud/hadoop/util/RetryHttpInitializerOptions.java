/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.hadoop.util;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Map;
import javax.annotation.Nullable;

/** Options for the {@link RetryHttpInitializer} class. */
@AutoValue
public abstract class RetryHttpInitializerOptions {

  public static final String DEFAULT_DEFAULT_USER_AGENT = null;
  public static final ImmutableMap<String, String> DEFAULT_HTTP_HEADERS = ImmutableMap.of();
  public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(20);
  public static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(20);
  public static final int DEFAULT_MAX_REQUEST_RETRIES = 10;

  public static final RetryHttpInitializerOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_RetryHttpInitializerOptions.Builder()
        .setDefaultUserAgent(DEFAULT_DEFAULT_USER_AGENT)
        .setHttpHeaders(DEFAULT_HTTP_HEADERS)
        .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT)
        .setReadTimeout(DEFAULT_READ_TIMEOUT)
        .setMaxRequestRetries(DEFAULT_MAX_REQUEST_RETRIES);
  }

  public abstract Builder toBuilder();

  /**
   * A String to set as the user-agent when initializing an HTTP request if it doesn't already have
   * a "User-Agent" header.
   */
  @Nullable
  public abstract String getDefaultUserAgent();

  /** An additional HTTP headers to set in an HTTP request. */
  public abstract ImmutableMap<String, String> getHttpHeaders();

  /** A connection timeout - maximum time to wait until connection will be established. */
  public abstract Duration getConnectTimeout();

  /**
   * A read timeout from an established connection - maximum time to wait for a read form an
   * established connection to finish.
   */
  public abstract Duration getReadTimeout();

  /** A max number of retries for an HTTP request. */
  public abstract int getMaxRequestRetries();

  /** Builder for {@link RetryHttpInitializerOptions} */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDefaultUserAgent(String userAgent);

    public abstract Builder setHttpHeaders(Map<String, String> httpHeaders);

    public abstract Builder setConnectTimeout(Duration connectTimeout);

    public abstract Builder setReadTimeout(Duration readTimeout);

    public abstract Builder setMaxRequestRetries(int maxRequestRetries);

    public abstract RetryHttpInitializerOptions build();
  }
}
