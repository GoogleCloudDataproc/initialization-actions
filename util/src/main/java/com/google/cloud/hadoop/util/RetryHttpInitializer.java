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
package com.google.cloud.hadoop.util;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpStatus;

public class RetryHttpInitializer implements HttpRequestInitializer {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /** HTTP status code indicating too many requests in a given amount of time. */
  private static final int HTTP_SC_TOO_MANY_REQUESTS = 429;

  // Base impl of BackOffRequired determining the default set of cases where we'll retry on
  // unsuccessful HTTP responses; we'll mix in additional retriable response cases on top
  // of the bases cases defined by this instance.
  private static final HttpBackOffUnsuccessfulResponseHandler.BackOffRequired
      BASE_HTTP_BACKOFF_REQUIRED =
          HttpBackOffUnsuccessfulResponseHandler.BackOffRequired.ON_SERVER_ERROR;

  // Default number of retries.
  private static final int DEFAULT_MAX_REQUEST_RETRIES = HttpRequest.DEFAULT_NUMBER_OF_RETRIES;

  // Default number of connection timeout (20 seconds).
  private static final int DEFAULT_CONNECT_TIMEOUT = 20 * 1000;

  // Default number of read timeout (20 seconds).
  private static final int DEFAULT_READ_TIMEOUT = 20 * 1000;

  private static final ImmutableMap<String, String> DEFAULT_HTTP_HEADERS = ImmutableMap.of();

  // To be used as a request interceptor for filling in the "Authorization" header field, as well
  // as a response handler for certain unsuccessful error codes wherein the Credential must refresh
  // its token for a retry.
  private final Credential credential;

  // If non-null, the backoff handlers will be set to use this sleeper instead of their defaults.
  // Only used for testing.
  private Sleeper sleeperOverride;

  // String to set as user-agent when initializing HttpRequests if none already set.
  private String defaultUserAgent;

  // Max number of retires.
  private final int maxRequestRetries;

  // Connect timeout, in milliseconds.
  private final int connectTimeoutMillis;

  // Read timeout, in milliseconds.
  private final int readTimeoutMillis;

  // HTTP request headers.
  private final ImmutableMap<String, String> headers;

  /** A HttpUnsuccessfulResponseHandler logs the URL that generated certain failures. */
  private static class LoggingResponseHandler
      implements HttpUnsuccessfulResponseHandler, HttpIOExceptionHandler {

    private static final String LOG_MESSAGE_FORMAT =
        "Encountered status code %d when accessing URL '%s'. "
            + "Delegating to response handler for possible retry.";

    private final HttpUnsuccessfulResponseHandler delegateResponseHandler;
    private final HttpIOExceptionHandler delegateIOExceptionHandler;
    private final ImmutableSet<Integer> responseCodesToLog;
    private final ImmutableSet<Integer> responseCodesToLogWithRateLimit;

    /**
     * @param delegateResponseHandler The HttpUnsuccessfulResponseHandler to invoke to really handle
     *     errors.
     * @param delegateIOExceptionHandler The HttpIOExceptionResponseHandler to delegate to.
     * @param responseCodesToLog The set of response codes to log URLs for.
     * @param responseCodesToLogWithRateLimit The set of response codes to log URLs for with reate
     *     limit.
     */
    public LoggingResponseHandler(
        HttpUnsuccessfulResponseHandler delegateResponseHandler,
        HttpIOExceptionHandler delegateIOExceptionHandler,
        Set<Integer> responseCodesToLog,
        Set<Integer> responseCodesToLogWithRateLimit) {
      this.delegateResponseHandler = delegateResponseHandler;
      this.delegateIOExceptionHandler = delegateIOExceptionHandler;
      this.responseCodesToLog = ImmutableSet.copyOf(responseCodesToLog);
      this.responseCodesToLogWithRateLimit = ImmutableSet.copyOf(responseCodesToLogWithRateLimit);
    }

    @Override
    public boolean handleResponse(
        HttpRequest httpRequest, HttpResponse httpResponse, boolean supportsRetry)
        throws IOException {
      if (responseCodesToLogWithRateLimit.contains(httpResponse.getStatusCode())) {
        switch (httpResponse.getStatusCode()) {
          case HTTP_SC_TOO_MANY_REQUESTS:
            logger.atInfo().atMostEvery(10, SECONDS).log(
                LOG_MESSAGE_FORMAT, httpResponse.getStatusCode(), httpRequest.getUrl());
            break;
          default:
            logger.atInfo().atMostEvery(10, SECONDS).log(
                "Encountered status code %d (and maybe others) when accessing URL '%s'."
                    + " Delegating to response handler for possible retry.",
                httpResponse.getStatusCode(), httpRequest.getUrl());
        }
      } else if (responseCodesToLog.contains(httpResponse.getStatusCode())) {
        logger.atInfo().log(LOG_MESSAGE_FORMAT, httpResponse.getStatusCode(), httpRequest.getUrl());
      }

      return delegateResponseHandler.handleResponse(httpRequest, httpResponse, supportsRetry);
    }

    @Override
    public boolean handleIOException(HttpRequest httpRequest, boolean supportsRetry)
        throws IOException {
      // We sadly don't get anything helpful to see if this is something we want to log. As a result
      // we'll turn down the logging level to debug.
      logger.atFine().log("Encountered an IOException when accessing URL %s", httpRequest.getUrl());
      return delegateIOExceptionHandler.handleIOException(httpRequest, supportsRetry);
    }
  }

  /**
   * An inner class allowing this initializer to create a new handler instance per HttpRequest which
   * shares the Credential of the outer class and which will compose the Credential with a backoff
   * handler to handle unsuccessful HTTP codes.
   */
  private class CredentialOrBackoffResponseHandler implements HttpUnsuccessfulResponseHandler {
    // The backoff-handler instance to use whenever the outer-class's Credential does not handle
    // the error.
    private final HttpUnsuccessfulResponseHandler delegateHandler;

    public CredentialOrBackoffResponseHandler() {
      HttpBackOffUnsuccessfulResponseHandler errorCodeHandler =
          new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff());
      errorCodeHandler.setBackOffRequired(
          new HttpBackOffUnsuccessfulResponseHandler.BackOffRequired() {
            @Override
            public boolean isRequired(HttpResponse response) {
              return BASE_HTTP_BACKOFF_REQUIRED.isRequired(response)
                  || response.getStatusCode() == HTTP_SC_TOO_MANY_REQUESTS;
            }
          });
      if (sleeperOverride != null) {
        errorCodeHandler.setSleeper(sleeperOverride);
      }
      this.delegateHandler = errorCodeHandler;
    }

    @Override
    public boolean handleResponse(HttpRequest request, HttpResponse response, boolean supportsRetry)
        throws IOException {
      if (credential.handleResponse(request, response, supportsRetry)) {
        // If credential decides it can handle it, the return code or message indicated something
        // specific to authentication, and no backoff is desired.
        return true;
      }

      if (delegateHandler.handleResponse(request, response, supportsRetry)) {
        // Otherwise, we defer to the judgement of our internal backoff handler.
        return true;
      }

      if (HttpStatusCodes.isRedirect(response.getStatusCode())
          && request.getFollowRedirects()
          && response.getHeaders() != null
          && response.getHeaders().getLocation() != null) {
        // Hack: Reach in and fix any '+' in the URL but still report 'false'. The client library
        // incorrectly tries to decode '+' into ' ', even though the backend servers treat '+'
        // as a legitimate path character, and so do not encode it. This is safe to do whether
        // or not the client library fixes the bug, since %2B will correctly be decoded as '+'
        // even after the fix.
        String redirectLocation = response.getHeaders().getLocation();
        if (redirectLocation.contains("+")) {
          String escapedLocation = redirectLocation.replace("+", "%2B");
          logger.atFine().log(
              "Redirect path '%s' contains unescaped '+', replacing with '%%2B': '%s'",
              redirectLocation, escapedLocation);
          response.getHeaders().setLocation(escapedLocation);
        }
      }

      return false;
    }
  }

  /**
   * @param credential A credential which will be set as an interceptor on HttpRequests and as the
   *     delegate for a CredentialOrBackoffResponsehandler.
   * @param defaultUserAgent A String to set as the user-agent when initializing an HttpRequest if
   *     the HttpRequest doesn't already have a user-agent header.
   * @param maxRequestRetries An int to indicate the max number of retries of an HttpRequest.
   * @param connectTimeoutMillis An int to indicate the number of milliseconds for connection
   *     timeout. Use {@code 0} for infinite timeout.
   * @param readTimeoutMillis An int to indicate the number of milliseconds for read timeout from an
   *     established connection. Use {@code 0} for infinite timeout.
   */
  public RetryHttpInitializer(
      Credential credential,
      String defaultUserAgent,
      int maxRequestRetries,
      int connectTimeoutMillis,
      int readTimeoutMillis,
      Map<String, String> headers) {
    Preconditions.checkNotNull(credential, "A valid Credential is required");
    this.credential = credential;
    this.sleeperOverride = null;
    this.defaultUserAgent = defaultUserAgent;
    this.maxRequestRetries = maxRequestRetries;
    this.connectTimeoutMillis = connectTimeoutMillis;
    this.readTimeoutMillis = readTimeoutMillis;
    this.headers = ImmutableMap.copyOf(headers);
  }

  /**
   * {@code maxRequestRetries} defaults to {@link RetryHttpInitializer#DEFAULT_MAX_REQUEST_RETRIES}.
   *
   * <p>{@code connectTimeoutMillis} defaults to {@link
   * RetryHttpInitializer#DEFAULT_CONNECT_TIMEOUT}.
   *
   * <p>{@code readTimeoutMillis} defaults to {@link RetryHttpInitializer#DEFAULT_READ_TIMEOUT}.
   *
   * @param credential A credential which will be set as an interceptor on HttpRequests and as the
   *     delegate for a CredentialOrBackoffResponseHandler.
   * @param defaultUserAgent A String to set as the user-agent when initializing an HttpRequest if
   *     the HttpRequest doesn't already have a user-agent header.
   */
  public RetryHttpInitializer(Credential credential, String defaultUserAgent) {
    this(
        credential,
        defaultUserAgent,
        DEFAULT_MAX_REQUEST_RETRIES,
        DEFAULT_CONNECT_TIMEOUT,
        DEFAULT_READ_TIMEOUT,
        DEFAULT_HTTP_HEADERS);
  }

  @Override
  public void initialize(HttpRequest request) {
    // Credential must be the interceptor to fill in accessToken fields.
    request.setInterceptor(credential);

    // Request will be retried if server errors (5XX) or I/O errors are encountered.
    request.setNumberOfRetries(maxRequestRetries);

    // Set the timeout configurations.
    request.setConnectTimeout(connectTimeoutMillis);
    request.setReadTimeout(readTimeoutMillis);

    // IOExceptions such as "socket timed out" of "insufficient bytes written" will follow a
    // straightforward backoff.
    HttpBackOffIOExceptionHandler exceptionHandler =
        new HttpBackOffIOExceptionHandler(new ExponentialBackOff());
    if (sleeperOverride != null) {
      exceptionHandler.setSleeper(sleeperOverride);
    }

    // Supply a new composite handler for unsuccessful return codes. 401 Unauthorized will be
    // handled by the Credential, 410 Gone will be logged, and 5XX will be handled by a backoff
    // handler.
    LoggingResponseHandler loggingResponseHandler =
        new LoggingResponseHandler(
            new CredentialOrBackoffResponseHandler(),
            exceptionHandler,
            ImmutableSet.of(HttpStatus.SC_GONE, HttpStatus.SC_SERVICE_UNAVAILABLE),
            ImmutableSet.of(HTTP_SC_TOO_MANY_REQUESTS));
    request.setUnsuccessfulResponseHandler(loggingResponseHandler);
    request.setIOExceptionHandler(loggingResponseHandler);

    if (Strings.isNullOrEmpty(request.getHeaders().getUserAgent())) {
      logger.atFiner().log(
          "Request is missing a user-agent, adding default value of '%s'", defaultUserAgent);
      request.getHeaders().setUserAgent(defaultUserAgent);
    }

    for (Map.Entry<String, String> header : headers.entrySet()) {
      request.getHeaders().set(header.getKey(), header.getValue());
    }
  }

  /** Overrides the default Sleepers used in backoff retry handler instances. */
  @VisibleForTesting
  void setSleeperOverride(Sleeper sleeper) {
    sleeperOverride = sleeper;
  }
}
