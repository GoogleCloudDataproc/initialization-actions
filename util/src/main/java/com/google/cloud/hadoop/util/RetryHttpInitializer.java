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
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Set;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryHttpInitializer implements HttpRequestInitializer {

  /** HTTP status code indicating too many requests in a given amount of time. */
  public static final int STATUS_CODE_TOO_MANY_REQUESTS = 429;

  // Logger.
  private static final Logger LOG = LoggerFactory.getLogger(RetryHttpInitializer.class);

  // Base impl of BackOffRequired determining the default set of cases where we'll retry on
  // unsuccessful HTTP responses; we'll mix in additional retriable response cases on top
  // of the bases cases defined by this instance.
  private static final HttpBackOffUnsuccessfulResponseHandler.BackOffRequired
      BASE_HTTP_BACKOFF_REQUIRED =
          HttpBackOffUnsuccessfulResponseHandler.BackOffRequired.ON_SERVER_ERROR;

  // To be used as a request interceptor for filling in the "Authorization" header field, as well
  // as a response handler for certain unsuccessful error codes wherein the Credential must refresh
  // its token for a retry.
  private final Credential credential;

  // If non-null, the backoff handlers will be set to use this sleeper instead of their defaults.
  // Only used for testing.
  private Sleeper sleeperOverride;

  // String to set as user-agent when initializing HttpRequests if none already set.
  private String defaultUserAgent;

  /**
   * A HttpUnsuccessfulResponseHandler logs the URL that generated certain failures.
   */
  private static class LoggingResponseHandler
      implements HttpUnsuccessfulResponseHandler, HttpIOExceptionHandler {

    private final HttpUnsuccessfulResponseHandler delegateResponseHandler;
    private final HttpIOExceptionHandler delegateIOExceptionHandler;
    private final ImmutableSet<Integer> responseCodesToLog;

    /**
     * @param delegateResponseHandler The HttpUnsuccessfulResponseHandler to invoke to
     *    really handle errors.
     * @param delegateIOExceptionHandler The HttpIOExceptionResponseHandler to delegate to.
     * @param responseCodesToLog The set of response codes to log URLs for.
     */
    public LoggingResponseHandler(
        HttpUnsuccessfulResponseHandler delegateResponseHandler,
        HttpIOExceptionHandler delegateIOExceptionHandler,
        Set<Integer> responseCodesToLog) {
      this.delegateResponseHandler = delegateResponseHandler;
      this.delegateIOExceptionHandler = delegateIOExceptionHandler;
      this.responseCodesToLog = ImmutableSet.copyOf(responseCodesToLog);
    }

    @Override
    public boolean handleResponse(
        HttpRequest httpRequest, HttpResponse httpResponse, boolean supportsRetry)
        throws IOException {
      if (responseCodesToLog.contains(httpResponse.getStatusCode())) {
        LOG.info(
            "Encountered status code {} when accessing URL {}. "
                + "Delegating to response handler for possible retry.",
            httpResponse.getStatusCode(),
            httpRequest.getUrl());
      }

      return delegateResponseHandler.handleResponse(httpRequest, httpResponse, supportsRetry);
    }

    @Override
    public boolean handleIOException(HttpRequest httpRequest, boolean supportsRetry)
        throws IOException {
      // We sadly don't get anything helpful to see if this is something we want to log. As a result
      // we'll turn down the logging level to debug.
      LOG.debug("Encountered an IOException when accessing URL {}", httpRequest.getUrl());
      return delegateIOExceptionHandler.handleIOException(httpRequest, supportsRetry);
    }
  }

  /**
   * An inner class allowing this initializer to create a new handler instance per HttpRequest
   * which shares the Credential of the outer class and which will compose the Credential with
   * a backoff handler to handle unsuccessful HTTP codes.
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
                  || response.getStatusCode() == STATUS_CODE_TOO_MANY_REQUESTS;
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
          LOG.debug(
              "Redirect path '{}' contains unescaped '+', replacing with '%2B': '{}'",
              redirectLocation, escapedLocation);
          response.getHeaders().setLocation(escapedLocation);
        }
      }

      return false;
    }
  }

  /**
   * @param credential A credential which will be set as an interceptor on HttpRequests and
   *     as the delegate for a CredentialOrBackoffResponsehandler.
   * @param defaultUserAgent A String to set as the user-agent when initializing an HttpRequest
   *     if the HttpRequest doesn't already have a user-agent header.
   */
  public RetryHttpInitializer(Credential credential, String defaultUserAgent) {
    Preconditions.checkNotNull(credential, "A valid Credential is required");
    this.credential = credential;
    this.sleeperOverride = null;
    this.defaultUserAgent = defaultUserAgent;
  }

  @Override
  public void initialize(HttpRequest request) {
    // Credential must be the interceptor to fill in accessToken fields.
    request.setInterceptor(credential);

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
            ImmutableSet.of(HttpStatus.SC_GONE, HttpStatus.SC_SERVICE_UNAVAILABLE));
    request.setUnsuccessfulResponseHandler(loggingResponseHandler);
    request.setIOExceptionHandler(loggingResponseHandler);

    if (Strings.isNullOrEmpty(request.getHeaders().getUserAgent())) {
      LOG.debug("Request is missing a user-agent, adding default value of '{}'", defaultUserAgent);
      request.getHeaders().setUserAgent(defaultUserAgent);
    }
  }

  /**
   * Overrides the default Sleepers used in backoff retry handler instances.
   */
  @VisibleForTesting
  void setSleeperOverride(Sleeper sleeper) {
    sleeperOverride = sleeper;
  }
}
