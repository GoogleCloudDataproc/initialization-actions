/**
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Arrays;

/**
 * Unit-tests for ApiErrorExtractor class.
 */
@RunWith(JUnit4.class)
public class ApiErrorExtractorTest {
  private GoogleJsonResponseException accessDenied;  // STATUS_CODE_FORBIDDEN
  private GoogleJsonResponseException statusOk;  // STATUS_CODE_OK
  private GoogleJsonResponseException notFound;  // STATUS_CODE_NOT_FOUND
  private GoogleJsonResponseException badRange;  // STATUS_CODE_RANGE_NOT_SATISFIABLE;
  private GoogleJsonResponseException alreadyExists;  // STATUS_CODE_CONFLICT
  private GoogleJsonResponseException rateLimited;  // rate limited
  private GoogleJsonResponseException notRateLimited;  // not rate limited because of domain
  private GoogleJsonResponseException bigqueryRateLimited;  // bigquery rate limited
  private static final int POSSIBLE_RATE_LIMIT = 429;  // Can be many things, but not STATUS_CODE_OK

  private ApiErrorExtractor errorExtractor = new ApiErrorExtractor();

  @Before
  public void setUp() throws Exception {
    accessDenied = googleJsonResponseException(
        HttpStatusCodes.STATUS_CODE_FORBIDDEN, "Forbidden", "Forbidden");
    statusOk = googleJsonResponseException(
        HttpStatusCodes.STATUS_CODE_OK, "A reason", "ok");
    notFound = googleJsonResponseException(
        HttpStatusCodes.STATUS_CODE_NOT_FOUND, "Not found", "Not found");
    badRange = googleJsonResponseException(
        ApiErrorExtractor.STATUS_CODE_RANGE_NOT_SATISFIABLE, "Bad range", "Bad range");
    alreadyExists = googleJsonResponseException(
        409, "409", "409");

    // This works because googleJsonResponseException takes final ErrorInfo
    ErrorInfo errorInfo = new ErrorInfo();
    errorInfo.setReason(ApiErrorExtractor.RATE_LIMITED_REASON_CODE);
    notRateLimited = googleJsonResponseException(POSSIBLE_RATE_LIMIT, errorInfo);
    errorInfo.setDomain(ApiErrorExtractor.USAGE_LIMITS_DOMAIN);
    rateLimited = googleJsonResponseException(POSSIBLE_RATE_LIMIT, errorInfo);
    errorInfo.setDomain(ApiErrorExtractor.GLOBAL_DOMAIN);
    bigqueryRateLimited = googleJsonResponseException(POSSIBLE_RATE_LIMIT, errorInfo);
  }

  /**
   * Validates accessDenied().
   */
  @Test
  public void testAccessDenied() {
    // Check success case.
    assertTrue(errorExtractor.accessDenied(accessDenied));
    assertTrue(errorExtractor.accessDenied(new IOException(accessDenied)));
    assertTrue(errorExtractor.accessDenied(
        new IOException(new IOException(accessDenied))));

    // Check failure case.
    assertFalse(errorExtractor.accessDenied(statusOk));
    assertFalse(errorExtractor.accessDenied(new IOException(statusOk)));
  }

  /**
   * Validates itemAlreadyExists().
   */
  @Test
  public void testItemAlreadyExists() {
    // Check success cases.
    assertTrue(errorExtractor.itemAlreadyExists(alreadyExists));
    assertTrue(errorExtractor.itemAlreadyExists(new IOException(alreadyExists)));
    assertTrue(errorExtractor.itemAlreadyExists(
        new IOException(new IOException(alreadyExists))));

    // Check failure cases.
    assertFalse(errorExtractor.itemAlreadyExists(statusOk));
    assertFalse(errorExtractor.itemAlreadyExists(new IOException(statusOk)));
  }

  /**
   * Validates itemNotFound().
   */
  @Test
  public void testItemNotFound() {
    // Check success cases.
    assertTrue(errorExtractor.itemNotFound(notFound));
    GoogleJsonError gje = new GoogleJsonError();
    gje.setCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND);
    assertTrue(errorExtractor.itemNotFound(gje));
    assertTrue(errorExtractor.itemNotFound(new IOException(notFound)));
    assertTrue(errorExtractor.itemNotFound(new IOException(new IOException(notFound))));

    // Check failure case.
    assertFalse(errorExtractor.itemNotFound(statusOk));
    assertFalse(errorExtractor.itemNotFound(new IOException()));
    assertFalse(errorExtractor.itemNotFound(new IOException(new IOException())));
  }

  /**
   * Validates rangeNotSatisfiable().
   */
  @Test
  public void testRangeNotSatisfiable() {
    // Check success case.
    assertTrue(errorExtractor.rangeNotSatisfiable(badRange));
    assertTrue(errorExtractor.rangeNotSatisfiable(new IOException(badRange)));
    assertTrue(errorExtractor.rangeNotSatisfiable(
        new IOException(new IOException(badRange))));

    // Check failure case.
    assertFalse(errorExtractor.rangeNotSatisfiable(statusOk));
    assertFalse(errorExtractor.rangeNotSatisfiable(notFound));
    assertFalse(errorExtractor.rangeNotSatisfiable(new IOException(notFound)));
  }

  /**
   * Validates rateLimited().
   */
  @Test
  public void testRateLimited() {
    // Check success case.
    assertTrue(errorExtractor.rateLimited(rateLimited));
    assertTrue(errorExtractor.rateLimited(new IOException(rateLimited)));
    assertTrue(errorExtractor.rateLimited(new IOException(new IOException(rateLimited))));

    // Check failure cases.
    assertFalse(errorExtractor.rateLimited(notRateLimited));
    assertFalse(errorExtractor.rateLimited(new IOException(notRateLimited)));
  }

  /**
   * Validates rateLimited() with BigQuery domain / reason codes
   */
  @Test
  public void testBigQueryRateLimited() {
    // Check success case.
    assertTrue(errorExtractor.rateLimited(bigqueryRateLimited));
    assertTrue(errorExtractor.rateLimited(new IOException(bigqueryRateLimited)));
    assertTrue(errorExtractor.rateLimited(
        new IOException(new IOException(bigqueryRateLimited))));

    // Check failure cases.
    assertFalse(errorExtractor.rateLimited(notRateLimited));
  }

  /**
   * Validates readTimedOut().
   */
  @Test
  public void testReadTimedOut() {
    // Check success case.
    IOException x = new SocketTimeoutException("Read timed out");
    assertTrue(errorExtractor.readTimedOut(x));

    // Check failure cases.
    x = new IOException("not a SocketTimeoutException");
    assertFalse(errorExtractor.readTimedOut(x));
    x = new SocketTimeoutException("not the right kind of timeout");
    assertFalse(errorExtractor.readTimedOut(x));
  }

  /**
   * Builds a fake GoogleJsonResponseException for testing API error handling.
   */
  private static GoogleJsonResponseException googleJsonResponseException(
      final int status, final String reason, final String message) throws IOException {
    ErrorInfo errorInfo = new ErrorInfo();
    errorInfo.setReason(reason);
    errorInfo.setMessage(message);
    return googleJsonResponseException(status, errorInfo);
  }

  private static GoogleJsonResponseException googleJsonResponseException(
      final int status, final ErrorInfo errorInfo) throws IOException {
    final JsonFactory jsonFactory = new JacksonFactory();
    HttpTransport transport = new MockHttpTransport() {
      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
        errorInfo.setFactory(jsonFactory);
        GenericJson error = new GenericJson();
        error.set("code", status);
        error.set("errors", Arrays.asList(errorInfo));
        error.setFactory(jsonFactory);
        GenericJson errorResponse = new GenericJson();
        errorResponse.set("error", error);
        errorResponse.setFactory(jsonFactory);
        return new MockLowLevelHttpRequest().setResponse(
            new MockLowLevelHttpResponse().setContent(errorResponse.toPrettyString())
            .setContentType(Json.MEDIA_TYPE).setStatusCode(status));
        }
    };
    HttpRequest request =
        transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
    request.setThrowExceptionOnExecuteError(false);
    HttpResponse response = request.execute();
    return GoogleJsonResponseException.from(jsonFactory, response);
  }
}
