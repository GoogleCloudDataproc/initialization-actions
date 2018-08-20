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

import static com.google.common.truth.Truth.assertThat;

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
import java.io.EOFException;
import java.io.IOError;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import javax.net.ssl.SSLException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
  private GoogleJsonResponseException resourceNotReady;
  private GoogleJsonResponseException bigqueryRateLimited;  // bigquery rate limited
  private static final int POSSIBLE_RATE_LIMIT = 429;  // Can be many things, but not STATUS_CODE_OK

  private final ApiErrorExtractor errorExtractor = ApiErrorExtractor.INSTANCE;

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
    resourceNotReady = googleJsonResponseException(
        400, ApiErrorExtractor.RESOURCE_NOT_READY_REASON_CODE, "Resource not ready");

    // This works because googleJsonResponseException takes final ErrorInfo
    ErrorInfo errorInfo = new ErrorInfo();
    errorInfo.setReason(ApiErrorExtractor.RATE_LIMITED_REASON_CODE);
    notRateLimited = googleJsonResponseException(POSSIBLE_RATE_LIMIT, errorInfo, "");
    errorInfo.setDomain(ApiErrorExtractor.USAGE_LIMITS_DOMAIN);
    rateLimited = googleJsonResponseException(POSSIBLE_RATE_LIMIT, errorInfo, "");
    errorInfo.setDomain(ApiErrorExtractor.GLOBAL_DOMAIN);
    bigqueryRateLimited = googleJsonResponseException(POSSIBLE_RATE_LIMIT, errorInfo, "");
  }

  /**
   * Validates accessDenied().
   */
  @Test
  public void testAccessDenied() {
    // Check success case.
    assertThat(errorExtractor.accessDenied(accessDenied)).isTrue();
    assertThat(errorExtractor.accessDenied(new IOException(accessDenied))).isTrue();
    assertThat(errorExtractor.accessDenied(new IOException(new IOException(accessDenied))))
        .isTrue();

    // Check failure case.
    assertThat(errorExtractor.accessDenied(statusOk)).isFalse();
    assertThat(errorExtractor.accessDenied(new IOException(statusOk))).isFalse();
  }

  /**
   * Validates itemAlreadyExists().
   */
  @Test
  public void testItemAlreadyExists() {
    // Check success cases.
    assertThat(errorExtractor.itemAlreadyExists(alreadyExists)).isTrue();
    assertThat(errorExtractor.itemAlreadyExists(new IOException(alreadyExists))).isTrue();
    assertThat(errorExtractor.itemAlreadyExists(new IOException(new IOException(alreadyExists))))
        .isTrue();

    // Check failure cases.
    assertThat(errorExtractor.itemAlreadyExists(statusOk)).isFalse();
    assertThat(errorExtractor.itemAlreadyExists(new IOException(statusOk))).isFalse();
  }

  /**
   * Validates itemNotFound().
   */
  @Test
  public void testItemNotFound() {
    // Check success cases.
    assertThat(errorExtractor.itemNotFound(notFound)).isTrue();
    GoogleJsonError gje = new GoogleJsonError();
    gje.setCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND);
    assertThat(errorExtractor.itemNotFound(gje)).isTrue();
    assertThat(errorExtractor.itemNotFound(new IOException(notFound))).isTrue();
    assertThat(errorExtractor.itemNotFound(new IOException(new IOException(notFound)))).isTrue();

    // Check failure case.
    assertThat(errorExtractor.itemNotFound(statusOk)).isFalse();
    assertThat(errorExtractor.itemNotFound(new IOException())).isFalse();
    assertThat(errorExtractor.itemNotFound(new IOException(new IOException()))).isFalse();
  }

  /**
   * Validates rangeNotSatisfiable().
   */
  @Test
  public void testRangeNotSatisfiable() {
    // Check success case.
    assertThat(errorExtractor.rangeNotSatisfiable(badRange)).isTrue();
    assertThat(errorExtractor.rangeNotSatisfiable(new IOException(badRange))).isTrue();
    assertThat(errorExtractor.rangeNotSatisfiable(new IOException(new IOException(badRange))))
        .isTrue();

    // Check failure case.
    assertThat(errorExtractor.rangeNotSatisfiable(statusOk)).isFalse();
    assertThat(errorExtractor.rangeNotSatisfiable(notFound)).isFalse();
    assertThat(errorExtractor.rangeNotSatisfiable(new IOException(notFound))).isFalse();
  }

  /**
   * Validates rateLimited().
   */
  @Test
  public void testRateLimited() {
    // Check success case.
    assertThat(errorExtractor.rateLimited(rateLimited)).isTrue();
    assertThat(errorExtractor.rateLimited(new IOException(rateLimited))).isTrue();
    assertThat(errorExtractor.rateLimited(new IOException(new IOException(rateLimited)))).isTrue();

    // Check failure cases.
    assertThat(errorExtractor.rateLimited(notRateLimited)).isFalse();
    assertThat(errorExtractor.rateLimited(new IOException(notRateLimited))).isFalse();
  }

  /**
   * Validates rateLimited() with BigQuery domain / reason codes
   */
  @Test
  public void testBigQueryRateLimited() {
    // Check success case.
    assertThat(errorExtractor.rateLimited(bigqueryRateLimited)).isTrue();
    assertThat(errorExtractor.rateLimited(new IOException(bigqueryRateLimited))).isTrue();
    assertThat(errorExtractor.rateLimited(new IOException(new IOException(bigqueryRateLimited))))
        .isTrue();

    // Check failure cases.
    assertThat(errorExtractor.rateLimited(notRateLimited)).isFalse();
  }

  /**
   * Validates ioError().
   */
  @Test
  public void testIOError() {
    // Check true cases.
    Throwable ioError1 = new EOFException("io error 1");
    assertThat(errorExtractor.ioError(ioError1)).isTrue();
    assertThat(errorExtractor.ioError(new Exception(ioError1))).isTrue();
    assertThat(errorExtractor.ioError(new RuntimeException(new RuntimeException(ioError1))))
        .isTrue();

    Throwable ioError2 = new IOException("io error 2");
    assertThat(errorExtractor.ioError(ioError2)).isTrue();
    assertThat(errorExtractor.ioError(new Exception(ioError2))).isTrue();
    assertThat(errorExtractor.ioError(new RuntimeException(new RuntimeException(ioError2))))
        .isTrue();

    Throwable ioError3 = new IOError(new Exception("io error 3"));
    assertThat(errorExtractor.ioError(ioError3)).isTrue();
    assertThat(errorExtractor.ioError(new Exception(ioError3))).isTrue();
    assertThat(errorExtractor.ioError(new RuntimeException(new RuntimeException(ioError3))))
        .isTrue();

    // Check false cases.
    Throwable notIOError = new Exception("not io error");
    assertThat(errorExtractor.ioError(notIOError)).isFalse();
    assertThat(errorExtractor.ioError(new RuntimeException(notIOError))).isFalse();
  }

  /**
   * Validates socketError().
   */
  @Test
  public void testSocketError() {
    // Check true cases.
    Throwable socketError1 = new SocketTimeoutException("socket error 1");
    assertThat(errorExtractor.socketError(socketError1)).isTrue();
    assertThat(errorExtractor.socketError(new Exception(socketError1))).isTrue();
    assertThat(errorExtractor.socketError(new IOException(new IOException(socketError1)))).isTrue();

    Throwable socketError2 = new SocketException("socket error 2");
    assertThat(errorExtractor.socketError(socketError2)).isTrue();
    assertThat(errorExtractor.socketError(new Exception(socketError2))).isTrue();
    assertThat(errorExtractor.socketError(new IOException(new IOException(socketError2)))).isTrue();

    Throwable socketError3 = new SSLException("ssl exception", new EOFException("eof"));
    assertThat(errorExtractor.socketError(socketError3)).isTrue();
    assertThat(errorExtractor.socketError(new Exception(socketError3))).isTrue();
    assertThat(errorExtractor.socketError(new IOException(new IOException(socketError3)))).isTrue();

    // Check false cases.
    Throwable notSocketError = new Exception("not socket error");
    Throwable notIOError = new Exception("not io error");
    assertThat(errorExtractor.socketError(notSocketError)).isFalse();
    assertThat(errorExtractor.socketError(new IOException(notSocketError))).isFalse();
    assertThat(errorExtractor.socketError(new SSLException("handshake failed", notIOError)))
        .isFalse();
  }

  /**
   * Validates readTimedOut().
   */
  @Test
  public void testReadTimedOut() {
    // Check success case.
    IOException x = new SocketTimeoutException("Read timed out");
    assertThat(errorExtractor.readTimedOut(x)).isTrue();

    // Check failure cases.
    x = new IOException("not a SocketTimeoutException");
    assertThat(errorExtractor.readTimedOut(x)).isFalse();
    x = new SocketTimeoutException("not the right kind of timeout");
    assertThat(errorExtractor.readTimedOut(x)).isFalse();
  }

  /**
   * Validates resourceNotReady().
   */
  @Test
  public void testResourceNotReady() {
    // Check success case.
    assertThat(errorExtractor.resourceNotReady(resourceNotReady)).isTrue();
    assertThat(errorExtractor.resourceNotReady(new IOException(resourceNotReady))).isTrue();
    assertThat(errorExtractor.resourceNotReady(new IOException(new IOException(resourceNotReady))))
        .isTrue();

    // Check failure case.
    assertThat(errorExtractor.resourceNotReady(statusOk)).isFalse();
    assertThat(errorExtractor.resourceNotReady(new IOException(statusOk))).isFalse();
  }

  @Test
  public void testGetErrorMessage() throws IOException {
    IOException withJsonError = googleJsonResponseException(
        42, "Detail Reason", "Detail message", "Top Level HTTP Message");
    assertThat(errorExtractor.getErrorMessage(withJsonError)).isEqualTo("Top Level HTTP Message");

    IOException nullJsonError = googleJsonResponseException(
        42, null, null, "Top Level HTTP Message");
    assertThat(errorExtractor.getErrorMessage(nullJsonError)).isEqualTo("Top Level HTTP Message");
  }

  /**
   * Builds a fake GoogleJsonResponseException for testing API error handling.
   */
  private static GoogleJsonResponseException googleJsonResponseException(
      int httpStatus, String reason, String message) throws IOException {
    return googleJsonResponseException(httpStatus, reason, message, message);
  }

  /**
   * Builds a fake GoogleJsonResponseException for testing API error handling.
   */
  private static GoogleJsonResponseException googleJsonResponseException(
      int httpStatus, String reason, String message, String httpStatusString) throws IOException {
    ErrorInfo errorInfo = new ErrorInfo();
    errorInfo.setReason(reason);
    errorInfo.setMessage(message);
    return googleJsonResponseException(httpStatus, errorInfo, httpStatusString);
  }

  private static GoogleJsonResponseException googleJsonResponseException(
      final int status, final ErrorInfo errorInfo, final String httpStatusString)
      throws IOException {
    final JsonFactory jsonFactory = new JacksonFactory();
    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
            errorInfo.setFactory(jsonFactory);
            GoogleJsonError jsonError = new GoogleJsonError();
            jsonError.setCode(status);
            jsonError.setErrors(Collections.singletonList(errorInfo));
            jsonError.setMessage(httpStatusString);
            jsonError.setFactory(jsonFactory);
            GenericJson errorResponse = new GenericJson();
            errorResponse.set("error", jsonError);
            errorResponse.setFactory(jsonFactory);
            return new MockLowLevelHttpRequest()
                .setResponse(
                    new MockLowLevelHttpResponse()
                        .setContent(errorResponse.toPrettyString())
                        .setContentType(Json.MEDIA_TYPE)
                        .setStatusCode(status));
          }
        };
    HttpRequest request =
        transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
    request.setThrowExceptionOnExecuteError(false);
    HttpResponse response = request.execute();
    return GoogleJsonResponseException.from(jsonFactory, response);
  }
}
