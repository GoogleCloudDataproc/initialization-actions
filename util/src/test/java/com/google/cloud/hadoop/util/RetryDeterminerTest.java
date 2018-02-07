/**
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import javax.net.ssl.SSLException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ResilientOperation}. */
@RunWith(JUnit4.class)
public class RetryDeterminerTest {
  HttpResponseException makeHttpException(int status) throws IOException {
    MockHttpTransport.Builder builder = new MockHttpTransport.Builder();
    MockLowLevelHttpResponse resp = new MockLowLevelHttpResponse();
    resp.setStatusCode(status);
    builder.setLowLevelHttpResponse(resp);
    try {
      HttpResponse res =
          builder.build()
              .createRequestFactory()
              .buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL)
              .execute();
      return new HttpResponseException(res);
    } catch (HttpResponseException exception) {
      return exception; // Throws the exception we want anyway, so just return it.
    }
  }

  @Test
  public void defaultRetriesCorrectly() throws Exception {
    assertThat(RetryDeterminer.DEFAULT.shouldRetry(new SocketTimeoutException())).isTrue();
    assertThat(
            RetryDeterminer.SOCKET_ERRORS.shouldRetry(
                new SSLException("test", new SocketException())))
        .isTrue();
    assertThat(RetryDeterminer.SOCKET_ERRORS.shouldRetry(new SSLException("invalid certificate")))
        .isFalse();
    assertThat(RetryDeterminer.DEFAULT.shouldRetry(new IllegalArgumentException())).isFalse();
    assertThat(RetryDeterminer.DEFAULT.shouldRetry(new InterruptedException())).isFalse();
    assertThat(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(300))).isFalse();
    assertThat(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(504))).isTrue();
    assertThat(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(500))).isTrue();
    assertThat(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(599))).isTrue();
    assertThat(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(499))).isFalse();
  }

  @Test
  public void socketRetriesCorrectly() throws IOException {
    assertThat(RetryDeterminer.SOCKET_ERRORS.shouldRetry(new SocketTimeoutException())).isTrue();
    assertThat(
            RetryDeterminer.SOCKET_ERRORS.shouldRetry(
                new SSLException("test", new SocketException())))
        .isTrue();
    assertThat(RetryDeterminer.SOCKET_ERRORS.shouldRetry(new SSLException("invalid certificate")))
        .isFalse();
    assertThat(RetryDeterminer.SOCKET_ERRORS.shouldRetry(new IOException("Hey"))).isFalse();
    assertThat(RetryDeterminer.SOCKET_ERRORS.shouldRetry(makeHttpException(300))).isFalse();
    assertThat(RetryDeterminer.SOCKET_ERRORS.shouldRetry(makeHttpException(504))).isFalse();
  }

  @Test
  public void serverRetriesCorrectly() throws IOException {
    assertThat(RetryDeterminer.SERVER_ERRORS.shouldRetry(new SocketTimeoutException())).isFalse();
    assertThat(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(300))).isFalse();
    assertThat(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(504))).isTrue();
    assertThat(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(500))).isTrue();
    assertThat(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(599))).isTrue();
    assertThat(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(499))).isFalse();
  }
}
