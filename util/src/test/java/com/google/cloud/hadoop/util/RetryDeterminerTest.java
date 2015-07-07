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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.net.SocketTimeoutException;

import javax.net.ssl.SSLException;

/** Unit tests for {@link ResilientOperation}. */
@RunWith(JUnit4.class)
public class RetryDeterminerTest {
  @Rule
  public ExpectedException exception = ExpectedException.none();
  
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
    assertTrue(RetryDeterminer.DEFAULT.shouldRetry(new SocketTimeoutException()));
    assertFalse(RetryDeterminer.DEFAULT.shouldRetry(new IllegalArgumentException()));
    assertFalse(RetryDeterminer.DEFAULT.shouldRetry(new InterruptedException()));
    assertFalse(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(300)));
    assertTrue(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(504)));
    assertTrue(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(500)));
    assertTrue(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(599)));
    assertFalse(RetryDeterminer.DEFAULT.shouldRetry(makeHttpException(499)));
  }

  @Test
  public void socketRetriesCorrectly() throws IOException {
    assertTrue(RetryDeterminer.SOCKET_ERRORS.shouldRetry(new SocketTimeoutException()));
    assertTrue(RetryDeterminer.SOCKET_ERRORS.shouldRetry(new SSLException("test")));
    assertFalse(RetryDeterminer.SOCKET_ERRORS.shouldRetry(new IOException("Hey")));
    assertFalse(RetryDeterminer.SOCKET_ERRORS.shouldRetry(makeHttpException(300)));
    assertFalse(RetryDeterminer.SOCKET_ERRORS.shouldRetry(makeHttpException(504)));
  }

  @Test
  public void serverRetriesCorrectly() throws IOException {
    assertFalse(RetryDeterminer.SERVER_ERRORS.shouldRetry(new SocketTimeoutException()));
    assertFalse(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(300)));
    assertTrue(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(504)));
    assertTrue(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(500)));
    assertTrue(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(599)));
    assertFalse(RetryDeterminer.SERVER_ERRORS.shouldRetry(makeHttpException(499)));
  }
}
