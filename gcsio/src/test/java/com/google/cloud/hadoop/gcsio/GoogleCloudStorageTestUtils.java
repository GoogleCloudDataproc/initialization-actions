/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import java.io.IOException;
import java.io.InputStream;

/** Utility class with helper methods for GCS IO tests. */
public final class GoogleCloudStorageTestUtils {

  public static final JacksonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final String BUCKET_NAME = "foo-bucket";
  private static final String OBJECT_NAME = "bar-object";

  private static final ApiErrorExtractor ERROR_EXTRACTOR = ApiErrorExtractor.INSTANCE;
  private static final ClientRequestHelper<StorageObject> REQUEST_HELPER =
      new ClientRequestHelper<>();

  private GoogleCloudStorageTestUtils() {}

  public static GoogleCloudStorageReadChannel createReadChannel(
      Storage storage, GoogleCloudStorageReadOptions options) throws IOException {
    return new GoogleCloudStorageReadChannel(
        storage, BUCKET_NAME, OBJECT_NAME, ERROR_EXTRACTOR, REQUEST_HELPER, options);
  }

  public static HttpResponse fakeResponseWithLength(long contentLength, InputStream content)
      throws IOException {
    return fakeResponse("Content-Length", Long.toString(contentLength), content);
  }

  public static HttpResponse fakeResponseWithRange(long contentLength, InputStream content)
      throws IOException {
    return fakeResponse("Content-Range", "bytes=0-123/" + contentLength, content);
  }

  public static HttpResponse fakeResponse(
      String responseHeader, String responseValue, InputStream content) throws IOException {
    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                return new MockLowLevelHttpResponse()
                    .addHeader(responseHeader, responseValue)
                    .setContent(content);
              }
            };
          }
        };
    HttpRequest request =
        transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
    return request.execute();
  }

  public static MockHttpTransport mockTransport(LowLevelHttpResponse... responsesIn) {
    return new MockHttpTransport() {
      int responsesIndex = 0;
      final LowLevelHttpResponse[] responses = responsesIn;

      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) {
        return new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() {
            return responses[responsesIndex++];
          }
        };
      }
    };
  }

  public static MockLowLevelHttpResponse metadataResponse(StorageObject metadataObject)
      throws IOException {
    return dataResponse(JSON_FACTORY.toByteArray(metadataObject));
  }

  public static MockLowLevelHttpResponse dataResponse(byte[] content) {
    return new MockLowLevelHttpResponse()
        .addHeader("Content-Length", String.valueOf(content.length))
        .setContent(content);
  }
}
