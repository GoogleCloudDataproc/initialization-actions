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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.UUID;

/** Utility class with helper methods for GCS IO tests. */
public final class GoogleCloudStorageTestUtils {

  public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  public static final JacksonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final String GOOGLEAPIS_ENDPOINT = "https://www.googleapis.com";
  private static final String RESUMABLE_UPLOAD_LOCATION_FORMAT =
      GOOGLEAPIS_ENDPOINT + "/upload/storage/v1/b/%s/o?name=%s&uploadType=resumable&upload_id=%s";

  static final String BUCKET_NAME = "foo-bucket";
  static final String OBJECT_NAME = "bar-object";

  private static final ApiErrorExtractor ERROR_EXTRACTOR = ApiErrorExtractor.INSTANCE;
  private static final ClientRequestHelper<StorageObject> REQUEST_HELPER =
      new ClientRequestHelper<>();

  private GoogleCloudStorageTestUtils() {}

  public static GoogleCloudStorageReadChannel createReadChannel(
      Storage storage, GoogleCloudStorageReadOptions options) throws IOException {
    return new GoogleCloudStorageReadChannel(
        storage, BUCKET_NAME, OBJECT_NAME, ERROR_EXTRACTOR, REQUEST_HELPER, options);
  }

  public static HttpResponse fakeResponse(String header, Object headerValue, InputStream content)
      throws IOException {
    return fakeResponse(ImmutableMap.of(header, headerValue), content);
  }

  public static HttpResponse fakeResponse(Map<String, Object> headers, InputStream content)
      throws IOException {
    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                headers.forEach((h, hv) -> response.addHeader(h, String.valueOf(hv)));
                return response.setContent(content);
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

  public static MockHttpTransport mockBatchTransport(
      int requestsPerBatch, LowLevelHttpResponse... responses) {
    return new MockHttpTransport() {
      int responsesIndex = 0;

      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) {
        return new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() {
            String boundary = "batch_pK7JBAk73-E=_AA5eFwv4m2Q=";
            String contentId = "";

            MockLowLevelHttpResponse response =
                new MockLowLevelHttpResponse()
                    .setStatusCode(HttpStatusCodes.STATUS_CODE_OK)
                    .setContentType("multipart/mixed; boundary=" + boundary);

            StringBuilder batchResponse = new StringBuilder();

            for (int i = 0; i < requestsPerBatch; i++) {
              try {
                LowLevelHttpResponse resp = responses[responsesIndex++];
                batchResponse
                    .append(String.format("\n--%s\n", boundary))
                    .append("Content-Type: application/http\n")
                    .append(String.format("Content-ID: <response-%s%d>\n\n", contentId, i + 1))
                    .append(String.format("HTTP/1.1 %s OK\n", resp.getStatusCode()))
                    .append(String.format("Content-Length: %s\n\n", resp.getContentLength()))
                    .append(CharStreams.toString(new InputStreamReader(resp.getContent(), UTF_8)))
                    .append('\n');
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            batchResponse.append(String.format("\n--%s--\n", boundary));

            return response.setContent(batchResponse.toString());
          }
        };
      }
    };
  }

  public static MockLowLevelHttpResponse emptyResponse(int statusCode) {
    return new MockLowLevelHttpResponse().setStatusCode(statusCode);
  }

  public static MockLowLevelHttpResponse metadataResponse(StorageObject metadataObject)
      throws IOException {
    return dataResponse(JSON_FACTORY.toByteArray(metadataObject));
  }

  public static MockLowLevelHttpResponse dataRangeResponse(
      byte[] content, long rangeStart, long totalSize) {
    long rangeEnd = rangeStart + content.length - 1;
    return dataResponse(content)
        .addHeader("Content-Range", rangeStart + "-" + rangeEnd + "/" + totalSize);
  }

  public static MockLowLevelHttpResponse dataResponse(byte[] content) {
    return new MockLowLevelHttpResponse()
        .addHeader("Content-Length", String.valueOf(content.length))
        .setContent(content);
  }

  public static MockLowLevelHttpResponse resumableUploadResponse(String bucket, String object) {
    String uploadId = UUID.randomUUID().toString();
    return new MockLowLevelHttpResponse()
        .addHeader(
            "location", String.format(RESUMABLE_UPLOAD_LOCATION_FORMAT, bucket, object, uploadId));
  }
}
