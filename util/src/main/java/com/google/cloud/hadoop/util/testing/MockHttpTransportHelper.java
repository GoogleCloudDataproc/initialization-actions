package com.google.cloud.hadoop.util.testing;

import static com.google.api.client.http.HttpStatusCodes.STATUS_CODE_NOT_FOUND;
import static com.google.api.client.http.HttpStatusCodes.STATUS_CODE_SERVER_ERROR;
import static com.google.api.client.http.HttpStatusCodes.STATUS_CODE_SERVICE_UNAVAILABLE;
import static com.google.cloud.hadoop.util.ApiErrorExtractor.GLOBAL_DOMAIN;
import static com.google.cloud.hadoop.util.ApiErrorExtractor.RATE_LIMITED_REASON;
import static com.google.cloud.hadoop.util.ApiErrorExtractor.USAGE_LIMITS_DOMAIN;
import static com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_RANGE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.http.HttpStatus.SC_GONE;
import static org.apache.http.HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Map;

public final class MockHttpTransportHelper {

  public enum ErrorResponses {
    GONE(SC_GONE, STATUS_CODE_SERVICE_UNAVAILABLE, "backendError", "Backend Error", GLOBAL_DOMAIN),
    NOT_FOUND(STATUS_CODE_NOT_FOUND, "notFound", "Not Found", GLOBAL_DOMAIN),
    RANGE_NOT_SATISFIABLE(
        SC_REQUESTED_RANGE_NOT_SATISFIABLE,
        "requestedRangeNotSatisfiable",
        "Request range not satisfiable",
        GLOBAL_DOMAIN),
    RATE_LIMITED(429, RATE_LIMITED_REASON, "The total number of changes ...", USAGE_LIMITS_DOMAIN),
    SERVER_ERROR(STATUS_CODE_SERVER_ERROR, "backendError", "Backend Error", GLOBAL_DOMAIN);

    private final int responseCode;
    private final int errorCode;
    private final String errorReason;
    private final String errorMessage;
    private final String errorDomain;

    ErrorResponses(int statusCode, String errorReason, String errorMessage, String errorDomain) {
      this(statusCode, statusCode, errorReason, errorMessage, errorDomain);
    }

    ErrorResponses(
        int responseCode,
        int errorCode,
        String errorReason,
        String errorMessage,
        String errorDomain) {
      this.responseCode = responseCode;
      this.errorCode = errorCode;
      this.errorReason = errorReason;
      this.errorMessage = errorMessage;
      this.errorDomain = errorDomain;
    }

    public int getResponseCode() {
      return responseCode;
    }

    public int getErrorCode() {
      return errorCode;
    }

    public String getErrorReason() {
      return errorReason;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    public String getErrorDomain() {
      return errorDomain;
    }
  }

  private static final int UNKNOWN_CONTENT_LENGTH = -1;

  public static final JacksonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private MockHttpTransportHelper() {}

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

  public static MockLowLevelHttpResponse dataRangeResponse(
      byte[] content, long rangeStart, long totalSize) {
    long rangeEnd = rangeStart + content.length - 1;
    return dataResponse(content)
        .addHeader(CONTENT_RANGE, rangeStart + "-" + rangeEnd + "/" + totalSize);
  }

  public static MockLowLevelHttpResponse jsonDataResponse(Object object) throws IOException {
    return dataResponse(JSON_FACTORY.toByteArray(object));
  }

  public static MockLowLevelHttpResponse dataResponse(byte[] content) {
    return dataResponse(ImmutableMap.of(CONTENT_LENGTH, String.valueOf(content.length)), content);
  }

  public static MockLowLevelHttpResponse dataResponse(Map<String, Object> headers, byte[] content) {
    return setHeaders(new MockLowLevelHttpResponse(), headers, (long) content.length)
        .setContent(content);
  }

  public static MockLowLevelHttpResponse jsonErrorResponse(ErrorResponses errorResponse)
      throws IOException {
    GoogleJsonError.ErrorInfo errorInfo = new GoogleJsonError.ErrorInfo();
    errorInfo.setReason(errorResponse.getErrorReason());
    errorInfo.setDomain(errorResponse.getErrorDomain());
    errorInfo.setFactory(JSON_FACTORY);

    GoogleJsonError jsonError = new GoogleJsonError();
    jsonError.setCode(errorResponse.getErrorCode());
    jsonError.setErrors(Collections.singletonList(errorInfo));
    jsonError.setMessage(errorResponse.getErrorMessage());
    jsonError.setFactory(JSON_FACTORY);

    GenericJson errorResponseJson = new GenericJson();
    errorResponseJson.set("error", jsonError);
    errorResponseJson.setFactory(JSON_FACTORY);

    return new MockLowLevelHttpResponse()
        .setContent(errorResponseJson.toPrettyString())
        .setContentType(Json.MEDIA_TYPE)
        .setStatusCode(errorResponse.getResponseCode());
  }

  public static MockLowLevelHttpResponse inputStreamResponse(
      String header, Object headerValue, InputStream content) {
    return inputStreamResponse(ImmutableMap.of(header, headerValue), content);
  }

  public static MockLowLevelHttpResponse inputStreamResponse(
      Map<String, Object> headers, InputStream content) {
    return setHeaders(new MockLowLevelHttpResponse(), headers, UNKNOWN_CONTENT_LENGTH)
        .setContent(content);
  }

  private static MockLowLevelHttpResponse setHeaders(
      MockLowLevelHttpResponse response, Map<String, Object> headers, long defaultContentLength) {
    Object contentLength = headers.getOrDefault(CONTENT_LENGTH, defaultContentLength);
    Object contentEncoding = headers.get(CONTENT_ENCODING);
    headers.forEach((h, hv) -> response.addHeader(h, String.valueOf(hv)));
    return response
        .setContentLength(Long.parseLong(String.valueOf(contentLength)))
        .setContentEncoding(contentEncoding == null ? null : String.valueOf(contentEncoding));
  }
}
