/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.commons.codec.CharEncoding.UTF_8;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TrackingHttpRequestInitializer implements HttpRequestInitializer {

  private static final String GET_REQUEST_FORMAT =
      "GET:https://www.googleapis.com/storage/v1/b/%s/o/%s";

  private static final String POST_REQUEST_FORMAT =
      "POST:https://www.googleapis.com/storage/v1/b/%s/o/%s";

  private static final String UPLOAD_REQUEST_FORMAT =
      "POST:https://www.googleapis.com/upload/storage/v1/b/%s/o"
          + "?ifGenerationMatch=0&uploadType=multipart:%s";

  private static final String UPDATE_METADATA_REQUEST_FORMAT =
      "POST:https://www.googleapis.com/storage/v1/b/%s/o/%s?ifMetagenerationMatch=%d";

  private static final String DELETE_REQUEST_FORMAT =
      "DELETE:https://www.googleapis.com/storage/v1/b/%s/o/%s?ifMetagenerationMatch=%d";

  private static final String LIST_REQUEST_FORMAT =
      "GET:https://www.googleapis.com/storage/v1/b/%s/o"
          + "?delimiter=/&includeTrailingDelimiter=%s&maxResults=%d%s&prefix=%s";

  private static final String BATCH_REQUEST_FORMAT =
      "POST:https://www.googleapis.com/batch/storage/v1";

  private static final String PAGE_TOKEN_PARAM_PATTERN = "&pageToken=[^&]+";

  private final HttpRequestInitializer delegate;

  private final List<HttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

  public TrackingHttpRequestInitializer(HttpRequestInitializer delegate) {
    this.delegate = delegate;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    delegate.initialize(request);
    HttpExecuteInterceptor executeInterceptor = checkNotNull(request.getInterceptor());
    request.setInterceptor(
        r -> {
          executeInterceptor.intercept(r);
          requests.add(r);
        });
  }

  public ImmutableList<HttpRequest> getAllRequests() {
    return ImmutableList.copyOf(requests);
  }

  public ImmutableList<String> getAllRequestStrings() {
    AtomicLong pageTokenId = new AtomicLong();
    return requests.stream()
        .map(GoogleCloudStorageIntegrationHelper::requestToString)
        // Replace randomized pageToken with predictable value so it could be asserted in tests
        .map(r -> replacePageTokenWithId(r, pageTokenId.getAndIncrement()))
        .collect(toImmutableList());
  }

  private String replacePageTokenWithId(String request, long pageTokenId) {
    return request.replaceAll(PAGE_TOKEN_PARAM_PATTERN, "&pageToken=token_" + pageTokenId);
  }

  public void reset() {
    requests.clear();
  }

  public static String getRequestString(String bucketName, String object) {
    return String.format(GET_REQUEST_FORMAT, bucketName, object);
  }

  public static String postRequestString(String bucketName, String object) {
    return String.format(POST_REQUEST_FORMAT, bucketName, object);
  }

  public static String uploadRequestString(
      String bucketName, String object, boolean generationMatch) {
    String request = String.format(UPLOAD_REQUEST_FORMAT, bucketName, object);
    return generationMatch ? request : request.replaceAll("ifGenerationMatch=0&", "");
  }

  public static String updateMetadataRequestString(
      String bucketName, String object, int metaGeneration) throws UnsupportedEncodingException {
    return String.format(
        UPDATE_METADATA_REQUEST_FORMAT,
        bucketName,
        URLEncoder.encode(object, UTF_8),
        metaGeneration);
  }

  public static String deleteRequestString(String bucketName, String object, long metaGeneration)
      throws UnsupportedEncodingException {
    return String.format(
        DELETE_REQUEST_FORMAT, bucketName, URLEncoder.encode(object, UTF_8), metaGeneration);
  }

  public static String batchRequestString() {
    return String.format(BATCH_REQUEST_FORMAT);
  }

  public static String listRequestString(
      String bucket, String prefix, int maxResults, String pageToken) {
    return listRequestString(
        bucket, /* includeTrailingDelimiter= */ false, prefix, maxResults, pageToken);
  }

  public static String listRequestString(
      String bucket,
      boolean includeTrailingDelimiter,
      String prefix,
      int maxResults,
      String pageToken) {
    String pageTokenParam = pageToken == null ? "" : "&pageToken=" + pageToken;
    return String.format(
        LIST_REQUEST_FORMAT, bucket, includeTrailingDelimiter, maxResults, pageTokenParam, prefix);
  }
}
