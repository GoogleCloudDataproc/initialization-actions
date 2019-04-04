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

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class TrackingHttpRequestInitializer implements HttpRequestInitializer {

  private final HttpRequestInitializer delegate;

  private final Collection<HttpRequest> requests = Collections.synchronizedList(new ArrayList<>());

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

  public Collection<HttpRequest> getAllRequests() {
    return requests;
  }

  public void reset() {
    requests.clear();
  }
}
