/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link HttpRequestInitializer} that composes handlers and interceptors
 * supplied by component handlers. All interceptors in a chain are executed.
 * All handlers are executed until a handler returns true from its handle
 * method. If an initializer configures other parameters on request, last
 * initializer to do so wins.
 */
public class ChainingHttpRequestInitializer implements HttpRequestInitializer {

  private final List<HttpRequestInitializer> initializers;

  public ChainingHttpRequestInitializer(HttpRequestInitializer...initializers) {
    this.initializers = ImmutableList.copyOf(initializers);
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    List<HttpIOExceptionHandler> ioExceptionHandlers = new ArrayList<>();
    List<HttpUnsuccessfulResponseHandler> unsuccessfulResponseHandlers = new ArrayList<>();
    List<HttpExecuteInterceptor> interceptors = new ArrayList<>();
    List<HttpResponseInterceptor> responseInterceptors = new ArrayList<>();
    for (HttpRequestInitializer initializer : initializers) {
      initializer.initialize(request);
      if (request.getIOExceptionHandler() != null) {
        ioExceptionHandlers.add(request.getIOExceptionHandler());
        request.setIOExceptionHandler(null);
      }
      if (request.getUnsuccessfulResponseHandler() != null) {
        unsuccessfulResponseHandlers.add(request.getUnsuccessfulResponseHandler());
        request.setUnsuccessfulResponseHandler(null);
      }
      if (request.getInterceptor() != null) {
        interceptors.add(request.getInterceptor());
        request.setInterceptor(null);
      }
      if (request.getResponseInterceptor() != null) {
        responseInterceptors.add(request.getResponseInterceptor());
        request.setResponseInterceptor(null);
      }
    }
    request.setIOExceptionHandler(
        makeIoExceptionHandler(ioExceptionHandlers));
    request.setUnsuccessfulResponseHandler(
        makeUnsuccessfulResponseHandler(unsuccessfulResponseHandlers));
    request.setInterceptor(
        makeInterceptor(interceptors));
    request.setResponseInterceptor(
        makeResponseInterceptor(responseInterceptors));
  }

  private HttpResponseInterceptor makeResponseInterceptor(
      final Iterable<HttpResponseInterceptor> responseInterceptors) {
    return new HttpResponseInterceptor() {
      @Override
      public void interceptResponse(HttpResponse response) throws IOException {
        for (HttpResponseInterceptor interceptor : responseInterceptors) {
          interceptor.interceptResponse(response);
        }
      }
    };
  }

  private HttpExecuteInterceptor makeInterceptor(
      final Iterable<HttpExecuteInterceptor> interceptors) {
    return new HttpExecuteInterceptor() {
      @Override
      public void intercept(HttpRequest request) throws IOException {
        for (HttpExecuteInterceptor interceptor : interceptors) {
          interceptor.intercept(request);
        }
      }
    };
  }

  private HttpUnsuccessfulResponseHandler makeUnsuccessfulResponseHandler(
      final Iterable<HttpUnsuccessfulResponseHandler> unsuccessfulResponseHandlers) {
    return new HttpUnsuccessfulResponseHandler() {
      @Override
      public boolean handleResponse(HttpRequest request, HttpResponse response,
          boolean supportsRetry) throws IOException {
        for (HttpUnsuccessfulResponseHandler handler : unsuccessfulResponseHandlers) {
          if (handler.handleResponse(request, response, supportsRetry)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  private HttpIOExceptionHandler makeIoExceptionHandler(
      final Iterable<HttpIOExceptionHandler> ioExceptionHandlers) {
    return new HttpIOExceptionHandler() {
      @Override
      public boolean handleIOException(HttpRequest request, boolean supportsRetry)
          throws IOException {
        for (HttpIOExceptionHandler handler : ioExceptionHandlers) {
          if (handler.handleIOException(request, supportsRetry)) {
            return true;
          }
        }
        return false;
      }
    };
  }
}
