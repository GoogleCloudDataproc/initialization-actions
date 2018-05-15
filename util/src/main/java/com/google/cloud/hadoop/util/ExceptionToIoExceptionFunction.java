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

import com.google.common.base.Function;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Converter function from {@link Exception} to {@link IOException}.
 */
public class ExceptionToIoExceptionFunction implements Function<Exception, IOException> {

  public static final Function<Exception, IOException> INSTANCE
    = new ExceptionToIoExceptionFunction();

  private ExceptionToIoExceptionFunction() {
    // Singleton.
  }

  @Override
  public IOException apply(Exception exception) {
    if (exception instanceof ExecutionException) {
      if (exception.getCause() instanceof IOException) {
        return (IOException) exception.getCause();
      }
    } else if (exception instanceof IOException) {
      return (IOException) exception;
    }
    return new IOException(exception);
  }
}
