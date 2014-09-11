/**
 * Copyright 2013 Google Inc. All Rights Reserved.
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

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

/**
 * Translates exceptions from API calls into higher-level meaning, while allowing injectability
 * for testing how API errors are handled.
 */
public class ApiErrorExtractor {
  // TODO(user): Move this into HttpStatusCodes.java.
  public static final int STATUS_CODE_RANGE_NOT_SATISFIABLE = 416;
  public static final String USAGE_LIMITS_DOMAIN = "usageLimits";
  public static final String RATE_LIMITED_REASON_CODE = "rateLimitExceeded";

  /**
   * Determines if the given exception indicates 'item not found'.
   */
  public boolean itemNotFound(IOException e) {
    if (e instanceof GoogleJsonResponseException) {
      return (getHttpStatusCode((GoogleJsonResponseException) e))
          == HttpStatusCodes.STATUS_CODE_NOT_FOUND;
    }
    return false;
  }

  /**
   * Determines if the given GoogleJsonError indicates 'item not found'.
   */
  public boolean itemNotFound(GoogleJsonError e) {
    return e.getCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND;
  }

  /**
   * Determines if the given exception indicates 'range not satisfiable'.
   */
  public boolean rangeNotSatisfiable(IOException e) {
    if (e instanceof GoogleJsonResponseException) {
      return (getHttpStatusCode((GoogleJsonResponseException) e))
          == STATUS_CODE_RANGE_NOT_SATISFIABLE;
    }
    return false;
  }

  /**
   * Determines if the given exception indicates 'access denied'.
   */
  public boolean accessDenied(GoogleJsonResponseException e) {
    return getHttpStatusCode(e) == HttpStatusCodes.STATUS_CODE_FORBIDDEN;
  }

  /**
   * Determines if the given exception indicates 'access denied', recursively checking inner
   * getCause() if outer exception isn't an instance of the correct class.
   */
  public boolean accessDenied(IOException e) {
    return (e.getCause() != null)
        && (e.getCause() instanceof GoogleJsonResponseException)
        && accessDenied((GoogleJsonResponseException) e.getCause());
  }

  /**
   * Returns HTTP status code from the given exception.
   *
   * Note: GoogleJsonResponseException.getStatusCode() method is marked final therefore
   * it cannot be mocked using Mockito. We use this helper so that we can override it in tests.
   */
  @VisibleForTesting
  int getHttpStatusCode(GoogleJsonResponseException e) {
    return e.getStatusCode();
  }

  /**
   * Determine if a given IOException is caused by a rate limit being applied.
   * @param ioe The IOException to check.
   * @return True if the IOException is a result of rate limiting being applied.
   */
  public boolean rateLimited(IOException ioe) {
    if (ioe instanceof GoogleJsonResponseException) {
      GoogleJsonResponseException googleJsonResponseException = (GoogleJsonResponseException) ioe;
      return rateLimited(googleJsonResponseException.getDetails());
    }
    return false;
  }


  /**
   * Determine if a given GoogleJsonError is caused by, and only by, a rate limit being applied.
   * @param e The GoogleJsonError returned by the request
   * @return True if the error is caused by a rate limit being applied.
   */
  public boolean rateLimited(GoogleJsonError e) {
    return e.getErrors() != null
        && e.getErrors().size() == 1
        && USAGE_LIMITS_DOMAIN.equals(e.getErrors().get(0).getDomain())
        && RATE_LIMITED_REASON_CODE.equals(e.getErrors().get(0).getReason());
  }
}
