/**
 * Copyright 2015 Google Inc. All Rights Reserved.
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
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;

import java.io.IOError;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

/**
 * Translates exceptions from API calls into higher-level meaning, while allowing injectability
 * for testing how API errors are handled.
 */
public class ApiErrorExtractor {
  // TODO(user): Move this into HttpStatusCodes.java.
  public static final int STATUS_CODE_CONFLICT = 409;
  public static final int STATUS_CODE_PRECONDITION_FAILED = 412;
  public static final int STATUS_CODE_RANGE_NOT_SATISFIABLE = 416;
  public static final String GLOBAL_DOMAIN = "global";
  public static final String USAGE_LIMITS_DOMAIN = "usageLimits";
  public static final String RATE_LIMITED_REASON_CODE = "rateLimitExceeded";
  public static final String USER_RATE_LIMITED_REASON_CODE = "userRateLimitExceeded";

  // These come with "The account for ... has been disabled" message.
  public static final String ACCOUNT_DISABLED_REASON_CODE = "accountDisabled";

  // These come with "Project marked for deletion" message.
  public static final String ACCESS_NOT_CONFIGURED_REASON_CODE = "accessNotConfigured";

  // These are 400 error codes with "resource 'xyz' is not ready" message.
  // These sometimes happens when create operation is still in-flight but resource
  // representation is already available via get call.
  // Only explanation I could find for this is described here:
  //    java/com/google/cloud/cluster/data/cognac/cognac.proto
  // with an example "because resource is being created in reconciler."
  public static final String RESOURCE_NOT_READY_REASON_CODE = "resourceNotReady";

  // HTTP 413 with message "Value for field 'foo' is too large".
  public static final String FIELD_SIZE_TOO_LARGE = "fieldSizeTooLarge";

  // Public methods here are in alphabetical order.


  /**
   * Determines if the given exception indicates 'access denied'.
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   *
   * <p> Warning: this method only checks for access denied status code,
   * however this may include potentially recoverable reason codes such as
   * rate limiting. For alternative, see
   * {@link #accessDeniedNonRecoverable(IOException)}.
   */
  public boolean accessDenied(IOException e) {
    return recursiveCheckForCode(e, HttpStatusCodes.STATUS_CODE_FORBIDDEN);
  }


  /**
   * Determine if the given exception indicates the request was unauthenticated.
   * This can be caused by attaching invalid credentials to a request.
   */
  public boolean unauthorized(IOException e) {
    return recursiveCheckForCode(e, HttpStatusCodes.STATUS_CODE_UNAUTHORIZED);
  }

  /**
   * Determine if the exception is a non-recoverable access denied code
   * (such as account closed or marked for deletion).
   */
  public boolean accessDeniedNonRecoverable(IOException e) {
    GoogleJsonResponseException jsonException = getJsonResponseExceptionOrNull(e);
    if (jsonException != null) {
      return accessDeniedNonRecoverable(getDetails(jsonException));
    }
    return false;
  }

  /**
   * Determine if a given GoogleJsonError is caused by, and only by,
   * account disabled error.
   */
  public boolean accessDeniedNonRecoverable(GoogleJsonError e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    if (errorInfo != null) {
      String reason = errorInfo.getReason();
      return ACCOUNT_DISABLED_REASON_CODE.equals(reason)
          || ACCESS_NOT_CONFIGURED_REASON_CODE.equals(reason);
    }
    return false;
  }

  /**
   * Determines if the exception is a client error.
   */
  public boolean isClientError(IOException e) {
    GoogleJsonResponseException jsonException = getJsonResponseExceptionOrNull(e);
    if (jsonException != null) {
      return (getHttpStatusCode(jsonException)) / 100 == 4;
    }
    return false;
  }

  /**
   * Determines if the exception is an internal server error.
   */
  public boolean isInternalServerError(IOException e) {
    GoogleJsonResponseException jsonException = getJsonResponseExceptionOrNull(e);
    if (jsonException != null) {
      return (getHttpStatusCode(jsonException)) / 100 == 5;
    }
    return false;
  }

  /**
   * Determines if the given exception indicates 'item already exists'.
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   */
  public boolean itemAlreadyExists(IOException e) {
      return recursiveCheckForCode(e, STATUS_CODE_CONFLICT);
  }

  /**
   * Determines if the given GoogleJsonError indicates 'item not found'.
   */
  public boolean itemNotFound(GoogleJsonError e) {
    return e.getCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND;
  }

  /**
   * Determines if the given exception indicates 'item not found'.
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   */
  public boolean itemNotFound(IOException e) {
    return recursiveCheckForCode(e, HttpStatusCodes.STATUS_CODE_NOT_FOUND);
  }

  /**
   * Determines if the given GoogleJsonError indicates 'field size too large'.
   */
  public boolean fieldSizeTooLarge(GoogleJsonError e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    if (errorInfo != null) {
      String reason = errorInfo.getReason();
      return FIELD_SIZE_TOO_LARGE.equals(reason);
    }
    return false;
  }

  /**
   * Determines if the given exception indicates 'field size too large'.
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   */
  public boolean fieldSizeTooLarge(IOException e) {
    GoogleJsonResponseException jsonException = getJsonResponseExceptionOrNull(e);
    if (jsonException != null) {
      return fieldSizeTooLarge(getDetails(jsonException));
    }
    return false;
  }

  /**
   * Determines if the given GoogleJsonError indicates 'resource not ready'.
   */
  public boolean resourceNotReady(GoogleJsonError e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    if (errorInfo != null) {
      String reason = errorInfo.getReason();
      return RESOURCE_NOT_READY_REASON_CODE.equals(reason);
    }
    return false;
  }

  /**
   * Determines if the given exception indicates 'resource not ready'.
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   */
  public boolean resourceNotReady(IOException e) {
    GoogleJsonResponseException jsonException = getJsonResponseExceptionOrNull(e);
    if (jsonException != null) {
      return resourceNotReady(getDetails(jsonException));
    }
    return false;
  }

  /**
   * Determines if the given GoogleJsonError indicates 'precondition not met'
   */
  public boolean preconditionNotMet(GoogleJsonError e) {
    return e.getCode() == STATUS_CODE_PRECONDITION_FAILED;
  }

  /**
   * Determine if the given IOException indicates 'precondition not met'
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   */
  public boolean preconditionNotMet(IOException e) {
    return recursiveCheckForCode(e, STATUS_CODE_PRECONDITION_FAILED);
  }

  /**
   * Determines if the given exception indicates 'range not satisfiable'.
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   */
  public boolean rangeNotSatisfiable(IOException e) {
    return recursiveCheckForCode(e, STATUS_CODE_RANGE_NOT_SATISFIABLE);
  }

  /**
   * Determine if a given GoogleJsonError is caused by, and only by,
   * a rate limit being applied.
   * @param e The GoogleJsonError returned by the request
   * @return True if the error is caused by a rate limit being applied.
   */
  public boolean rateLimited(GoogleJsonError e) {
    ErrorInfo errorInfo = getErrorInfo(e);
    if (errorInfo != null) {
      String domain = errorInfo.getDomain();
      String reason = errorInfo.getReason();
      boolean isRateLimitedOrGlobalDomain =
          USAGE_LIMITS_DOMAIN.equals(domain) || GLOBAL_DOMAIN.equals(domain);
      boolean isRateLimitedReason =
          RATE_LIMITED_REASON_CODE.equals(reason) || USER_RATE_LIMITED_REASON_CODE.equals(reason);
      return isRateLimitedOrGlobalDomain && isRateLimitedReason;
    }
    return false;
  }

  /**
   * Determine if a given Throwable is caused by a rate limit being applied.
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   * @param throwable The Throwable to check.
   * @return True if the Throwable is a result of rate limiting being applied.
   */
  // TODO(user): change to accept IOException, in line with other methods.
  public boolean rateLimited(Throwable throwable) {
    GoogleJsonResponseException jsonException = getJsonResponseExceptionOrNull(throwable);
    if (jsonException != null) {
      return rateLimited(getDetails(jsonException));
    }
    return false;
  }

  /**
   * Determine if a given Throwable is caused by an IO error.
   * Recursively checks getCause() if outer exception isn't an instance of the
   * correct class.
   *
   * @param throwable The Throwable to check.
   * @return True if the Throwable is a result of an IO error.
   */

  public boolean ioError(Throwable throwable) {
    if (throwable instanceof IOException || throwable instanceof IOError) {
      return true;
    }
    return throwable.getCause() != null && ioError(throwable.getCause());
  }

  /**
   * Determine if a given Throwable is caused by a socket error.
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   * @param throwable The Throwable to check.
   * @return True if the Throwable is a result of a socket error.
   */
  public boolean socketError(Throwable throwable) {
    if (throwable instanceof SocketException || throwable instanceof SocketTimeoutException) {
      return true;
    }
    Throwable cause = throwable.getCause();
    // Subset of SSL exceptions that are caused by IO errors (e.g. SSLHandshakeException due to
    // unexpected connection closure) is also a socket error.
    if (throwable instanceof SSLException && cause != null && ioError(cause)) {
      return true;
    }
    return cause != null && socketError(cause);
  }

  /**
   * True if the exception is a "read timed out".
   */
  public boolean readTimedOut(IOException ex) {
    if (!(ex instanceof SocketTimeoutException)) {
      return false;
    }
    return (ex.getMessage().equals("Read timed out"));
  }

  /**
   * Extracts the error message.
   */
  public String getErrorMessage(IOException e) {
    if (e instanceof GoogleJsonResponseException) {
      GoogleJsonResponseException gjre = ((GoogleJsonResponseException) e);
      if (gjre.getDetails() != null) {
        return gjre.getDetails().getMessage();
      }
    }
    return e.getMessage();
  }

  /**
   * Converts the exception to a user-presentable error message. Specifically,
   * extracts message field for HTTP 4xx codes, and creates a generic
   * "Internal Server Error" for HTTP 5xx codes.
   *
   * @param ioe the exception
   * @param action the description of the action being performed at the time of error.
   * @see #toUserPresentableMessage(IOException, String)
   */
  public IOException toUserPresentableException(IOException ioe, String action) throws IOException {
    throw new IOException(toUserPresentableMessage(ioe, action), ioe);
  }

  /**
   * Converts the exception to a user-presentable error message. Specifically,
   * extracts message field for HTTP 4xx codes, and creates a generic
   * "Internal Server Error" for HTTP 5xx codes.
   */
  public String toUserPresentableMessage(IOException ioe, @Nullable String action) {
    String message = "Internal server error";
    if (isClientError(ioe)) {
      message = getErrorMessage(ioe);
    }

    if (action == null) {
      return message;
    } else {
      return String.format("Encountered an error while %s: %s", action, message);
    }
  }

  /**
   * @see #toUserPresentableMessage(IOException, String)
   */
  public String toUserPresentableMessage(IOException ioe) {
    return toUserPresentableMessage(ioe, null);
  }

  /**
   * Returns HTTP status code from the given exception.
   *
   * <p> Note: GoogleJsonResponseException.getStatusCode() method is marked final
   * therefore it cannot be mocked using Mockito. We use this helper so that
   * we can override it in tests.
   */
  protected int getHttpStatusCode(GoogleJsonResponseException e) {
    return e.getStatusCode();
  }

  /**
   * Get the first ErrorInfo from an IOException if it is an instance of
   * GoogleJsonResponseException, otherwise return null.
   */
  protected ErrorInfo getErrorInfo(IOException e) {
    GoogleJsonError gjre = getDetails(e);
    if (gjre != null) {
      return getErrorInfo(gjre);
    }
    return null;
  }

  /**
   * If the exception is a GoogleJsonResponseException, get the
   * error details, else return null.
   */
  protected GoogleJsonError getDetails(IOException e) {
    if (e instanceof GoogleJsonResponseException) {
      GoogleJsonResponseException ex = (GoogleJsonResponseException) e;
      return ex.getDetails();
    } else {
      return null;
    }
  }

  /**
   * Get the first ErrorInfo from a GoogleJsonError, or null if
   * there is not one.
   */
  protected ErrorInfo getErrorInfo(GoogleJsonError details) {
    if (details == null) {
      return null;
    }
    List<ErrorInfo> errors = details.getErrors();
    if (errors.isEmpty()) {
      return null;
    } else {
      return errors.get(0);
    }
  }

  /**
   * Recursively checks getCause() if outer exception isn't
   * an instance of the correct class.
   */
  protected boolean recursiveCheckForCode(Throwable e, int code) {
    GoogleJsonResponseException jsonException = getJsonResponseExceptionOrNull(e);
    if (jsonException != null) {
      return getHttpStatusCode(jsonException) == code;
    }
    return false;
  }

  @Nullable
  private static GoogleJsonResponseException getJsonResponseExceptionOrNull(Throwable t) {
    Throwable cause = t;
    while (cause != null) {
      if (cause instanceof GoogleJsonResponseException) {
        return (GoogleJsonResponseException) cause;
      }
      cause = cause.getCause();
    }
    return null;
  }
}
