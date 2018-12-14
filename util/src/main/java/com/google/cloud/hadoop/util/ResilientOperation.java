/*
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

import static com.google.api.client.util.Preconditions.checkNotNull;

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * A class which defines static functions to be called to make a user-provided function more
 * resilient by attempting retries.
 */
public class ResilientOperation {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Retries the given executable function in the case of transient errors defined by the
   * RetryDeterminer.
   *
   * <p>Does not support unchecked exceptions that are not instances of RuntimeException.
   *
   * @param callable CheckedCallable to retry execution of
   * @param backoff BackOff to determine how long to sleep for
   * @param retryDet RetryDeterminer to determine when to retry
   * @param classType class type of X
   * @param sleeper Used to sleep
   * @param <T> Type of object returned by the call.
   * @param <X> Type of exception thrown by the call.
   * @throws X What is thrown from the executable or the RetryDeterminer
   * @throws InterruptedException - Exception thrown from sleep
   */
  @SuppressWarnings("unchecked")
  public static <T, X extends Exception> T retry(
      CheckedCallable<T, X> callable,
      BackOff backoff,
      RetryDeterminer<? super X> retryDet,
      Class<X> classType,
      Sleeper sleeper)
      throws X, InterruptedException {
    checkNotNull(backoff, "Must provide a non-null BackOff.");
    checkNotNull(retryDet, "Must provide a non-null RetryDeterminer.");
    checkNotNull(sleeper, "Must provide a non-null Sleeper.");
    checkNotNull(callable, "Must provide a non-null Executable object.");

    X currentException;
    do {
      try {
        return callable.call();
      } catch (Exception e) {
        if (classType.isInstance(e)) { // e is something that extends X
          currentException = (X) e;
          if (!retryDet.shouldRetry(currentException)) {
            throw currentException;
          }
        } else {
          if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          }
          throw new RuntimeException(
              "Retrying with unchecked exceptions that are not RuntimeExceptions is not supported.",
              e);
        }
      }
    } while (nextSleep(backoff, sleeper, currentException));
    throw currentException;
  }

  /**
   * Retries the given executable function in the case of transient errors defined by the
   * RetryDeterminer and uses default sleeper.
   *
   * @param callable CheckedCallable to retry execution of
   * @param backoff BackOff to determine how long to sleep for
   * @param retryDet RetryDeterminer to determine when to retry
   * @param classType class type of X
   * @param <T> Type of object returned by the call.
   * @param <X> Type of exception thrown by the call.
   * @throws X What is thrown from the executable or the RetryDeterminer
   * @throws InterruptedException - Exception thrown from sleep
   */
  public static <T, X extends Exception> T retry(
      CheckedCallable<T, X> callable,
      BackOff backoff,
      RetryDeterminer<? super X> retryDet,
      Class<X> classType)
      throws X, InterruptedException {
    return retry(callable, backoff, retryDet, classType, Sleeper.DEFAULT);
  }

  /**
   * Determines the amount to sleep for and sleeps if needed.
   *
   * @param backoff BackOff to determine how long to sleep for
   * @param sleeper Used to sleep
   * @param currentException exception that caused the retry and sleep. For logging.
   * @throws InterruptedException if sleep is interrupted
   */
  private static boolean nextSleep(BackOff backoff, Sleeper sleeper, Exception currentException)
      throws InterruptedException {
    long backOffTime;
    try {
      backOffTime = backoff.nextBackOffMillis();
    } catch (IOException e) {
      throw new RuntimeException("Failed to to get next back off time", e);
    }
    if (backOffTime == BackOff.STOP) {
      return false;
    }
    logger.atInfo().withCause(currentException).log(
        "Transient exception caught. Sleeping for %d, then retrying.", backOffTime);
    sleeper.sleep(backOffTime);
    return true;
  }

  /**
   * Interface that allows a call that can throw an exception X.
   *
   * @param <T> Type of object returned by the call.
   * @param <X> Type of exception thrown by the call.
   */
  // TODO: Replace with Guava's CheckedCallable when not in beta.
  public interface CheckedCallable<T, X extends Exception> extends Callable<T> {
    @Override
    T call() throws X;
  }

  /**
   * Returns a {@link CheckedCallable} that encompasses a {@link AbstractGoogleClientRequest} and
   * can be used to retry the execution for an AbstractGoogleClientRequest.
   *
   * @param request The AbstractGoogleClientRequest to turn into a {@link CheckedCallable}.
   * @return a CheckedCallable object that attempts a AbstractGoogleClientRequest
   */
  public static <V> CheckedCallable<V, IOException> getGoogleRequestCallable(
      AbstractGoogleClientRequest<V> request) {
    return new AbstractGoogleClientRequestExecutor<>(request);
  }

  /**
   * Simple class to create a {@link CheckedCallable} from a {@link AbstractGoogleClientRequest}.
   */
  private static class AbstractGoogleClientRequestExecutor<T>
      implements CheckedCallable<T, IOException> {
    private final AbstractGoogleClientRequest<T> request;

    private AbstractGoogleClientRequestExecutor(AbstractGoogleClientRequest<T> request) {
      this.request = request;
    }

    @Override
    public T call() throws IOException {
      return request.execute();
    }
  }
}
