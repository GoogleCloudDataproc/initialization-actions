/**
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

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.common.base.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implements application level operation retries.
 * @param <T> The StorageRequest class
 * @param <S> The result of the StorageRequest
 */
public class OperationWithRetry<T extends AbstractGoogleClientRequest<S>, S> {

  /**
   * Create a Predicate to determine whether a given exception is due to rate limiting.
   */
  public static Predicate<IOException> createRateLimitedExceptionPredicate(
      final ApiErrorExtractor errorExtractor) {
    return new Predicate<IOException>() {
      @Override
      public boolean apply(IOException e) {
        return errorExtractor.rateLimited(e);
      }
    };
  }

  // Logger.
  private static final Logger LOG = LoggerFactory.getLogger(OperationWithRetry.class);

  private final Sleeper sleeper;
  private final BackOff backOff;
  private final T request;
  private final Predicate<IOException> shouldRetryPredicate;

  public OperationWithRetry(Sleeper sleeper, BackOff backOff, T request,
      Predicate<IOException> shouldRetryPredicate) {
    this.sleeper = sleeper;
    this.backOff = backOff;
    this.request = request;
    this.shouldRetryPredicate = shouldRetryPredicate;
  }

  /**
   * Execute the operation until while we have not yet completed successfully and the
   * shouldRetryPredicate returns true and the backOff has not signalled completion.
   * @return The result of the operation
   * @throws IOException If we exhaust our backOff retries or the operation throws an
   * exception we should not retry. The exception will be as was thrown by the StorageRequest's
   * execute method.
   */
  public S execute() throws IOException {
    long nextRetryBackoff;
    IOException lastException;

    do {
      try {
        return request.execute();
      } catch (IOException ioe) {
        if (shouldRetryPredicate.apply(ioe)) {
          LOG.debug("Retrying after catching exception", ioe);
          lastException = ioe;
          nextRetryBackoff = backOff.nextBackOffMillis();
          try {
            sleeper.sleep(nextRetryBackoff);
          } catch (InterruptedException ie) {
            throw new IOException(ie);
          }
        } else {
          LOG.debug("Not retrying after catching exception", ioe);
          throw ioe;
        }
      }
    } while (nextRetryBackoff != BackOff.STOP);

    LOG.debug("Exhausted retries. lastException was: ", lastException);
    throw lastException;
  }
}
