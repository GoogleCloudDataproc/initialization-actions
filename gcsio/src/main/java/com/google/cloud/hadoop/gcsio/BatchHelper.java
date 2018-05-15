/*
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

package com.google.cloud.hadoop.gcsio;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * BatchHelper abstracts out the logic for maximum requests per batch, and also allows a workaround
 * for the fact that {@code BatchRequest} was made a "final class" for some reason, making it
 * impossible to unittest. Instead, batch interactions with a Storage API client will be funneled
 * through this class, while unittests can inject a mock batch helper.
 * <p>
 * This class is not thread-safe; expected usage is to create a new BatchHelper instance per
 * single-threaded logical grouping of requests.
 */
public class BatchHelper {
  /**
   * Since each BatchHelper instance should be tied to a particular related set of requests,
   * use cases will generally interact via an injectable BatchHelper.Factory.
   */
  public static class Factory {
    public BatchHelper newBatchHelper(
        HttpRequestInitializer requestInitializer, Storage gcs, long maxRequestsPerBatch) {
      return new BatchHelper(requestInitializer, gcs, maxRequestsPerBatch);
    }
  }

  /**
   * Callback that causes a single StorageRequest to be added to the BatchRequest.
   */
  protected static interface QueueRequestCallback {
    void enqueue() throws IOException;
  }

  private final List<QueueRequestCallback> pendingBatchEntries;
  private final BatchRequest batch;
  // Number of requests that can be queued into a single actual HTTP request
  // before a sub-batch is sent.
  private final long maxRequestsPerBatch;
  // Flag that indicates whether there is an in-progress flush.
  private boolean flushing = false;

  /**
   * Primary constructor, generally accessed only via the inner Factory class.
   */
  private BatchHelper(HttpRequestInitializer requestInitializer, Storage gcs,
      long maxRequestsPerBatch) {
    this.pendingBatchEntries = new LinkedList<>();
    this.batch = gcs.batch(requestInitializer);
    this.maxRequestsPerBatch = maxRequestsPerBatch;
  }

  @VisibleForTesting
  protected BatchHelper() {
    this.pendingBatchEntries = new LinkedList<>();
    this.batch = null;
    this.maxRequestsPerBatch = -1;
  }

  /**
   * Adds an additional request to the batch, and possibly flushes the current contents of the batch
   * if {@code maxRequestsPerBatch} has been reached.
   */
  public <T> void queue(final StorageRequest<T> req, final JsonBatchCallback<T> callback)
      throws IOException {
      QueueRequestCallback queueCallback = new QueueRequestCallback() {
        @Override
        public void enqueue() throws IOException {
          req.queue(batch, callback);
        }
      };
    pendingBatchEntries.add(queueCallback);

    flushIfPossibleAndRequired();
  }

  // Flush our buffer if we have at least maxRequestsPerBatch pending entries
  private void flushIfPossibleAndRequired() throws IOException {
    if (pendingBatchEntries.size() >= maxRequestsPerBatch) {
      flushIfPossible(false);
    }
  }

  // Flush our buffer if we are not already in a flush operation and we have data to flush.
  private void flushIfPossible(boolean flushAll) throws IOException {
    if (!flushing && pendingBatchEntries.size() > 0) {
      flushing = true;
      try {
        do {
          while (batch.size() < maxRequestsPerBatch && !pendingBatchEntries.isEmpty()) {
            QueueRequestCallback head = pendingBatchEntries.remove(0);
            head.enqueue();
          }

          batch.execute();
        } while (flushAll && !pendingBatchEntries.isEmpty());
      } finally {
        flushing = false;
      }
    }
  }

  /**
   * Sends any currently remaining requests in the batch; should be caleld at the end of any series
   * of batched requests to ensure everything has been sent.
   */
  public void flush() throws IOException {
    flushIfPossible(true);
  }

  /**
   * Returns true if there are no currently queued entries in the batch helper.
   */
  public boolean isEmpty() {
    return pendingBatchEntries.isEmpty();
  }
}
