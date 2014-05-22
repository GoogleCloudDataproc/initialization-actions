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

package com.google.cloud.hadoop.gcsio;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

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
    public BatchHelper newBatchHelper(HttpRequestInitializer requestInitializer, Storage gcs,
        long maxRequestsPerBatch) {
      return new BatchHelper(requestInitializer, gcs, maxRequestsPerBatch);
    }
  }

  // Created at construction-time, this request is reused for multiple sub-batches.
  private final BatchRequest batch;

  // Number of requests which can be queued into a single actual HTTP request before a sub-batch
  // is sent.
  private final long maxRequestsPerBatch;

  /**
   * Primary constructor, generally accessed only via the inner Factory class.
   */
  private BatchHelper(HttpRequestInitializer requestInitializer, Storage gcs,
      long maxRequestsPerBatch) {
    this.batch = gcs.batch(requestInitializer);
    this.maxRequestsPerBatch = maxRequestsPerBatch;
  }

  @VisibleForTesting
  protected BatchHelper() {
    this.batch = null;
    this.maxRequestsPerBatch = -1;
  }

  /**
   * Adds an additional request to the batch, and possibly flushes the current contents of the batch
   * if {@code maxRequestsPerBatch} has been reached.
   */
  public <T> void queue(StorageRequest<T> req, JsonBatchCallback<T> callback)
      throws IOException {
    req.queue(batch, callback);
    if (batch.size() >= maxRequestsPerBatch) {
      // BatchRequest.java indicates that execute() also resets the batch's internal state at
      // the end, so that the same BatchRequest object can be reused for further batching.
      flush();
    }
  }

  /**
   * Sends any currently remaining requests in the batch; should be caleld at the end of any
   * series of batched requests to ensure everything has been sent.
   */
  public void flush()
      throws IOException {
    if (batch.size() > 0) {
      batch.execute();
    }
  }
}
