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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchHelper abstracts out the logic for maximum requests per batch, and also allows a workaround
 * for the fact that {@code BatchRequest} was made a "final class" for some reason, making it
 * impossible to unittest. Instead, batch interactions with a Storage API client will be funneled
 * through this class, while unittests can inject a mock batch helper.
 *
 * <p>This class is thread-safe, because if {@code numThreads} is greater than 0, request callbacks
 * will be executed on a different thread(s) than a client thread that queues requests.
 *
 * <p>Expected usage is to create a new BatchHelper instance per client operation (copy, rename,
 * delete, etc.) that represent logical grouping of requests.
 *
 * <p>Instance of this class can not be used again after {@link #flush()} method call.
 */
public class BatchHelper {

  private static final Logger LOG = LoggerFactory.getLogger(BatchHelper.class);

  private static final ThreadFactory THREAD_FACTORY =
      new ThreadFactoryBuilder().setNameFormat("gcsfs-batch-helper-%d").setDaemon(true).build();

  /**
   * Since each BatchHelper instance should be tied to a particular related set of requests, use
   * cases will generally interact via an injectable BatchHelper.Factory.
   */
  public static class Factory {
    public BatchHelper newBatchHelper(
        HttpRequestInitializer requestInitializer, Storage gcs, long maxRequestsPerBatch) {
      return new BatchHelper(requestInitializer, gcs, maxRequestsPerBatch, /* numThreads= */ 0);
    }

    BatchHelper newBatchHelper(
        HttpRequestInitializer requestInitializer,
        Storage gcs,
        long maxRequestsPerBatch,
        long totalRequests,
        int maxThreads) {
      checkArgument(maxRequestsPerBatch > 0, "maxRequestsPerBatch should be greater than 0");
      checkArgument(totalRequests > 0, "totalRequests should be greater than 0");
      checkArgument(maxThreads >= 0, "maxThreads should be greater or equal to 0");
      // Do not send batch request when performing operations on 1 object.
      if (totalRequests == 1) {
        return new BatchHelper(
            requestInitializer, gcs, /* maxRequestsPerBatch= */ 1, /* numThreads= */ 0);
      }
      if (maxThreads == 0) {
        return new BatchHelper(requestInitializer, gcs, maxRequestsPerBatch, maxThreads);
      }
      // If maxRequestsPerBatch is too high to fill up all parallel batches (maxThreads)
      // then reduce it to evenly distribute requests across the batches
      long requestsPerBatch = (long) Math.ceil((double) totalRequests / maxThreads);
      requestsPerBatch = Math.min(requestsPerBatch, maxRequestsPerBatch);
      // If maxThreads is too high to execute all requests (totalRequests)
      // in batches (requestsPerBatch) then reduce it to minimum required number of threads
      int numThreads = Math.toIntExact((long) Math.ceil((double) totalRequests / requestsPerBatch));
      numThreads = Math.min(numThreads, maxThreads);
      return new BatchHelper(requestInitializer, gcs, requestsPerBatch, numThreads);
    }
  }

  /** Callback that causes a single StorageRequest to be added to the {@link BatchRequest}. */
  protected static interface QueueRequestCallback {
    void enqueue(BatchRequest batch) throws IOException;
  }

  private final Queue<QueueRequestCallback> pendingRequests = new ConcurrentLinkedQueue<>();
  private final ExecutorService requestsExecutor;
  private final Queue<Future<Void>> responseFutures = new ConcurrentLinkedQueue<>();

  private final HttpRequestInitializer requestInitializer;
  private final Storage gcs;
  // Number of requests that can be queued into a single HTTP batch request.
  private final long maxRequestsPerBatch;

  private final Lock flushLock = new ReentrantLock();

  /**
   * Primary constructor, generally accessed only via the inner Factory class.
   *
   * @param numThreads Number of threads to execute HTTP batch requests in parallel.
   */
  private BatchHelper(
      HttpRequestInitializer requestInitializer,
      Storage gcs,
      long maxRequestsPerBatch,
      int numThreads) {
    this.requestInitializer = requestInitializer;
    this.gcs = gcs;
    this.requestsExecutor =
        numThreads == 0 ? newDirectExecutorService() : newRequestsExecutor(numThreads);
    this.maxRequestsPerBatch = maxRequestsPerBatch;
  }

  private static ExecutorService newRequestsExecutor(int numThreads) {
    ThreadPoolExecutor requestsExecutor =
        new ThreadPoolExecutor(
            /* corePoolSize= */ numThreads,
            /* maximumPoolSize= */ numThreads,
            /* keepAliveTime= */ 5, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(numThreads * 20),
            THREAD_FACTORY);
    // Prevents memory leaks in case flush() method was not called.
    requestsExecutor.allowCoreThreadTimeOut(true);
    requestsExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    return requestsExecutor;
  }

  /**
   * Adds an additional request to the batch, and possibly flushes the current contents of the batch
   * if {@code maxRequestsPerBatch} has been reached.
   */
  public <T> void queue(StorageRequest<T> req, JsonBatchCallback<T> callback) throws IOException {
    checkState(
        !requestsExecutor.isShutdown() && !requestsExecutor.isTerminated(),
        "requestsExecutor should not be terminated to queue batch requests");
    if (maxRequestsPerBatch == 1) {
      responseFutures.add(
          requestsExecutor.submit(
              () -> {
                execute(req, callback);
                return null;
              }));
    } else {
      pendingRequests.add(batch -> req.queue(batch, callback));

      flushIfPossibleAndRequired();
    }
  }

  public <T> void execute(StorageRequest<T> req, JsonBatchCallback<T> callback) throws IOException {
    try {
      T result = req.execute();
      callback.onSuccess(result, req.getLastResponseHeaders());
    } catch (IOException e) {
      GoogleJsonResponseException je = ApiErrorExtractor.getJsonResponseExceptionOrNull(e);
      if (je == null) {
        throw e;
      }
      callback.onFailure(je.getDetails(), je.getHeaders());
    }
  }

  // Flush our buffer if we have at least maxRequestsPerBatch pending entries
  private void flushIfPossibleAndRequired() throws IOException {
    if (pendingRequests.size() >= maxRequestsPerBatch) {
      flushIfPossible(false);
    }
  }

  // Flush our buffer if we are not already in a flush operation and we have data to flush.
  private void flushIfPossible(boolean flushAll) throws IOException {
    if (flushAll) {
      flushLock.lock();
    } else if (pendingRequests.isEmpty() || !flushLock.tryLock()) {
      return;
    }
    try {
      do {
        flushPendingRequests();
        if (flushAll) {
          awaitRequestsCompletion();
        }
      } while (flushAll && (!pendingRequests.isEmpty() || !responseFutures.isEmpty()));
    } finally {
      flushLock.unlock();
    }
  }

  private void flushPendingRequests() throws IOException {
    if (pendingRequests.isEmpty()) {
      return;
    }
    BatchRequest batch = gcs.batch(requestInitializer);
    while (batch.size() < maxRequestsPerBatch && !pendingRequests.isEmpty()) {
      // enqueue request at head
      pendingRequests.remove().enqueue(batch);
    }
    responseFutures.add(
        requestsExecutor.submit(
            () -> {
              batch.execute();
              return null;
            }));
  }

  /**
   * Sends any currently remaining requests in the batch; should be called at the end of any series
   * of batched requests to ensure everything has been sent.
   */
  public void flush() throws IOException {
    try {
      flushIfPossible(true);
      checkState(pendingRequests.isEmpty(), "pendingRequests should be empty after flush");
      checkState(responseFutures.isEmpty(), "responseFutures should be empty after flush");
    } finally {
      requestsExecutor.shutdown();
      try {
        if (!requestsExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
          LOG.warn("Forcibly shutting down batch helper thread pool.");
          requestsExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.debug(
            "Failed to await termination: forcibly shutting down batch helper thread pool.", e);
        requestsExecutor.shutdownNow();
      }
    }
  }

  /** Returns true if there are no currently queued entries in the batch helper. */
  public boolean isEmpty() {
    return pendingRequests.isEmpty();
  }

  /** Awaits until all sent requests are completed. Should be serialized */
  private void awaitRequestsCompletion() throws IOException {
    // Don't wait until all requests will be completed if enough requests are pending for full batch
    while (!responseFutures.isEmpty() && pendingRequests.size() < maxRequestsPerBatch) {
      try {
        responseFutures.remove().get();
      } catch (InterruptedException | ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new RuntimeException("Failed to execute batch", e);
      }
    }
  }
}
