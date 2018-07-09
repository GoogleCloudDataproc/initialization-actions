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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newConcurrentHashSet;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Data;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Buckets;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.RewriteResponse;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.RequesterPaysOptions;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides read/write access to Google Cloud Storage (GCS), using Java nio channel semantics. This
 * is a basic implementation of the GoogleCloudStorage interface that mostly delegates through to
 * the appropriate API call(s) via the generated JSON API client, while adding reliability and
 * performance features such as setting up low-level retries, translating low-level exceptions, and
 * request batching.
 */
public class GoogleCloudStorageImpl implements GoogleCloudStorage {

  // Number of retries to make when waiting for a bucket to be empty.
  public static final int BUCKET_EMPTY_MAX_RETRIES = 20;

  // Duration of wait (in milliseconds) per retry for a bucket to be empty.
  public static final int BUCKET_EMPTY_WAIT_TIME_MS = 500;

  // JSON factory used for formatting GCS JSON API payloads.
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  // Logger.
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorage.class);

  // Maximum number of times to retry deletes in the case of precondition failures.
  private static final int MAXIMUM_PRECONDITION_FAILURES_IN_DELETE = 4;

  private static final String USER_PROJECT_FIELD_NAME = "userProject";

  // A function to encode metadata map values
  private static final Function<byte[], String> ENCODE_METADATA_VALUES =
      new Function<byte[], String>() {
        @Override
        public String apply(byte[] bytes) {
          if (bytes == null) {
            return Data.NULL_STRING;
          } else {
            return BaseEncoding.base64().encode(bytes);
          }
        }
      };

  private static final Function<String, byte[]> DECODE_METADATA_VALUES =
      new Function<String, byte[]>() {
        @Override
        public byte[] apply(String value) {
          try {
            return BaseEncoding.base64().decode(value);
          } catch (IllegalArgumentException iae) {
            LOG.error("Failed to parse base64 encoded attribute value {} - {}", value, iae);
            return null;
          }
        }
      };

  /**
   * A factory for producing BackOff objects.
   */
  public static interface BackOffFactory {
    public static final BackOffFactory DEFAULT = new BackOffFactory() {
      @Override
      public BackOff newBackOff() {
        return new ExponentialBackOff();
      }
    };

    BackOff newBackOff();
  }

  private final LoadingCache<String, Boolean> autoBuckets =
      CacheBuilder.newBuilder()
          .expireAfterWrite(1, TimeUnit.HOURS)
          .build(
              new CacheLoader<String, Boolean>() {
                final List<String> iamPermissions = ImmutableList.of("storage.buckets.get");
                @Override
                public Boolean load(String bucketName) throws Exception {
                  try {
                    gcs.buckets()
                        .testIamPermissions(bucketName, iamPermissions)
                        .executeUnparsed()
                        .disconnect();
                  } catch (IOException e) {
                    return errorExtractor.userProjectMissing(e);
                  }
                  return false;
                }
              });

  // GCS access instance.
  private Storage gcs;

  // Thread-pool used for background tasks.
  private ExecutorService threadPool = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder()
          .setNameFormat("gcs-async-channel-pool-%d")
          .setDaemon(true)
          .build());

  // Thread-pool for manual matching of metadata tasks.
  // TODO(user): Wire out GoogleCloudStorageOptions for these.
  private ExecutorService manualBatchingThreadPool = createManualBatchingThreadPool();

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private ApiErrorExtractor errorExtractor = ApiErrorExtractor.INSTANCE;

  // Helper for interacting with objects invovled with the API client libraries.
  private ClientRequestHelper<StorageObject> clientRequestHelper =
      new ClientRequestHelper<>();

  // Factory for BatchHelpers setting up BatchRequests; can be swapped out for testing purposes.
  private BatchHelper.Factory batchFactory = new BatchHelper.Factory();

  // Request initializer to use for batch and non-batch requests.
  private HttpRequestInitializer httpRequestInitializer;

  // Configuration values for this instance
  private final GoogleCloudStorageOptions storageOptions;

  // Object to use to perform sleep operations
  private Sleeper sleeper = Sleeper.DEFAULT;

  // BackOff objects are per-request, use this to make new ones.
  private BackOffFactory backOffFactory = BackOffFactory.DEFAULT;

  // Determine if a given IOException is due to rate-limiting.
  private RetryDeterminer<IOException> rateLimitedRetryDeterminer =
      RetryDeterminer.createRateLimitedRetryDeterminer(errorExtractor);

  /**
   * Constructs an instance of GoogleCloudStorageImpl.
   *
   * @param credential OAuth2 credential that allows access to GCS
   * @throws IOException on IO error
   */
  public GoogleCloudStorageImpl(GoogleCloudStorageOptions options, Credential credential)
      throws IOException {
    Preconditions.checkNotNull(options, "options must not be null");

    LOG.debug("GCS({})", options.getAppName());

    options.throwIfNotValid();

    this.storageOptions = options;

    Preconditions.checkNotNull(credential, "credential must not be null");

    this.httpRequestInitializer =
        new RetryHttpInitializer(
            credential,
            options.getAppName(),
            options.getMaxHttpRequestRetries(),
            options.getHttpRequestConnectTimeout(),
            options.getHttpRequestReadTimeout());

    HttpTransport httpTransport = HttpTransportFactory.createHttpTransport(
        options.getTransportType(), options.getProxyAddress());

    // Create GCS instance.
    this.gcs = new Storage.Builder(
        httpTransport, JSON_FACTORY, httpRequestInitializer)
        .setApplicationName(options.getAppName())
        .build();
  }

  /**
   * Constructs an instance of GoogleCloudStorageImpl.
   *
   * @param gcs Preconstructed Storage to use for I/O.
   */
  public GoogleCloudStorageImpl(GoogleCloudStorageOptions options, Storage gcs) {
    Preconditions.checkNotNull(options, "options must not be null");

    LOG.debug("GCS({})", options.getAppName());

    options.throwIfNotValid();

    this.storageOptions = options;

    Preconditions.checkNotNull(gcs, "gcs must not be null");

    this.gcs = gcs;

    if (gcs.getRequestFactory() != null) {
      this.httpRequestInitializer = gcs.getRequestFactory().getInitializer();
    }
  }

  @VisibleForTesting
  protected GoogleCloudStorageImpl() {
    this.storageOptions = GoogleCloudStorageOptions.newBuilder().build();
  }

  private ExecutorService createManualBatchingThreadPool() {
    ThreadPoolExecutor service =
        new ThreadPoolExecutor(
            /* corePoolSize= */ 10,
            /* maximumPoolSize= */ 20,
            /* keepAliveTime= */ 10L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-manual-batching-pool-%d")
                .setDaemon(true)
                .build());
    // allowCoreThreadTimeOut needs to be enabled for cases where the encapsulating class does not
    service.allowCoreThreadTimeOut(true);
    return service;
  }

  @VisibleForTesting
  void setThreadPool(ExecutorService threadPool) {
    this.threadPool = threadPool;
  }

  @VisibleForTesting
  void setManualBatchingThreadPool(ExecutorService manualBatchingThreadPool) {
    this.manualBatchingThreadPool = manualBatchingThreadPool;
  }

  @VisibleForTesting
  void setErrorExtractor(ApiErrorExtractor errorExtractor) {
    this.errorExtractor = errorExtractor;
    this.rateLimitedRetryDeterminer = RetryDeterminer.createRateLimitedRetryDeterminer(
        errorExtractor);
  }

  @VisibleForTesting
  void setClientRequestHelper(
      ClientRequestHelper<StorageObject> clientRequestHelper) {
    this.clientRequestHelper = clientRequestHelper;
  }

  @VisibleForTesting
  void setBatchFactory(BatchHelper.Factory batchFactory) {
    this.batchFactory = batchFactory;
  }

  @VisibleForTesting
  void setSleeper(Sleeper sleeper) {
    this.sleeper = sleeper;
  }

  @VisibleForTesting
  void setBackOffFactory(BackOffFactory factory) {
    backOffFactory = factory;
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return storageOptions;
  }

  @Override
  public WritableByteChannel create(final StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    LOG.debug("create({})", resourceId);
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    /*
     * When performing mutations in GCS, even when we aren't concerned with parallel writers,
     * we need to protect ourselves from what appear to be out-of-order writes to the writer. These
     * most commonly manifest themselves as a sequence of:
     * 1) Perform mutation M1 on object O1, which results in an HTTP 503 error,
     *    but can be any 5XX class error.
     * 2) Retry mutation M1, which yields a 200 OK
     * 3) Perform mutation M2 on O1, which yields a 200 OK
     * 4) Some time later, get O1 and see M1 and not M2, even though M2 appears to have happened
     *    later.
     *
     * To counter this we need to perform mutations with a condition attached, always.
     *
     * To perform a mutation with a condition, we first must get the content generation of the
     * current object. Once we have the current generation, we will create a marker file
     * conditionally with an ifGenerationMatch. We will then create the final object only if the
     * generation matches the marker file.
     */

    // TODO(user): Have createEmptyObject return enough information to use that instead.
    Optional<Long> overwriteGeneration = Optional.absent();
    long backOffSleep = 0L;

    if (storageOptions.isMarkerFileCreationEnabled()) {
      BackOff backOff = backOffFactory.newBackOff();
      do {
        if (backOffSleep != 0) {
          try {
            sleeper.sleep(backOffSleep);
          } catch (InterruptedException ie) {
            throw new IOException(String.format(
                "Interrupted while sleeping for backoff in create of %s", resourceId));
          }
        }

        backOffSleep = backOff.nextBackOffMillis();

        Storage.Objects.Insert insertObject = prepareEmptyInsert(resourceId, options);
        // If resourceId.hasHasGenerationId(), we'll expect the underlying prepareEmptyInsert
        // to already set the setIfGenerationMatch; otherwise we must explicitly fetch the
        // current generationId here.
        if (!resourceId.hasGenerationId()) {
          insertObject.setIfGenerationMatch(
              getWriteGeneration(resourceId, options.overwriteExisting()));
        }

        try {
          StorageObject result = insertObject.execute();
          overwriteGeneration = Optional.of(result.getGeneration());
        } catch (IOException ioe) {
          if (errorExtractor.preconditionNotMet(ioe)) {
            LOG.info(
                "Retrying marker file creation. Retrying according to backoff policy, %s - %s",
                resourceId,
                ioe);
          } else {
            throw ioe;
          }
        }
      } while (!overwriteGeneration.isPresent() && backOffSleep != BackOff.STOP);

      if (backOffSleep == BackOff.STOP) {
        throw new IOException(
            String.format(
                "Retries exhausted while attempting to create marker file for %s", resourceId));
      }
    } else {
      // Do not use a marker-file
      if (resourceId.hasGenerationId()) {
        overwriteGeneration = Optional.of(resourceId.getGenerationId());
      } else {
        overwriteGeneration =
            Optional.of(getWriteGeneration(resourceId, options.overwriteExisting()));
      }
    }

    ObjectWriteConditions writeConditions =
        new ObjectWriteConditions(overwriteGeneration, Optional.<Long>absent());

    Map<String, String> rewrittenMetadata = encodeMetadata(options.getMetadata());

    GoogleCloudStorageWriteChannel channel =
        new GoogleCloudStorageWriteChannel(
            threadPool,
            gcs,
            clientRequestHelper,
            resourceId.getBucketName(),
            resourceId.getObjectName(),
            storageOptions.getWriteChannelOptions(),
            writeConditions,
            rewrittenMetadata,
            options.getContentType()) {

          @Override
          public Storage.Objects.Insert createRequest(InputStreamContent inputStream)
              throws IOException {
            return configureRequest(super.createRequest(inputStream), resourceId.getBucketName());
          }
        };

    channel.initialize();

    return channel;
  }

  /**
   * See {@link GoogleCloudStorage#create(StorageResourceId)} for details about expected behavior.
   */
  @Override
  public WritableByteChannel create(StorageResourceId resourceId) throws IOException {
    LOG.debug("create({})", resourceId);
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    return create(resourceId, CreateObjectOptions.DEFAULT);
  }

  /** See {@link GoogleCloudStorage#create(String)} for details about expected behavior. */
  @Override
  public void create(String bucketName) throws IOException {
    create(bucketName, CreateBucketOptions.DEFAULT);
  }

  /**
   * See {@link GoogleCloudStorage#create(String, CreateBucketOptions)} for details about expected
   * behavior.
   */
  @Override
  public void create(String bucketName, CreateBucketOptions options) throws IOException {
    LOG.debug("create({})", bucketName);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    checkNotNull(options, "options must not be null");
    checkNotNull(storageOptions.getProjectId(), "projectId must not be null");

    Bucket bucket = new Bucket();
    bucket.setName(bucketName);
    bucket.setLocation(options.getLocation());
    bucket.setStorageClass(options.getStorageClass());
    Storage.Buckets.Insert insertBucket =
        configureRequest(gcs.buckets().insert(storageOptions.getProjectId(), bucket), bucketName);
    // TODO(user): To match the behavior of throwing FileNotFoundException for 404, we probably
    // want to throw org.apache.commons.io.FileExistsException for 409 here.
    try {
      ResilientOperation.retry(
          ResilientOperation.getGoogleRequestCallable(insertBucket),
          backOffFactory.newBackOff(),
          rateLimitedRetryDeterminer,
          IOException.class,
          sleeper);
    } catch (InterruptedException e) {
      throw new IOException(e); // From sleep
    }
  }

  @Override
  public void createEmptyObject(StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    Storage.Objects.Insert insertObject = prepareEmptyInsert(resourceId, options);
    try {
      insertObject.execute();
    } catch (IOException ioe) {
      if (canIgnoreExceptionForEmptyObject(ioe, resourceId, options)) {
        LOG.info("Ignoring exception; verified object already exists with desired state.", ioe);
      } else {
        throw ioe;
      }
    }
  }

  /**
   * See {@link GoogleCloudStorage#createEmptyObject(StorageResourceId)} for details about
   * expected behavior.
   */
  @Override
  public void createEmptyObject(StorageResourceId resourceId)
      throws IOException {
    LOG.debug("createEmptyObject({})", resourceId);
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    createEmptyObject(resourceId, CreateObjectOptions.DEFAULT);
  }

  @Override
  public void createEmptyObjects(
      List<StorageResourceId> resourceIds, final CreateObjectOptions options)
      throws IOException {
    // TODO(user): This method largely follows a pattern similar to
    // deleteObjects(List<StorageResourceId>); extract a generic method for both.
    LOG.debug("createEmptyObjects({})", resourceIds);

    if (resourceIds.isEmpty()) {
      return;
    }

    // Don't go through batch interface for a single-item case to avoid batching overhead.
    if (resourceIds.size() == 1) {
      createEmptyObject(Iterables.getOnlyElement(resourceIds), options);
      return;
    }

    // Validate that all the elements represent StorageObjects.
    for (StorageResourceId resourceId : resourceIds) {
      Preconditions.checkArgument(resourceId.isStorageObject(),
          "Expected full StorageObject names only, got: '%s'", resourceId);
    }

    // Gather exceptions to wrap in a composite exception at the end.
    final Set<IOException> innerExceptions = newConcurrentHashSet();
    final CountDownLatch latch = new CountDownLatch(resourceIds.size());
    for (final StorageResourceId resourceId : resourceIds) {
      final Storage.Objects.Insert insertObject = prepareEmptyInsert(resourceId, options);
      manualBatchingThreadPool.execute(
          new Runnable() {
            @Override
            public void run() {
              try {
                insertObject.execute();
                LOG.debug("Successfully inserted {}", resourceId);
              } catch (IOException ioe) {
                boolean canIgnoreException = false;
                try {
                  canIgnoreException = canIgnoreExceptionForEmptyObject(ioe, resourceId, options);
                } catch (Throwable t) {
                  // Make sure to catch Throwable instead of only IOException so that we can
                  // correctly wrap other such throwables and propagate them out cleanly inside
                  // innerExceptions; common sources of non-IOExceptions include Preconditions
                  // checks which get enforced at varous layers in the library stack.
                  IOException toWrap =
                      (t instanceof IOException ? (IOException) t : new IOException(t));
                  innerExceptions.add(
                      wrapException(
                          toWrap,
                          "Error re-fetching after rate-limit error.",
                          resourceId.getBucketName(),
                          resourceId.getObjectName()));
                }
                if (canIgnoreException) {
                  LOG.info(
                      "Ignoring exception; verified object already exists with desired state.",
                      ioe);
                } else {
                  innerExceptions.add(
                      wrapException(
                          ioe,
                          "Error inserting.",
                          resourceId.getBucketName(),
                          resourceId.getObjectName()));
                }
              } catch (Throwable t) {
                innerExceptions.add(
                    wrapException(
                        new IOException(t),
                        "Error inserting.",
                        resourceId.getBucketName(),
                        resourceId.getObjectName()));
              } finally {
                latch.countDown();
              }
            }
          });
    }

    try {
      latch.await();
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  /**
   * See {@link GoogleCloudStorage#createEmptyObjects(List)} for details about
   * expected behavior.
   */
  @Override
  public void createEmptyObjects(List<StorageResourceId> resourceIds) throws IOException {
    createEmptyObjects(resourceIds, CreateObjectOptions.DEFAULT);
  }

  /**
   * See {@link GoogleCloudStorage#open(StorageResourceId)} for details about expected behavior.
   */
  @Override
  public SeekableByteChannel open(StorageResourceId resourceId)
      throws IOException {
    return open(resourceId, GoogleCloudStorageReadOptions.DEFAULT);
  }

  /** See {@link GoogleCloudStorage#open(StorageResourceId)} for details about expected behavior. */
  @Override
  public SeekableByteChannel open(
      final StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    LOG.debug("open({}, {})", resourceId, readOptions);
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    return new GoogleCloudStorageReadChannel(
        gcs,
        resourceId.getBucketName(),
        resourceId.getObjectName(),
        errorExtractor,
        clientRequestHelper,
        readOptions) {

      @Override
      protected Storage.Objects.Get createRequest() throws IOException {
        return configureRequest(super.createRequest(), resourceId.getBucketName());
      }
    };
  }

  /**
   * See {@link GoogleCloudStorage#deleteBuckets(List<String>)} for details about expected behavior.
   */
  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    LOG.debug("deleteBuckets({})", bucketNames);

    // Validate all the inputs first.
    for (String bucketName : bucketNames) {
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    }

    // Gather exceptions to wrap in a composite exception at the end.
    final List<IOException> innerExceptions = new ArrayList<>();

    for (final String bucketName : bucketNames) {
      final Storage.Buckets.Delete deleteBucket =
          configureRequest(gcs.buckets().delete(bucketName), bucketName);

      try {
        ResilientOperation.retry(
            ResilientOperation.getGoogleRequestCallable(deleteBucket),
            backOffFactory.newBackOff(),
            rateLimitedRetryDeterminer,
            IOException.class,
            sleeper);
      } catch (IOException ioe) {
        if (errorExtractor.itemNotFound(ioe)) {
          LOG.debug("delete({}) : not found", bucketName);
          innerExceptions.add(
              GoogleCloudStorageExceptions.getFileNotFoundException(bucketName, null));
        } else {
          innerExceptions.add(
              wrapException(new IOException(ioe.toString()), "Error deleting", bucketName, null));
        }
      } catch (InterruptedException e) {
        throw new IOException(e);  // From sleep
      }
    }
    if (innerExceptions.size() > 0) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  /**
   * See {@link GoogleCloudStorage#deleteObjects(List<StorageResourceId>)} for details about
   * expected behavior.
   */
  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    LOG.debug("deleteObjects({})", fullObjectNames);

    // Validate that all the elements represent StorageObjects.
    for (StorageResourceId fullObjectName : fullObjectNames) {
      Preconditions.checkArgument(
          fullObjectName.isStorageObject(),
          "Expected full StorageObject names only, got: %s",
          fullObjectName);
    }

    // Gather exceptions to wrap in a composite exception at the end.
    final KeySetView<IOException, Boolean> innerExceptions = ConcurrentHashMap.newKeySet();
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(
            httpRequestInitializer, gcs, storageOptions.getMaxRequestsPerBatch());

    for (StorageResourceId fullObjectName : fullObjectNames) {
      queueSingleObjectDelete(fullObjectName, innerExceptions, batchHelper, 1);
    }

    batchHelper.flush();

    if (innerExceptions.size() > 0) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  /** Helper to create a callback for a particular deletion request. */
  private JsonBatchCallback<Void> getDeletionCallback(
      final StorageResourceId fullObjectName,
      final KeySetView<IOException, Boolean> innerExceptions,
      final BatchHelper batchHelper,
      final int attempt,
      final long generation) {
    final String bucketName = fullObjectName.getBucketName();
    final String objectName = fullObjectName.getObjectName();
    return new JsonBatchCallback<Void>() {
      @Override
      public void onSuccess(Void obj, HttpHeaders responseHeaders) {
        LOG.debug("Successfully deleted {} at generation {}", fullObjectName, generation);
      }

      @Override
      public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
        if (errorExtractor.itemNotFound(e)) {
          // Ignore item-not-found errors. We do not have to delete what we cannot find. This
          // error typically shows up when we make a request to delete something and the server
          // receives the request but we get a retry-able error before we get a response.
          // During a retry, we no longer find the item because the server had deleted
          // it already.
          LOG.debug("deleteObjects({}) : delete not found", fullObjectName);
        } else if (errorExtractor.preconditionNotMet(e)
            && attempt <= MAXIMUM_PRECONDITION_FAILURES_IN_DELETE) {
          LOG.info(
              "Precondition not met while deleting {} at generation {}. Attempt {}. Retrying.",
              fullObjectName, generation, attempt);
          queueSingleObjectDelete(fullObjectName, innerExceptions, batchHelper, attempt + 1);
        } else {
          innerExceptions.add(
              wrapException(
                  new IOException(e.toString()),
                  "Error deleting, stage 2 with generation " + generation, bucketName, objectName));
        }
      }
    };
  }

  private void queueSingleObjectDelete(
      final StorageResourceId fullObjectName,
      final KeySetView<IOException, Boolean> innerExceptions,
      final BatchHelper batchHelper,
      final int attempt) throws IOException {

    final String bucketName = fullObjectName.getBucketName();
    final String objectName = fullObjectName.getObjectName();

    if (fullObjectName.hasGenerationId()) {
      // We can go direct to the deletion request instead of first fetching generation id.
      long generationId = fullObjectName.getGenerationId();
      Storage.Objects.Delete deleteObject =
          configureRequest(gcs.objects().delete(bucketName, objectName), bucketName)
              .setIfGenerationMatch(generationId);
      batchHelper.queue(
          deleteObject,
          getDeletionCallback(fullObjectName, innerExceptions, batchHelper, attempt, generationId));
    } else {
      // We first need to get the current object version to issue a safe delete for only the
      // latest version of the object.
      Storage.Objects.Get getObject =
          configureRequest(gcs.objects().get(bucketName, objectName), bucketName);
      batchHelper.queue(
          getObject,
          new JsonBatchCallback<StorageObject>() {
            @Override
            public void onSuccess(StorageObject storageObject, HttpHeaders httpHeaders)
                throws IOException {
              final Long generation = storageObject.getGeneration();
              Storage.Objects.Delete deleteObject =
                  configureRequest(gcs.objects().delete(bucketName, objectName), bucketName)
                      .setIfGenerationMatch(generation);

              batchHelper.queue(
                  deleteObject,
                  getDeletionCallback(
                      fullObjectName, innerExceptions, batchHelper, attempt, generation));
            }

            @Override
            public void onFailure(GoogleJsonError googleJsonError, HttpHeaders httpHeaders) {
              if (errorExtractor.itemNotFound(googleJsonError)) {
                // If the item isn't found, treat it the same as if it's not found in the delete
                // case: assume the user wanted the object gone and now it is.
                LOG.debug("deleteObjects({}) : get not found", fullObjectName);
              } else {
                IOException ioException = new IOException(googleJsonError.toString());
                innerExceptions.add(
                    wrapException(ioException, "Error deleting, stage 1", bucketName, objectName));
              }
            }
          });
    }
  }

  /**
   * Validates basic argument constraints like non-null, non-empty Strings, using {@code
   * Preconditions} in addition to checking for src/dst bucket existence and compatibility of bucket
   * properties such as location and storage-class.
   *
   * @param gcsImpl A GoogleCloudStorage for retrieving bucket info via getItemInfo, but only if
   *     srcBucketName != dstBucketName; passed as a parameter so that this static method can be
   *     used by other implementations of GoogleCloudStorage that want to preserve the validation
   *     behavior of GoogleCloudStorageImpl, including disallowing cross-location copies.
   */
  @VisibleForTesting
  public static void validateCopyArguments(
      String srcBucketName, List<String> srcObjectNames,
      String dstBucketName, List<String> dstObjectNames,
      GoogleCloudStorage gcsImpl) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(srcBucketName),
        "srcBucketName must not be null or empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dstBucketName),
        "dstBucketName must not be null or empty");
    Preconditions.checkArgument(srcObjectNames != null,
        "srcObjectNames must not be null");
    Preconditions.checkArgument(dstObjectNames != null,
        "dstObjectNames must not be null");
    Preconditions.checkArgument(srcObjectNames.size() == dstObjectNames.size(),
        "Must supply same number of elements in srcObjectNames and dstObjectNames");

    // Avoid copy across locations or storage classes.
    if (!srcBucketName.equals(dstBucketName)) {
      GoogleCloudStorageItemInfo srcBucketInfo =
          gcsImpl.getItemInfo(new StorageResourceId(srcBucketName));
      if (!srcBucketInfo.exists()) {
        throw new FileNotFoundException("Bucket not found: " + srcBucketName);
      }

      GoogleCloudStorageItemInfo dstBucketInfo =
          gcsImpl.getItemInfo(new StorageResourceId(dstBucketName));
      if (!dstBucketInfo.exists()) {
        throw new FileNotFoundException("Bucket not found: " + dstBucketName);
      }

      if (!gcsImpl.getOptions().isCopyWithRewriteEnabled()) {
        if (!srcBucketInfo.getLocation().equals(dstBucketInfo.getLocation())) {
          throw new UnsupportedOperationException(
              "This operation is not supported across two different storage locations.");
        }

        if (!srcBucketInfo.getStorageClass().equals(dstBucketInfo.getStorageClass())) {
          throw new UnsupportedOperationException(
              "This operation is not supported across two different storage classes.");
        }
      }
    }
    for (int i = 0; i < srcObjectNames.size(); i++) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(srcObjectNames.get(i)),
          "srcObjectName must not be null or empty");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(dstObjectNames.get(i)),
          "dstObjectName must not be null or empty");
      if (srcBucketName.equals(dstBucketName)
          && srcObjectNames.get(i).equals(dstObjectNames.get(i))) {
        throw new IllegalArgumentException(String.format(
            "Copy destination must be different from source for %s.",
            StorageResourceId.createReadableString(srcBucketName, srcObjectNames.get(i))));
      }
    }
  }

  /**
   * See {@link GoogleCloudStorage#copy(String, List, String, List)} for details about expected
   * behavior.
   */
  @Override
  public void copy(
      String srcBucketName, List<String> srcObjectNames,
      String dstBucketName, List<String> dstObjectNames)
      throws IOException {
    validateCopyArguments(srcBucketName, srcObjectNames, dstBucketName, dstObjectNames, this);

    // Gather FileNotFoundExceptions for individual objects,
    // but only throw a single combined exception at the end.
    KeySetView<IOException, Boolean> innerExceptions = ConcurrentHashMap.newKeySet();

    // Perform the copy operations.
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(
            httpRequestInitializer, gcs, storageOptions.getMaxRequestsPerBatch());

    for (int i = 0; i < srcObjectNames.size(); i++) {
      if (storageOptions.isCopyWithRewriteEnabled()) {
        // Rewrite request has the same effect as Copy, but it can handle moving
        // large objects that may potentially timeout a Copy request.
        rewriteInternal(
            batchHelper,
            innerExceptions,
            srcBucketName, srcObjectNames.get(i),
            dstBucketName, dstObjectNames.get(i));
      } else {
        copyInternal(
            batchHelper,
            innerExceptions,
            srcBucketName, srcObjectNames.get(i),
            dstBucketName, dstObjectNames.get(i));
      }
    }

    // Execute any remaining requests not divisible by the max batch size.
    batchHelper.flush();

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  /**
   * Performs copy operation using GCS Rewrite requests
   *
   * @see GoogleCloudStorage#copy(String, List, String, List)
   */
  private void rewriteInternal(
      final BatchHelper batchHelper,
      final KeySetView<IOException, Boolean> innerExceptions,
      final String srcBucketName, final String srcObjectName,
      final String dstBucketName, final String dstObjectName)
      throws IOException {
    Storage.Objects.Rewrite rewriteObject =
        configureRequest(
            gcs.objects().rewrite(srcBucketName, srcObjectName, dstBucketName, dstObjectName, null),
            srcBucketName);

    // TODO(b/79750454) do not batch rewrite requests because they time out in batches.
    batchHelper.queue(
        rewriteObject,
        new JsonBatchCallback<RewriteResponse>() {
          @Override
          public void onSuccess(RewriteResponse rewriteResponse, HttpHeaders responseHeaders) {
            String srcString = StorageResourceId.createReadableString(srcBucketName, srcObjectName);
            String dstString = StorageResourceId.createReadableString(dstBucketName, dstObjectName);

            if (rewriteResponse.getDone()) {
              LOG.debug("Successfully copied {} to {}", srcString, dstString);
            } else {
              // If an object is very large, we need to continue making successive calls to
              // rewrite until the operation completes.
              LOG.debug("Copy ({} to {}) did not complete. Resuming...", srcString, dstString);
              try {
                Storage.Objects.Rewrite rewriteObjectWithToken =
                    configureRequest(
                        gcs.objects()
                            .rewrite(
                                srcBucketName, srcObjectName, dstBucketName, dstObjectName, null),
                        srcBucketName);
                rewriteObjectWithToken.setRewriteToken(rewriteResponse.getRewriteToken());
                batchHelper.queue(rewriteObjectWithToken, this);
              } catch (IOException e) {
                innerExceptions.add(e);
              }
            }
          }

          @Override
          public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            onCopyFailure(innerExceptions, e, srcBucketName, srcObjectName);
          }
        });
  }

  /**
   * Performs copy operation using GCS Copy requests
   *
   * @see GoogleCloudStorage#copy(String, List, String, List)
   */
  private void copyInternal(
      BatchHelper batchHelper,
      final KeySetView<IOException, Boolean> innerExceptions,
      final String srcBucketName, final String srcObjectName,
      final String dstBucketName, final String dstObjectName)
      throws IOException {
    Storage.Objects.Copy copyObject =
        configureRequest(
            gcs.objects().copy(srcBucketName, srcObjectName, dstBucketName, dstObjectName, null),
            srcBucketName);

    batchHelper.queue(
        copyObject,
        new JsonBatchCallback<StorageObject>() {
          @Override
          public void onSuccess(StorageObject copyResponse, HttpHeaders responseHeaders) {
            String srcString = StorageResourceId.createReadableString(srcBucketName, srcObjectName);
            String dstString = StorageResourceId.createReadableString(dstBucketName, dstObjectName);
            LOG.debug("Successfully copied {} to {}", srcString, dstString);
          }

          @Override
          public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            onCopyFailure(innerExceptions, e, srcBucketName, srcObjectName);
          }
        });
  }

  /** Processes failed copy requests */
  private void onCopyFailure(
      KeySetView<IOException, Boolean> innerExceptions,
      GoogleJsonError e,
      String srcBucketName, String srcObjectName) {
    if (errorExtractor.itemNotFound(e)) {
      String srcString = StorageResourceId.createReadableString(srcBucketName, srcObjectName);
      LOG.debug("copy({}) : not found", srcString);
      innerExceptions.add(
          GoogleCloudStorageExceptions.getFileNotFoundException(srcBucketName, srcObjectName));
    } else {
      innerExceptions.add(
          wrapException(
              new IOException(e.toString()), "Error copying", srcBucketName, srcObjectName));
    }
  }

  /**
   * Shared helper for actually dispatching buckets().list() API calls and accumulating paginated
   * results; these can then be used to either extract just their names, or to parse into full
   * GoogleCloudStorageItemInfos.
   */
  private List<Bucket> listBucketsInternal() throws IOException {
    LOG.debug("listBucketsInternal()");
    checkNotNull(storageOptions.getProjectId(), "projectId must not be null");
    List<Bucket> allBuckets = new ArrayList<>();
    Storage.Buckets.List listBucket =
        configureRequest(gcs.buckets().list(storageOptions.getProjectId()), null);

    // Set number of items to retrieve per call.
    listBucket.setMaxResults(storageOptions.getMaxListItemsPerCall());

    // Loop till we fetch all items.
    String pageToken = null;
    do {
      if (pageToken != null) {
        LOG.debug("listBucketsInternal: next page {}", pageToken);
        listBucket.setPageToken(pageToken);
      }

      Buckets items = listBucket.execute();

      // Accumulate buckets (if any).
      List<Bucket> buckets = items.getItems();
      if (buckets != null) {
        LOG.debug("listed {} items", buckets.size());
        allBuckets.addAll(buckets);
      }

      pageToken = items.getNextPageToken();
    } while (pageToken != null);

    return allBuckets;
  }

  /**
   * See {@link GoogleCloudStorage#listBucketNames()} for details about expected behavior.
   */
  @Override
  public List<String> listBucketNames()
      throws IOException {
    LOG.debug("listBucketNames()");
    List<Bucket> allBuckets = listBucketsInternal();
    List<String> bucketNames = new ArrayList<>(allBuckets.size());
    for (Bucket bucket : allBuckets) {
      bucketNames.add(bucket.getName());
    }
    return bucketNames;
  }

  /**
   * See {@link GoogleCloudStorage#listBucketInfo()} for details about expected behavior.
   */
  @Override
  public List<GoogleCloudStorageItemInfo> listBucketInfo()
      throws IOException {
    LOG.debug("listBucketInfo()");
    List<Bucket> allBuckets = listBucketsInternal();
    List<GoogleCloudStorageItemInfo> bucketInfos = new ArrayList<>(allBuckets.size());
    for (Bucket bucket : allBuckets) {
      bucketInfos.add(new GoogleCloudStorageItemInfo(
          new StorageResourceId(bucket.getName()), bucket.getTimeCreated().getValue(), 0,
          bucket.getLocation(), bucket.getStorageClass()));
    }
    return bucketInfos;
  }

  /**
   * Helper for creating a Storage.Objects.Copy object ready for dispatch given a bucket and object
   * for an empty object to be created. Caller must already verify that {@code resourceId}
   * represents a StorageObject and not a bucket.
   */
  private Storage.Objects.Insert prepareEmptyInsert(
      StorageResourceId resourceId, CreateObjectOptions createObjectOptions) throws IOException {
    StorageObject object = new StorageObject();
    object.setName(resourceId.getObjectName());
    Map<String, String> rewrittenMetadata = encodeMetadata(createObjectOptions.getMetadata());
    object.setMetadata(rewrittenMetadata);

    // Ideally we'd use EmptyContent, but Storage requires an AbstractInputStreamContent and not
    // just an HttpContent, so we'll just use the next easiest thing.
    ByteArrayContent emptyContent =
        new ByteArrayContent(createObjectOptions.getContentType(), new byte[0]);
    Storage.Objects.Insert insertObject =
        configureRequest(
            gcs.objects().insert(resourceId.getBucketName(), object, emptyContent),
            resourceId.getBucketName());
    insertObject.setDisableGZipContent(true);
    clientRequestHelper.setDirectUploadEnabled(insertObject, true);

    if (resourceId.hasGenerationId()) {
      insertObject.setIfGenerationMatch(resourceId.getGenerationId());
    } else if (!createObjectOptions.overwriteExisting()) {
      insertObject.setIfGenerationMatch(0L);
    }
    return insertObject;
  }

  /**
   * Helper for both listObjectNames and listObjectInfo that executes the actual API calls to get
   * paginated lists, accumulating the StorageObjects and String prefixes into the params {@code
   * listedObjects} and {@code listedPrefixes}.
   *
   * @param bucketName bucket name
   * @param objectNamePrefix object name prefix or null if all objects in the bucket are desired
   * @param delimiter delimiter to use (typically "/"), otherwise null
   * @param includeTrailingDelimiter whether to include prefix objects into the {@code
   *     listedObjects}
   * @param maxResults maximum number of results to return (total of both {@code listedObjects} and
   *     {@code listedPrefixes}), unlimited if negative or zero
   * @param listedObjects output parameter into which retrieved StorageObjects will be added
   * @param listedPrefixes output parameter into which retrieved prefixes will be added
   */
  private void listStorageObjectsAndPrefixes(
      String bucketName,
      String objectNamePrefix,
      String delimiter,
      boolean includeTrailingDelimiter,
      long maxResults,
      List<StorageObject> listedObjects,
      List<String> listedPrefixes)
      throws IOException {
    LOG.debug(
        "listStorageObjectsAndPrefixes({}, {}, {}, {})",
        bucketName, objectNamePrefix, delimiter, maxResults);

    checkArgument(!Strings.isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    checkArgument(
        listedObjects != null && listedObjects.isEmpty(),
        "Must provide a non-null empty container for listedObjects.");
    checkArgument(
        listedPrefixes != null && listedPrefixes.isEmpty(),
        "Must provide a non-null empty container for listedPrefixes.");

    Storage.Objects.List listObject = configureRequest(gcs.objects().list(bucketName), bucketName);

    // Set delimiter if supplied.
    if (delimiter != null) {
      listObject.setDelimiter(delimiter);
      listObject.setIncludeTrailingDelimiter(includeTrailingDelimiter);
    }

    // Set number of items to retrieve per call.
    if (maxResults <= 0 || maxResults + 1 >= storageOptions.getMaxListItemsPerCall()) {
      listObject.setMaxResults(storageOptions.getMaxListItemsPerCall());
    } else {
      // We add one in case we filter out objectNamePrefix.
      listObject.setMaxResults(maxResults + 1);
    }

    // Set prefix if supplied.
    if (!Strings.isNullOrEmpty(objectNamePrefix)) {
      listObject.setPrefix(objectNamePrefix);
    }

    // Loop till we fetch all items.
    String pageToken = null;

    // Deduplicate prefixes and items, because if 'includeTrailingDelimiter' set to true
    // then returned items will contain "prefix objects" too.
    Set<String> prefixes = new LinkedHashSet<>();
    do {
      if (pageToken != null) {
        LOG.debug("listStorageObjectsAndPrefixes: next page {}", pageToken);
        listObject.setPageToken(pageToken);
      }

      Objects items;
      try {
        items = listObject.execute();
      } catch (IOException e) {
        if (errorExtractor.itemNotFound(e)) {
          LOG.debug(
              "listStorageObjectsAndPrefixes({}, {}, {}, {}): item not found",
              bucketName, objectNamePrefix, delimiter, maxResults);
          break;
        } else {
          throw wrapException(e, "Error listing", bucketName, objectNamePrefix);
        }
      }

      // Add prefixes (if any).
      List<String> pagePrefixes = items.getPrefixes();
      if (pagePrefixes != null) {
        LOG.debug("listed {} prefixes", pagePrefixes.size());
        long maxRemainingResults = getMaxRemainingResults(maxResults, prefixes, listedObjects);
        // Do not cast 'maxRemainingResults' to int here, it could overflow
        long maxPrefixes = Math.min(maxRemainingResults, (long) pagePrefixes.size());
        prefixes.addAll(pagePrefixes.subList(0, (int) maxPrefixes));
      }

      // Add object names (if any).
      List<StorageObject> objects = items.getItems();
      if (objects != null) {
        LOG.debug("listed {} objects", objects.size());

        // Although GCS does not implement a file system, it treats objects that end
        // in delimiter as different from other objects when listing objects.
        //
        // If caller sends foo/ as the prefix, foo/ is returned as an object name.
        // That is inconsistent with listing items in a directory.
        // Not sure if that is a bug in GCS or the intended behavior.
        //
        // In this case, we do not want foo/ in the returned list because we want to
        // keep the behavior more like a file system without calling it as such.
        // Therefore, we filter out such entry.

        // Determine if the caller sent a directory name as a prefix.
        boolean objectPrefixEndsWithDelimiter =
            !Strings.isNullOrEmpty(objectNamePrefix) && objectNamePrefix.endsWith(PATH_DELIMITER);

        long maxRemainingResults = getMaxRemainingResults(maxResults, prefixes, listedObjects);
        for (StorageObject object : objects) {
          String objectName = object.getName();
          if (!objectPrefixEndsWithDelimiter || !objectName.equals(objectNamePrefix)) {
            if (prefixes.remove(objectName)) {
              listedObjects.add(object);
            } else if (maxRemainingResults > 0) {
              listedObjects.add(object);
              maxRemainingResults--;
            }
            // Do not break here, because we want to be sure
            // that we replaced all prefixes with prefix objects
          }
        }
      }

      pageToken = items.getNextPageToken();
    } while (pageToken != null && getMaxRemainingResults(maxResults, prefixes, listedObjects) > 0);

    listedPrefixes.addAll(prefixes);
  }

  private static long getMaxRemainingResults(
      long maxResults, Set<String> prefixes, List<StorageObject> objects) {
    if (maxResults <= 0) {
      return Long.MAX_VALUE;
    }
    long numResults = (long) prefixes.size() + objects.size();
    return maxResults - numResults;
  }

  /**
   * See {@link GoogleCloudStorage#listObjectNames(String, String, String)}
   * for details about expected behavior.
   */
  @Override
  public List<String> listObjectNames(
      String bucketName, String objectNamePrefix, String delimiter)
      throws IOException {
    return listObjectNames(
        bucketName, objectNamePrefix, delimiter, GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  /**
   * See {@link GoogleCloudStorage#listObjectNames(String, String, String, long)} for details about
   * expected behavior.
   */
  @Override
  public List<String> listObjectNames(
      String bucketName, String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    LOG.debug(
        "listObjectNames({}, {}, {}, {})", bucketName, objectNamePrefix, delimiter, maxResults);

    // Helper will handle going through pages of list results and accumulating them.
    List<StorageObject> listedObjects = new ArrayList<>();
    List<String> listedPrefixes = new ArrayList<>();
    listStorageObjectsAndPrefixes(
        bucketName,
        objectNamePrefix,
        delimiter,
        /* includeTrailingDelimiter= */ false,
        maxResults,
        listedObjects,
        listedPrefixes);

    // Just use the prefix list as a starting point, and extract all the names from the
    // StorageObjects, adding them to the list.
    // TODO(user): Maybe de-dupe if it's possible for GCS to return duplicates.
    List<String> objectNames = listedPrefixes;
    for (StorageObject obj : listedObjects) {
      objectNames.add(obj.getName());
    }
    return objectNames;
  }

  /**
   * See {@link GoogleCloudStorage#listObjectInfo(String, String, String)}
   * for details about expected behavior.
   */
  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      final String bucketName, String objectNamePrefix, String delimiter)
      throws IOException {
    return listObjectInfo(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  /**
   * See {@link GoogleCloudStorage#listObjectInfo(String, String, String, long)} for details about
   * expected behavior.
   */
  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      final String bucketName, String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    LOG.debug(
        "listObjectInfo({}, {}, {}, {})", bucketName, objectNamePrefix, delimiter, maxResults);

    // Helper will handle going through pages of list results and accumulating them.
    List<StorageObject> listedObjects = new ArrayList<>();
    List<String> listedPrefixes = new ArrayList<>();
    listStorageObjectsAndPrefixes(
        bucketName,
        objectNamePrefix,
        delimiter,
        /* includeTrailingDelimiter= */ storageOptions.isListDirectoryObjects(),
        maxResults,
        listedObjects,
        listedPrefixes);

    // For the listedObjects, we simply parse each item into a GoogleCloudStorageItemInfo without
    // further work.
    List<GoogleCloudStorageItemInfo> objectInfos = new ArrayList<>(listedObjects.size());
    for (StorageObject obj : listedObjects) {
      objectInfos.add(
          createItemInfoForStorageObject(new StorageResourceId(bucketName, obj.getName()), obj));
    }

    if (!listedPrefixes.isEmpty()) {
      Set<StorageResourceId> prefixIds = new LinkedHashSet<>(listedPrefixes.size());
      for (String prefix : listedPrefixes) {
        prefixIds.add(new StorageResourceId(bucketName, prefix));
      }

      if (!storageOptions.isListDirectoryObjects()) {
        // Send requests to fetch info about the directories associated with each prefix in batch
        // requests, maxRequestsPerBatch at a time.
        for (GoogleCloudStorageItemInfo prefixInfo : getItemInfos(new ArrayList<>(prefixIds))) {
          if (prefixInfo.exists()) {
            prefixIds.remove(prefixInfo.getResourceId());
            objectInfos.add(prefixInfo);
          }
        }

        if (prefixIds.isEmpty()) {
          return objectInfos;
        }
      }

      // Handle repairs.
      if (storageOptions.isAutoRepairImplicitDirectoriesEnabled()) {
        LOG.info("Repairing batch of {} missing directories.", prefixIds.size());
        LOG.debug("Directories to repair: {}", prefixIds);
        List<StorageResourceId> prefixIdsList = new ArrayList<>(prefixIds);
        try {
          createEmptyObjects(prefixIdsList);
        } catch (IOException ioe) {
          // Don't totally fail the listObjectInfo call, since auto-repair is best-effort anyways.
          LOG.error("Failed to repair some missing directories.", ioe);
        }

        List<GoogleCloudStorageItemInfo> repairedInfos = getItemInfos(prefixIdsList);
        objectInfos.addAll(
            inferOrFilterNotRepairedInfos(
                repairedInfos, storageOptions.isInferImplicitDirectoriesEnabled()));
      } else if (storageOptions.isInferImplicitDirectoriesEnabled()) {
        for (StorageResourceId prefixId : prefixIds) {
          objectInfos.add(GoogleCloudStorageItemInfo.createInferredDirectory(prefixId));
        }
      } else {
        LOG.info(
            "Directory repair and inferred directories are disabled, "
                + "giving up on retrieving missing directories: {}",
            prefixIds);
      }
    }

    return objectInfos;
  }

  static List<GoogleCloudStorageItemInfo> inferOrFilterNotRepairedInfos(
      List<GoogleCloudStorageItemInfo> repairInfos, boolean inferImplicitDirectories) {
    // Fetch and append all the repaired items metadata.
    List<GoogleCloudStorageItemInfo> objectInfos = new ArrayList<>(repairInfos.size());
    int numRepaired = 0;
    for (GoogleCloudStorageItemInfo repairedInfo : repairInfos) {
      if (repairedInfo.exists()) {
        objectInfos.add(repairedInfo);
        ++numRepaired;
      } else {
        StorageResourceId resourceId = repairedInfo.getResourceId();
        if (inferImplicitDirectories) {
          LOG.info("Repair for '{}' failed, using inferred directory", resourceId);
          objectInfos.add(GoogleCloudStorageItemInfo.createInferredDirectory(resourceId));
        } else {
          LOG.info("Repair for '{}' failed, skipping", resourceId);
        }
      }
    }
    LOG.info("Successfully repaired {}/{} implicit directories.", numRepaired, repairInfos.size());
    return objectInfos;
  }

  /** Helper for converting a StorageResourceId + Bucket into a GoogleCloudStorageItemInfo. */
  public static GoogleCloudStorageItemInfo createItemInfoForBucket(
      StorageResourceId resourceId, Bucket bucket) {
    Preconditions.checkArgument(resourceId != null, "resourceId must not be null");
    Preconditions.checkArgument(bucket != null, "bucket must not be null");
    Preconditions.checkArgument(
        resourceId.isBucket(), "resourceId must be a Bucket. resourceId: %s", resourceId);
    Preconditions.checkArgument(
        resourceId.getBucketName().equals(bucket.getName()),
        "resourceId.getBucketName() must equal bucket.getName(): '%s' vs '%s'",
        resourceId.getBucketName(), bucket.getName());

    // For buckets, size is 0.
    return new GoogleCloudStorageItemInfo(resourceId, bucket.getTimeCreated().getValue(),
        0, bucket.getLocation(), bucket.getStorageClass());
  }

  /**
   * Helper for converting a StorageResourceId + StorageObject into a GoogleCloudStorageItemInfo.
   */
  public static GoogleCloudStorageItemInfo createItemInfoForStorageObject(
      StorageResourceId resourceId, StorageObject object) {
    Preconditions.checkArgument(resourceId != null, "resourceId must not be null");
    Preconditions.checkArgument(object != null, "object must not be null");
    Preconditions.checkArgument(
        resourceId.isStorageObject(),
        "resourceId must be a StorageObject. resourceId: %s",
        resourceId);
    Preconditions.checkArgument(
        resourceId.getBucketName().equals(object.getBucket()),
        "resourceId.getBucketName() must equal object.getBucket(): '%s' vs '%s'",
        resourceId.getBucketName(), object.getBucket());
    Preconditions.checkArgument(
        resourceId.getObjectName().equals(object.getName()),
        "resourceId.getObjectName() must equal object.getName(): '%s' vs '%s'",
        resourceId.getObjectName(), object.getName());

    Map<String, byte[]> decodedMetadata =
        object.getMetadata() == null ? null : decodeMetadata(object.getMetadata());

    byte[] md5Hash = null;
    byte[] crc32c = null;

    if (!Strings.isNullOrEmpty(object.getCrc32c())) {
      crc32c = BaseEncoding.base64().decode(object.getCrc32c());
    }

    if (!Strings.isNullOrEmpty(object.getMd5Hash())) {
      md5Hash = BaseEncoding.base64().decode(object.getMd5Hash());
    }

    // GCS API does not make available location and storage class at object level at present
    // (it is same for all objects in a bucket). Further, we do not use the values for objects.
    // The GoogleCloudStorageItemInfo thus has 'null' for location and storage class.
    return new GoogleCloudStorageItemInfo(
        resourceId,
        object.getUpdated().getValue(),
        object.getSize().longValue(),
        /* location= */ null,
        /* storageClass= */ null,
        object.getContentType(),
        decodedMetadata,
        object.getGeneration(),
        object.getMetageneration(),
        new VerificationAttributes(md5Hash, crc32c));
  }

  /**
   * Helper for converting from a Map&lt;String, byte[]&gt; metadata map that may be in a
   * StorageObject into a Map&lt;String, String&gt; suitable for placement inside a
   * GoogleCloudStorageItemInfo.
   */
  @VisibleForTesting
  static Map<String, String> encodeMetadata(Map<String, byte[]> metadata) {
    return Maps.transformValues(metadata, ENCODE_METADATA_VALUES);
  }

  /**
   * Inverse function of {@link #encodeMetadata(Map)}.
   */
  @VisibleForTesting
  static Map<String, byte[]> decodeMetadata(Map<String, String> metadata) {
    return Maps.transformValues(metadata, DECODE_METADATA_VALUES);
  }

  /** See {@link GoogleCloudStorage#getItemInfos(List)} for details about expected behavior. */
  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    LOG.debug("getItemInfos({})", resourceIds);

    final Map<StorageResourceId, GoogleCloudStorageItemInfo> itemInfos = new ConcurrentHashMap<>();
    final Set<IOException> innerExceptions = newConcurrentHashSet();
    BatchHelper batchHelper = batchFactory.newBatchHelper(
        httpRequestInitializer,
        gcs,
        storageOptions.getMaxRequestsPerBatch());

    // For each resourceId, we'll either directly add ROOT_INFO, enqueue a Bucket fetch request, or
    // enqueue a StorageObject fetch request.
    for (final StorageResourceId resourceId : resourceIds) {
      if (resourceId.isRoot()) {
        itemInfos.put(resourceId, GoogleCloudStorageItemInfo.ROOT_INFO);
      } else if (resourceId.isBucket()) {
        batchHelper.queue(
            configureRequest(
                gcs.buckets().get(resourceId.getBucketName()), resourceId.getBucketName()),
            new JsonBatchCallback<Bucket>() {
              @Override
              public void onSuccess(Bucket bucket, HttpHeaders responseHeaders) {
                LOG.debug(
                    "getItemInfos: Successfully fetched bucket: {} for resourceId: {}",
                    bucket, resourceId);
                itemInfos.put(resourceId, createItemInfoForBucket(resourceId, bucket));
              }

              @Override
              public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
                if (errorExtractor.itemNotFound(e)) {
                  LOG.debug("getItemInfos: bucket not found: {}", resourceId.getBucketName());
                  itemInfos.put(resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
                } else {
                  IOException ioException = new IOException(e.toString());
                  innerExceptions.add(
                      wrapException(
                          ioException, "Error getting Bucket: ", resourceId.getBucketName(), null));
                }
              }
            });
      } else {
        final String bucketName = resourceId.getBucketName();
        final String objectName = resourceId.getObjectName();
        batchHelper.queue(
            configureRequest(gcs.objects().get(bucketName, objectName), bucketName),
            new JsonBatchCallback<StorageObject>() {
              @Override
              public void onSuccess(StorageObject obj, HttpHeaders responseHeaders) {
                LOG.debug(
                    "getItemInfos: Successfully fetched object '{}' for resourceId '{}'",
                    obj, resourceId);
                itemInfos.put(resourceId, createItemInfoForStorageObject(resourceId, obj));
              }

              @Override
              public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
                if (errorExtractor.itemNotFound(e)) {
                  LOG.debug("getItemInfos: object not found: {}", resourceId);
                  itemInfos.put(resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
                } else {
                  IOException ioException = new IOException(e.toString());
                  innerExceptions.add(
                      wrapException(
                          ioException, "Error getting StorageObject: ", bucketName, objectName));
                }
              }
            });
      }
    }
    batchHelper.flush();

    if (innerExceptions.size() > 0) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }

    // Assemble the return list in the same order as the input arguments.
    List<GoogleCloudStorageItemInfo> sortedItemInfos = new ArrayList<>();
    for (StorageResourceId resourceId : resourceIds) {
      Preconditions.checkState(
          itemInfos.containsKey(resourceId),
          "Somehow missing resourceId '%s' from map: %s",
          resourceId, itemInfos);
      sortedItemInfos.add(itemInfos.get(resourceId));
    }

    // We expect the return list to be the same size, even if some entries were "not found".
    Preconditions.checkState(
        sortedItemInfos.size() == resourceIds.size(),
        "sortedItemInfos.size() (%s) != resourceIds.size() (%s). infos: %s, ids: %s",
        sortedItemInfos.size(), resourceIds.size(), sortedItemInfos, resourceIds);
    return sortedItemInfos;
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    LOG.debug("updateItems({})", itemInfoList);

    final Map<StorageResourceId, GoogleCloudStorageItemInfo> resultItemInfos =
        new ConcurrentHashMap<>();
    final Set<IOException> innerExceptions = newConcurrentHashSet();
    BatchHelper batchHelper = batchFactory.newBatchHelper(
        httpRequestInitializer,
        gcs,
        storageOptions.getMaxRequestsPerBatch());

    for (UpdatableItemInfo itemInfo : itemInfoList) {
      Preconditions.checkArgument(!itemInfo.getStorageResourceId().isBucket()
          && !itemInfo.getStorageResourceId().isRoot(),
          "Buckets and GCS Root resources are not supported for updateItems");
    }

    for (final UpdatableItemInfo itemInfo : itemInfoList) {
      final StorageResourceId resourceId = itemInfo.getStorageResourceId();
      final String bucketName = resourceId.getBucketName();
      final String objectName = resourceId.getObjectName();

      Map<String, byte[]> originalMetadata = itemInfo.getMetadata();
      Map<String, String> rewrittenMetadata = encodeMetadata(originalMetadata);

      Storage.Objects.Patch patch =
          configureRequest(
              gcs.objects()
                  .patch(
                      bucketName, objectName, new StorageObject().setMetadata(rewrittenMetadata)),
              bucketName);

      batchHelper.queue(
          patch,
          new JsonBatchCallback<StorageObject>() {
            @Override
            public void onSuccess(StorageObject obj, HttpHeaders responseHeaders) {
              LOG.debug(
                  "updateItems: Successfully updated object '{}' for resourceId '{}'",
                  obj, resourceId);
              resultItemInfos.put(resourceId, createItemInfoForStorageObject(resourceId, obj));
            }

            @Override
            public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
              if (errorExtractor.itemNotFound(e)) {
                LOG.debug("updateItems: object not found: {}", resourceId);
                resultItemInfos.put(
                    resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
              } else {
                IOException ioException = new IOException(e.toString());
                innerExceptions.add(
                    wrapException(
                        ioException, "Error getting StorageObject: ", bucketName, objectName));
              }
            }
          });
    }
    batchHelper.flush();

    if (innerExceptions.size() > 0) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }

    // Assemble the return list in the same order as the input arguments.
    List<GoogleCloudStorageItemInfo> sortedItemInfos = new ArrayList<>();
    for (UpdatableItemInfo itemInfo : itemInfoList) {
      Preconditions.checkState(
          resultItemInfos.containsKey(itemInfo.getStorageResourceId()),
          "Missing resourceId '%s' from map: %s",
          itemInfo.getStorageResourceId(), resultItemInfos);
      sortedItemInfos.add(resultItemInfos.get(itemInfo.getStorageResourceId()));
    }

    // We expect the return list to be the same size, even if some entries were "not found".
    Preconditions.checkState(
        sortedItemInfos.size() == itemInfoList.size(),
        "sortedItemInfos.size() (%s) != resourceIds.size() (%s). infos: %s, updateItemInfos: %s",
        sortedItemInfos.size(), itemInfoList.size(), sortedItemInfos, itemInfoList);
    return sortedItemInfos;
  }

  /**
   * See {@link GoogleCloudStorage#getItemInfo(StorageResourceId)} for details about expected
   * behavior.
   */
  @Override
  public GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId)
      throws IOException {
    LOG.debug("getItemInfo({})", resourceId);

    // Handle ROOT case first.
    if (resourceId.isRoot()) {
      return GoogleCloudStorageItemInfo.ROOT_INFO;
    }

    GoogleCloudStorageItemInfo itemInfo = null;

    // Determine object size.
    //
    // For buckets, size is 0.
    // For objects not found, size is -1.
    // For objects that exist, size is in number of bytes.
    if (resourceId.isBucket()) {
      Bucket bucket = getBucket(resourceId.getBucketName());
      if (bucket != null) {
        itemInfo = createItemInfoForBucket(resourceId, bucket);
      }
    } else {
      StorageObject object = getObject(resourceId);
      if (object != null) {
        itemInfo = createItemInfoForStorageObject(resourceId, object);
      }
    }

    if (itemInfo == null) {
      itemInfo = GoogleCloudStorageItemInfo.createNotFound(resourceId);
    }
    LOG.debug("getItemInfo: {}", itemInfo);
    return itemInfo;
  }

  /**
   * See {@link GoogleCloudStorage#close()} for details about expected behavior.
   */
  @Override
  public void close() {
    // Calling shutdown() is a no-op if it was already called earlier,
    // therefore no need to guard against that by setting threadPool to null.
    LOG.debug("close()");
    threadPool.shutdown();
    manualBatchingThreadPool.shutdown();
  }

  /**
   * Gets the bucket with the given name.
   *
   * @param bucketName name of the bucket to get
   * @return the bucket with the given name or null if bucket not found
   * @throws IOException if the bucket exists but cannot be accessed
   */
  private Bucket getBucket(String bucketName)
      throws IOException {
    LOG.debug("getBucket({})", bucketName);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(bucketName),
        "bucketName must not be null or empty");
    Bucket bucket = null;
    Storage.Buckets.Get getBucket = configureRequest(gcs.buckets().get(bucketName), bucketName);
    try {
      bucket = getBucket.execute();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        LOG.debug("getBucket({}) : not found", bucketName);
      } else {
        LOG.debug(String.format("getBucket(%s) threw exception: ", bucketName), e);
        throw wrapException(e, "Error accessing", bucketName, null);
      }
    }
    return bucket;
  }

  /**
   * Gets the object generation for a Write operation
   *
   * @param resourceId object for which generation info is requested
   * @return the generation of the object
   * @throws IOException if the object already exists and cannot be overwritten
   */
  private long getWriteGeneration(StorageResourceId resourceId, boolean overwritable)
      throws IOException {
    LOG.debug("getWriteGeneration({}, {})", resourceId, overwritable);
    GoogleCloudStorageItemInfo info = getItemInfo(resourceId);
    if (!info.exists()) {
      return 0L;
    } else if (info.exists() && overwritable) {
      long generation = info.getContentGeneration();
      Preconditions.checkState(generation != 0, "Generation should not be 0 for an existing item");
      return generation;
    } else {
      throw new FileAlreadyExistsException(
          String.format("Object %s already exists.", resourceId.toString()));
    }
  }

  /**
   * Wraps the given IOException into another IOException,
   * adding the given error message and a reference to the supplied
   * bucket and object. It allows one to know which bucket and object
   * were being accessed when the exception occurred for an operation.
   */
  @VisibleForTesting
  IOException wrapException(IOException e, String message, String bucketName, String objectName) {
    String name = "bucket: " + bucketName;
    if (!Strings.isNullOrEmpty(objectName)) {
      name += ", object: " + objectName;
    }
    String fullMessage = String.format("%s: %s", message, name);
    return new IOException(fullMessage, e);
  }

  /**
   * Gets the object with the given resourceId.
   *
   * @param resourceId identifies a StorageObject
   * @return the object with the given name or null if object not found
   * @throws IOException if the object exists but cannot be accessed
   */
  private StorageObject getObject(StorageResourceId resourceId)
      throws IOException {
    LOG.debug("getObject({})", resourceId);
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    String bucketName = resourceId.getBucketName();
    String objectName = resourceId.getObjectName();
    StorageObject object = null;
    Storage.Objects.Get getObject =
        configureRequest(gcs.objects().get(bucketName, objectName), bucketName);
    try {
      object = getObject.execute();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        LOG.debug("getObject({}) : not found", resourceId);
      } else {
        LOG.debug(String.format("getObject(%s) threw exception: ", resourceId), e);
        throw wrapException(e, "Error accessing", bucketName, objectName);
      }
    }
    return object;
  }

  /**
   * Helper to check whether an empty object already exists with the expected metadata specified
   * in {@code options}, to be used to determine whether it's safe to ignore an exception that
   * was thrown when trying to create the object, {@code exceptionOnCreate}.
   */
  private boolean canIgnoreExceptionForEmptyObject(
      IOException exceptionOnCreate, StorageResourceId resourceId, CreateObjectOptions options)
      throws IOException {
    // TODO(user): Maybe also add 409 and even 412 errors if they pop up in this use case.
    // 500 ISE and 503 Service Unavailable tend to be raised when spamming GCS with create requests:
    if (errorExtractor.rateLimited(exceptionOnCreate)
        || errorExtractor.isInternalServerError(exceptionOnCreate)) {
      // We know that this is an error that is most often associated with trying to create an empty
      // object from multiple workers at the same time. We perform the following assuming that we
      // will eventually succeed and find an existing object. This will add up to a user-defined
      // maximum delay that caller will wait to receive an exception in the case of an incorrect
      // assumption and this being a scenario other than the multiple workers racing situation.
      GoogleCloudStorageItemInfo existingInfo;
      BackOff backOff;
      int maxWaitMillis = storageOptions.getMaxWaitMillisForEmptyObjectCreation();
      if (maxWaitMillis > 0) {
        backOff = new ExponentialBackOff.Builder()
            .setMaxElapsedTimeMillis(maxWaitMillis)
            .setMaxIntervalMillis(500)
            .setInitialIntervalMillis(100)
            .setMultiplier(1.5)
            .setRandomizationFactor(0.15)
            .build();
      } else {
        backOff = BackOff.STOP_BACKOFF;
      }
      long nextSleep = 0L;
      do {
        if (nextSleep > 0) {
          try {
            sleeper.sleep(nextSleep);
          } catch (InterruptedException e) {
            // We caught an InterruptedException, we should set the interrupted bit on this thread.
            Thread.currentThread().interrupt();
            nextSleep = BackOff.STOP;
          }
        }
        existingInfo = getItemInfo(resourceId);
        nextSleep = nextSleep == BackOff.STOP ? BackOff.STOP : backOff.nextBackOffMillis();
      } while (!existingInfo.exists() && nextSleep != BackOff.STOP);

      // Compare existence, size, and metadata; for 429 errors creating an empty object,
      // we don't care about metaGeneration/contentGeneration as long as the metadata
      // matches, since we don't know for sure whether our low-level request succeeded
      // first or some other client succeeded first.
      if (existingInfo.exists() && existingInfo.getSize() == 0) {
        if (!options.getRequireMetadataMatchForEmptyObjects()) {
          return true;
        } else if (existingInfo.metadataEquals(options.getMetadata())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * See {@link GoogleCloudStorage#waitForBucketEmpty(String)} for details about expected behavior.
   */
  @Override
  public void waitForBucketEmpty(String bucketName)
      throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(bucketName),
        "bucketName must not be null or empty");

    for (int i = 0; i < BUCKET_EMPTY_MAX_RETRIES; i++) {
      // We only need one item to see the bucket is not yet empty.
      List<String> objectNames = listObjectNames(bucketName, null, PATH_DELIMITER, 1);
      if (objectNames.isEmpty()) {
        return;
      }
      try {
        sleeper.sleep(BUCKET_EMPTY_WAIT_TIME_MS);
      } catch (InterruptedException ignored) {
        // Ignore the exception and loop.
      }
    }
    throw new IOException("Internal error: bucket not empty: " + bucketName);
  }

  @Override
  public void compose(
      final String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    LOG.debug("compose({}, {}, {}, {})", bucketName, sources, destination, contentType);
    List<StorageResourceId> sourceIds = Lists.transform(
        sources, new Function<String, StorageResourceId>() {
          @Override
          public StorageResourceId apply(String objectName) {
            return new StorageResourceId(bucketName, objectName);
          }
        });
    StorageResourceId destinationId = new StorageResourceId(bucketName, destination);
    CreateObjectOptions options = new CreateObjectOptions(
        true, contentType, CreateObjectOptions.EMPTY_METADATA);
    composeObjects(sourceIds, destinationId, options);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources,
      final StorageResourceId destination,
      CreateObjectOptions options)
      throws IOException {
    LOG.debug("composeObjects({}, {}, {})", sources, destination, options);
    for (StorageResourceId inputId : sources) {
      if (!destination.getBucketName().equals(inputId.getBucketName())) {
        throw new IOException(String.format(
            "Bucket doesn't match for source '%s' and destination '%s'!",
            inputId, destination));
      }
    }
    List<ComposeRequest.SourceObjects> sourceObjects =
        Lists.transform(
            sources,
            new Function<StorageResourceId, ComposeRequest.SourceObjects>() {
              @Override
              public ComposeRequest.SourceObjects apply(StorageResourceId input) {
                // TODO(user): Maybe set generationIds for source objects as well here.
                return new ComposeRequest.SourceObjects().setName(input.getObjectName());
              }
            });
    Storage.Objects.Compose compose =
        configureRequest(
            gcs.objects()
                .compose(
                    destination.getBucketName(),
                    destination.getObjectName(),
                    new ComposeRequest()
                        .setSourceObjects(sourceObjects)
                        .setDestination(
                            new StorageObject()
                                .setContentType(options.getContentType())
                                .setMetadata(encodeMetadata(options.getMetadata())))),
            destination.getBucketName());

    compose.setIfGenerationMatch(
        destination.hasGenerationId()
            ? destination.getGenerationId()
            : getWriteGeneration(destination, true));

    LOG.debug("composeObjects.execute()");
    GoogleCloudStorageItemInfo compositeInfo =
        createItemInfoForStorageObject(destination, compose.execute());
    LOG.debug("composeObjects() done, returning: {}", compositeInfo);
    return compositeInfo;
  }

  private <RequestT extends StorageRequest<?>> RequestT configureRequest(
      RequestT request, String bucketName) {
    setRequesterPaysProject(request, bucketName);
    return request;
  }

  private <RequestT extends StorageRequest<?>> void setRequesterPaysProject(
      RequestT request, String bucketName) {
    RequesterPaysOptions requesterPaysOptions = storageOptions.getRequesterPaysOptions();
    if (bucketName == null || requesterPaysOptions.getMode() == RequesterPaysMode.DISABLED) {
      return;
    }

    if (requesterPaysOptions.getMode() == RequesterPaysMode.ENABLED
        || (requesterPaysOptions.getMode() == RequesterPaysMode.CUSTOM
            && requesterPaysOptions.getBuckets().contains(bucketName))
        || (requesterPaysOptions.getMode() == RequesterPaysMode.AUTO
            && autoBuckets.getUnchecked(bucketName))) {
      setUserProject(request, requesterPaysOptions.getProjectId());
    }
  }

  private static <RequestT extends StorageRequest<?>> void setUserProject(
      RequestT request, String projectId) {
    Field userProjectField = request.getClassInfo().getField(USER_PROJECT_FIELD_NAME);
    if (userProjectField != null) {
      request.set(USER_PROJECT_FIELD_NAME, projectId);
    }
  }
}
