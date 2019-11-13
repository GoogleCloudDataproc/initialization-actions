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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createFileNotFoundException;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createJsonResponseException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newConcurrentHashSet;

import com.google.api.ClientProto;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
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
import com.google.api.services.storage.model.Bucket.Lifecycle;
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule;
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.Action;
import com.google.api.services.storage.model.Bucket.Lifecycle.Rule.Condition;
import com.google.api.services.storage.model.Buckets;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.RewriteResponse;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.google.storage.v1.StorageGrpc;
import com.google.google.storage.v1.StorageGrpc.StorageStub;
import com.google.google.storage.v1.StorageOuterClass;
import com.google.protobuf.util.Durations;
import io.grpc.alts.GoogleDefaultChannelBuilder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
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
import javax.annotation.Nullable;

/**
 * Provides read/write access to Google Cloud Storage (GCS), using Java nio channel semantics. This
 * is a basic implementation of the GoogleCloudStorage interface that mostly delegates through to
 * the appropriate API call(s) via the generated JSON API client, while adding reliability and
 * performance features such as setting up low-level retries, translating low-level exceptions, and
 * request batching.
 */
public class GoogleCloudStorageImpl implements GoogleCloudStorage {

  // JSON factory used for formatting GCS JSON API payloads.
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Maximum number of times to retry deletes in the case of precondition failures.
  private static final int MAXIMUM_PRECONDITION_FAILURES_IN_DELETE = 4;

  private static final String USER_PROJECT_FIELD_NAME = "userProject";

  // The GCS gRPC server.
  private static final String GRPC_TARGET =
      StorageOuterClass.getDescriptor()
          .findServiceByName("Storage")
          .getOptions()
          .getExtension(ClientProto.defaultHost);

  // The maximum number of times to automatically retry gRPC requests.
  private static final double GRPC_MAX_RETRY_ATTEMPTS = 10;

  // A function to encode metadata map values
  private static String encodeMetadataValues(byte[] bytes) {
    return bytes == null ? Data.NULL_STRING : BaseEncoding.base64().encode(bytes);
  }

  private static byte[] decodeMetadataValues(String value) {
    try {
      return BaseEncoding.base64().decode(value);
    } catch (IllegalArgumentException iae) {
      logger.atSevere().withCause(iae).log(
          "Failed to parse base64 encoded attribute value %s - %s", value, iae);
      return null;
    }
  }

  /** A factory for producing BackOff objects. */
  public interface BackOffFactory {
    BackOffFactory DEFAULT = ExponentialBackOff::new;

    BackOff newBackOff();
  }

  private final LoadingCache<String, Boolean> autoBuckets =
      CacheBuilder.newBuilder()
          .expireAfterWrite(Duration.ofHours(1))
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
  @VisibleForTesting Storage gcs;

  // GCS grpc stub.
  private StorageStub gcsGrpcStub;

  // Thread-pool used for background tasks.
  private ExecutorService backgroundTasksThreadPool =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setNameFormat("gcs-async-channel-pool-%d")
              .setDaemon(true)
              .build());

  // Thread-pool for manual matching of metadata tasks.
  // TODO(user): Wire out GoogleCloudStorageOptions for these.
  private ExecutorService manualBatchingThreadPool = createManualBatchingThreadPool();

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private ApiErrorExtractor errorExtractor = ApiErrorExtractor.INSTANCE;

  // Helper for interacting with objects involved with the API client libraries.
  private ClientRequestHelper<StorageObject> clientRequestHelper = new ClientRequestHelper<>();

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
    this(
        options,
        new RetryHttpInitializer(
            checkNotNull(credential, "credential must not be null"),
            options.getAppName(),
            options.getMaxHttpRequestRetries(),
            options.getHttpRequestConnectTimeout(),
            options.getHttpRequestReadTimeout(),
            options.getHttpRequestHeaders()));
  }

  @VisibleForTesting
  public GoogleCloudStorageImpl(
      GoogleCloudStorageOptions options, HttpRequestInitializer httpRequestInitializer)
      throws IOException {
    Preconditions.checkNotNull(options, "options must not be null");
    options.throwIfNotValid();
    logger.atFine().log("GCS(%s)", options.getAppName());
    this.storageOptions = options;

    this.httpRequestInitializer = httpRequestInitializer;

    HttpTransport httpTransport =
        HttpTransportFactory.createHttpTransport(
            options.getTransportType(),
            options.getProxyAddress(),
            options.getProxyUsername(),
            options.getProxyPassword());

    // Create GCS instance.
    this.gcs =
        new Storage.Builder(httpTransport, JSON_FACTORY, httpRequestInitializer)
            .setRootUrl(options.getStorageRootUrl())
            .setServicePath(options.getStorageServicePath())
            .setApplicationName(options.getAppName())
            .build();

    // Create the gRPC stub if necessary;
    if (storageOptions.isGrpcEnabled()) {
      Map<String, Object> serviceConfig = getGrpcServiceConfig(options.getReadChannelOptions());
      this.gcsGrpcStub =
          StorageGrpc.newStub(
              GoogleDefaultChannelBuilder.forTarget(GRPC_TARGET)
              .defaultServiceConfig(serviceConfig)
              .build())
          .withExecutor(backgroundTasksThreadPool);
    }
  }

  private Map<String, Object> getGrpcServiceConfig(GoogleCloudStorageReadOptions readOptions) {
    Map<String, Object> name = ImmutableMap.of("service", "google.storage.v1.Storage");

    Map<String, Object> retryPolicy =
        ImmutableMap.<String, Object>builder()
            .put("maxAttempts", GRPC_MAX_RETRY_ATTEMPTS)
            .put(
                "initialBackoff",
                Durations.fromMillis(readOptions.getBackoffInitialIntervalMillis()).toString())
            .put(
                "maxBackoff",
                Durations.fromMillis(readOptions.getBackoffMaxIntervalMillis()).toString())
            .put("backoffMultiplier", readOptions.getBackoffMultiplier())
            .put("retryableStatusCodes", ImmutableList.of("UNAVAILABLE", "RESOURCE_EXHAUSTED"))
            .build();

    Map<String, Object> methodConfig =
        ImmutableMap.of("name", ImmutableList.of(name), "retryPolicy", retryPolicy);

    return ImmutableMap.of("methodConfig", ImmutableList.of(methodConfig));
  }

  /**
   * Constructs an instance of GoogleCloudStorageImpl.
   *
   * @param gcs Preconstructed Storage to use for I/O.
   */
  public GoogleCloudStorageImpl(GoogleCloudStorageOptions options, Storage gcs) {
    Preconditions.checkNotNull(options, "options must not be null");

    logger.atFine().log("GCS(%s)", options.getAppName());

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
    this.storageOptions = GoogleCloudStorageOptions.builder().setAppName("test-app").build();
  }

  private ExecutorService createManualBatchingThreadPool() {
    ThreadPoolExecutor service =
        new ThreadPoolExecutor(
            /* corePoolSize= */ 10,
            /* maximumPoolSize= */ 20,
            /* keepAliveTime= */ 10L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-manual-batching-pool-%d")
                .setDaemon(true)
                .build());
    // allowCoreThreadTimeOut needs to be enabled for cases where the encapsulating class does not
    service.allowCoreThreadTimeOut(true);
    return service;
  }

  @VisibleForTesting
  void setBackgroundTasksThreadPool(ExecutorService backgroundTasksThreadPool) {
    this.backgroundTasksThreadPool = backgroundTasksThreadPool;
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
    logger.atFine().log("create(%s)", resourceId);
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

    Optional<Long> overwriteGeneration =
        resourceId.hasGenerationId()
            ? Optional.of(resourceId.getGenerationId())
            : Optional.of(getWriteGeneration(resourceId, options.overwriteExisting()));

    ObjectWriteConditions writeConditions =
        new ObjectWriteConditions(overwriteGeneration, Optional.<Long>absent());

    Map<String, String> rewrittenMetadata = encodeMetadata(options.getMetadata());

    if (storageOptions.isGrpcEnabled()) {
      Optional<String> requesterPaysProject =
          requesterShouldPay(resourceId.getBucketName())
              ? Optional.of(storageOptions.getRequesterPaysOptions().getProjectId())
              : Optional.absent();
      GoogleCloudStorageGrpcWriteChannel channel =
          new GoogleCloudStorageGrpcWriteChannel(
              backgroundTasksThreadPool,
              gcsGrpcStub,
              resourceId.getBucketName(),
              resourceId.getObjectName(),
              storageOptions.getWriteChannelOptions(),
              writeConditions,
              requesterPaysProject,
              rewrittenMetadata,
              options.getContentType());
      channel.initialize();
      return channel;
    } else {
      GoogleCloudStorageWriteChannel channel =
          new GoogleCloudStorageWriteChannel(
              backgroundTasksThreadPool,
              gcs,
              clientRequestHelper,
              resourceId.getBucketName(),
              resourceId.getObjectName(),
              options.getContentType(),
              options.getContentEncoding(),
              /* kmsKeyName= */ null,
              storageOptions.getWriteChannelOptions(),
              writeConditions,
              rewrittenMetadata) {

            @Override
            public Storage.Objects.Insert createRequest(InputStreamContent inputStream)
                throws IOException {
              return configureRequest(super.createRequest(inputStream), resourceId.getBucketName());
            }
          };
      channel.initialize();
      return channel;
    }
  }

  /**
   * See {@link GoogleCloudStorage#create(StorageResourceId)} for details about expected behavior.
   */
  @Override
  public WritableByteChannel create(StorageResourceId resourceId) throws IOException {
    logger.atFine().log("create(%s)", resourceId);
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
    logger.atFine().log("create(%s)", bucketName);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    checkNotNull(options, "options must not be null");
    checkNotNull(storageOptions.getProjectId(), "projectId must not be null");

    Bucket bucket =
        new Bucket()
            .setName(bucketName)
            .setLocation(options.getLocation())
            .setStorageClass(options.getStorageClass());
    if (options.getTtl() != null) {
      bucket.setLifecycle(
          new Lifecycle()
              .setRule(
                  ImmutableList.of(
                      new Rule()
                          .setAction(new Action().setType("Delete"))
                          .setCondition(
                              new Condition()
                                  .setAge(Math.toIntExact(options.getTtl().toDays()))))));
    }

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
      Thread.currentThread().interrupt();
      throw new IOException("Failed to create bucket", e);
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
        logger.atInfo().withCause(ioe).log(
            "Ignoring exception; verified object already exists with desired state.");
      } else {
        throw ioe;
      }
    }
  }

  /**
   * See {@link GoogleCloudStorage#createEmptyObject(StorageResourceId)} for details about expected
   * behavior.
   */
  @Override
  public void createEmptyObject(StorageResourceId resourceId) throws IOException {
    logger.atFine().log("createEmptyObject(%s)", resourceId);
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    createEmptyObject(resourceId, CreateObjectOptions.DEFAULT);
  }

  public void updateMetadata(GoogleCloudStorageItemInfo itemInfo, Map<String, byte[]> metadata)
      throws IOException {
    StorageResourceId resourceId = itemInfo.getResourceId();
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject ID, got %s", resourceId);

    StorageObject storageObject = new StorageObject().setMetadata(encodeMetadata(metadata));

    Storage.Objects.Patch patchObject =
        configureRequest(
                gcs.objects()
                    .patch(resourceId.getBucketName(), resourceId.getObjectName(), storageObject),
                resourceId.getBucketName())
            .setIfMetagenerationMatch(itemInfo.getMetaGeneration());

    patchObject.execute();
  }

  @Override
  public void createEmptyObjects(
      List<StorageResourceId> resourceIds, final CreateObjectOptions options) throws IOException {
    // TODO(user): This method largely follows a pattern similar to
    // deleteObjects(List<StorageResourceId>); extract a generic method for both.
    logger.atFine().log("createEmptyObjects(%s)", resourceIds);

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
          () -> {
            try {
              insertObject.execute();
              logger.atFine().log("Successfully inserted %s", resourceId);
            } catch (IOException ioe) {
              boolean canIgnoreException = false;
              try {
                canIgnoreException = canIgnoreExceptionForEmptyObject(ioe, resourceId, options);
              } catch (Exception e) {
                // Make sure to catch Exception instead of only IOException so that we can
                // correctly wrap other such exceptions and propagate them out cleanly inside
                // innerExceptions; common sources of non-IOExceptions include Preconditions
                // checks which get enforced at various layers in the library stack.
                innerExceptions.add(
                    new IOException("Error re-fetching after rate-limit error: " + resourceId, e));
              }
              if (canIgnoreException) {
                logger.atInfo().withCause(ioe).log(
                    "Ignoring exception; verified object already exists with desired state.");
              } else {
                innerExceptions.add(new IOException("Error inserting " + resourceId, ioe));
              }
            } catch (Exception e) {
              innerExceptions.add(new IOException("Error inserting " + resourceId, e));
            } finally {
              latch.countDown();
            }
          });
    }

    try {
      latch.await();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Failed to create empty objects", ie);
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
    logger.atFine().log("open(%s, %s)", resourceId, readOptions);
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);

    if (storageOptions.isGrpcEnabled()) {
      return new GoogleCloudStorageGrpcReadChannel(
          gcsGrpcStub, resourceId.getBucketName(), resourceId.getObjectName(), readOptions);
    }

    // The underlying channel doesn't initially read data, which means that we won't see a
    // FileNotFoundException until read is called. As a result, in order to find out if the object
    // exists, we'll need to do an RPC (metadata or data). A metadata check should be a less
    // expensive operation than a read data operation.
    GoogleCloudStorageItemInfo info;
    if (readOptions.getFastFailOnNotFound()) {
      info = getItemInfo(resourceId);
      if (!info.exists()) {
        throw createFileNotFoundException(
            resourceId.getBucketName(), resourceId.getObjectName(), /* cause= */ null);
      }
    } else {
      info = null;
    }

    return new GoogleCloudStorageReadChannel(
        gcs, resourceId, errorExtractor, clientRequestHelper, readOptions) {

      @Override
      @Nullable
      protected GoogleCloudStorageItemInfo getInitialMetadata() {
        return info;
      }

      @Override
      protected Storage.Objects.Get createRequest() throws IOException {
        return configureRequest(super.createRequest(), resourceId.getBucketName());
      }
    };
  }

  /** See {@link GoogleCloudStorage#deleteBuckets(List)} for details about expected behavior. */
  @Override
  public void deleteBuckets(List<String> bucketNames) throws IOException {
    logger.atFine().log("deleteBuckets(%s)", bucketNames);

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
      } catch (IOException e) {
        innerExceptions.add(
            errorExtractor.itemNotFound(e)
                ? createFileNotFoundException(bucketName, null, e)
                : new IOException(String.format("Error deleting '%s' bucket", bucketName), e));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Failed to delete buckets", e);
      }
    }
    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  public void deleteObject(StorageResourceId resourceId, long metaGeneration) throws IOException {
    String bucketName = resourceId.getBucketName();
    Storage.Objects.Delete deleteObject =
        configureRequest(gcs.objects().delete(bucketName, resourceId.getObjectName()), bucketName)
            .setIfMetagenerationMatch(metaGeneration);
    deleteObject.execute();
  }

  /** See {@link GoogleCloudStorage#deleteObjects(List)} for details about expected behavior. */
  @Override
  public void deleteObjects(List<StorageResourceId> fullObjectNames) throws IOException {
    logger.atFine().log("deleteObjects(%s)", fullObjectNames);

    if (fullObjectNames.isEmpty()) {
      return;
    }

    // Validate that all the elements represent StorageObjects.
    for (StorageResourceId fullObjectName : fullObjectNames) {
      Preconditions.checkArgument(
          fullObjectName.isStorageObject(),
          "Expected full StorageObject names only, got: %s", fullObjectName);
    }

    // Gather exceptions to wrap in a composite exception at the end.
    final KeySetView<IOException, Boolean> innerExceptions = ConcurrentHashMap.newKeySet();
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(
            httpRequestInitializer,
            gcs,
            storageOptions.getMaxRequestsPerBatch(),
            fullObjectNames.size(),
            storageOptions.getBatchThreads());

    for (StorageResourceId fullObjectName : fullObjectNames) {
      queueSingleObjectDelete(fullObjectName, innerExceptions, batchHelper, 1);
    }

    batchHelper.flush();

    if (!innerExceptions.isEmpty()) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  /** Helper to create a callback for a particular deletion request. */
  private JsonBatchCallback<Void> getDeletionCallback(
      final StorageResourceId resourceId,
      final KeySetView<IOException, Boolean> innerExceptions,
      final BatchHelper batchHelper,
      final int attempt,
      final long generation) {
    return new JsonBatchCallback<Void>() {
      @Override
      public void onSuccess(Void obj, HttpHeaders responseHeaders) {
        logger.atFine().log("Successfully deleted %s at generation %s", resourceId, generation);
      }

      @Override
      public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders)
          throws IOException {
        GoogleJsonResponseException cause = createJsonResponseException(jsonError, responseHeaders);
        if (errorExtractor.itemNotFound(cause)) {
          // Ignore item-not-found errors. We do not have to delete what we cannot find. This
          // error typically shows up when we make a request to delete something and the server
          // receives the request but we get a retry-able error before we get a response.
          // During a retry, we no longer find the item because the server had deleted
          // it already.
          logger.atFine().log("Delete object '%s' not found:%n%s", resourceId, jsonError);
        } else if (errorExtractor.preconditionNotMet(cause)
            && attempt <= MAXIMUM_PRECONDITION_FAILURES_IN_DELETE) {
          logger.atInfo().log(
              "Precondition not met while deleting '%s' at generation %s. Attempt %s."
                  + " Retrying:%n%s",
              resourceId, generation, attempt, jsonError);
          queueSingleObjectDelete(resourceId, innerExceptions, batchHelper, attempt + 1);
        } else {
          innerExceptions.add(
              new IOException(
                  String.format(
                      "Error deleting '%s', stage 2 with generation %s", resourceId, generation),
                  cause));
        }
      }
    };
  }

  private void queueSingleObjectDelete(
      final StorageResourceId resourceId,
      final KeySetView<IOException, Boolean> innerExceptions,
      final BatchHelper batchHelper,
      final int attempt)
      throws IOException {
    final String bucketName = resourceId.getBucketName();
    final String objectName = resourceId.getObjectName();

    if (resourceId.hasGenerationId()) {
      // We can go direct to the deletion request instead of first fetching generation id.
      long generationId = resourceId.getGenerationId();
      Storage.Objects.Delete deleteObject =
          configureRequest(gcs.objects().delete(bucketName, objectName), bucketName)
              .setIfGenerationMatch(generationId);
      batchHelper.queue(
          deleteObject,
          getDeletionCallback(resourceId, innerExceptions, batchHelper, attempt, generationId));
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
                      resourceId, innerExceptions, batchHelper, attempt, generation));
            }

            @Override
            public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
              GoogleJsonResponseException cause =
                  createJsonResponseException(jsonError, responseHeaders);
              if (errorExtractor.itemNotFound(cause)) {
                // If the item isn't found, treat it the same as if it's not found in the delete
                // case: assume the user wanted the object gone and now it is.
                logger.atFine().log("deleteObjects(%s): get not found:%n%s", resourceId, jsonError);
              } else {
                innerExceptions.add(
                    new IOException(
                        String.format("Error deleting '%s', stage 1", resourceId), cause));
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
  // TODO(b/120887495): This @VisibleForTesting annotation was being ignored by prod code.
  // Please check that removing it is correct, and remove this comment along with it.
  // @VisibleForTesting
  public static void validateCopyArguments(
      String srcBucketName,
      List<String> srcObjectNames,
      String dstBucketName,
      List<String> dstObjectNames,
      GoogleCloudStorage gcsImpl)
      throws IOException {
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
        throw new IllegalArgumentException(
            String.format(
                "Copy destination must be different from source for %s.",
                StringPaths.fromComponents(srcBucketName, srcObjectNames.get(i))));
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

    if (srcObjectNames.isEmpty()) {
      return;
    }

    // Gather FileNotFoundExceptions for individual objects,
    // but only throw a single combined exception at the end.
    KeySetView<IOException, Boolean> innerExceptions = ConcurrentHashMap.newKeySet();

    // Perform the copy operations.
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(
            httpRequestInitializer,
            gcs,
            storageOptions.getCopyMaxRequestsPerBatch(),
            srcObjectNames.size(),
            storageOptions.getCopyBatchThreads());

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
    if (storageOptions.getMaxBytesRewrittenPerCall() > 0) {
      rewriteObject.setMaxBytesRewrittenPerCall(storageOptions.getMaxBytesRewrittenPerCall());
    }

    // TODO(b/79750454) do not batch rewrite requests because they time out in batches.
    batchHelper.queue(
        rewriteObject,
        new JsonBatchCallback<RewriteResponse>() {
          @Override
          public void onSuccess(RewriteResponse rewriteResponse, HttpHeaders responseHeaders) {
            String srcString = StringPaths.fromComponents(srcBucketName, srcObjectName);
            String dstString = StringPaths.fromComponents(dstBucketName, dstObjectName);

            if (rewriteResponse.getDone()) {
              logger.atFine().log("Successfully copied %s to %s", srcString, dstString);
            } else {
              // If an object is very large, we need to continue making successive calls to
              // rewrite until the operation completes.
              logger.atFine().log(
                  "Copy (%s to %s) did not complete. Resuming...", srcString, dstString);
              try {
                Storage.Objects.Rewrite rewriteObjectWithToken =
                    configureRequest(
                        gcs.objects()
                            .rewrite(
                                srcBucketName, srcObjectName, dstBucketName, dstObjectName, null),
                        srcBucketName);
                if (storageOptions.getMaxBytesRewrittenPerCall() > 0) {
                  rewriteObjectWithToken.setMaxBytesRewrittenPerCall(
                      storageOptions.getMaxBytesRewrittenPerCall());
                }
                rewriteObjectWithToken.setRewriteToken(rewriteResponse.getRewriteToken());
                batchHelper.queue(rewriteObjectWithToken, this);
              } catch (IOException e) {
                innerExceptions.add(e);
              }
            }
          }

          @Override
          public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) {
            onCopyFailure(innerExceptions, e, responseHeaders, srcBucketName, srcObjectName);
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
            String srcString = StringPaths.fromComponents(srcBucketName, srcObjectName);
            String dstString = StringPaths.fromComponents(dstBucketName, dstObjectName);
            logger.atFine().log("Successfully copied %s to %s", srcString, dstString);
          }

          @Override
          public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
            onCopyFailure(
                innerExceptions, jsonError, responseHeaders, srcBucketName, srcObjectName);
          }
        });
  }

  /** Processes failed copy requests */
  private void onCopyFailure(
      KeySetView<IOException, Boolean> innerExceptions,
      GoogleJsonError jsonError,
      HttpHeaders responseHeaders,
      String srcBucketName,
      String srcObjectName) {
    GoogleJsonResponseException cause = createJsonResponseException(jsonError, responseHeaders);
    innerExceptions.add(
        errorExtractor.itemNotFound(cause)
            ? createFileNotFoundException(srcBucketName, srcObjectName, cause)
            : new IOException(
                String.format(
                    "Error copying '%s'", StringPaths.fromComponents(srcBucketName, srcObjectName)),
                cause));
  }

  /**
   * Shared helper for actually dispatching buckets().list() API calls and accumulating paginated
   * results; these can then be used to either extract just their names, or to parse into full
   * GoogleCloudStorageItemInfos.
   */
  private List<Bucket> listBucketsInternal() throws IOException {
    logger.atFine().log("listBucketsInternal()");
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
        logger.atFine().log("listBucketsInternal: next page %s", pageToken);
        listBucket.setPageToken(pageToken);
      }

      Buckets items = listBucket.execute();

      // Accumulate buckets (if any).
      List<Bucket> buckets = items.getItems();
      if (buckets != null) {
        logger.atFine().log("listed %s items", buckets.size());
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
    logger.atFine().log("listBucketNames()");
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
    logger.atFine().log("listBucketInfo()");
    List<Bucket> allBuckets = listBucketsInternal();
    List<GoogleCloudStorageItemInfo> bucketInfos = new ArrayList<>(allBuckets.size());
    for (Bucket bucket : allBuckets) {
      bucketInfos.add(
          new GoogleCloudStorageItemInfo(
              new StorageResourceId(bucket.getName()),
              bucket.getTimeCreated().getValue(),
              bucket.getUpdated().getValue(),
              /* size= */ 0,
              bucket.getLocation(),
              bucket.getStorageClass()));
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
    Map<String, String> rewrittenMetadata = encodeMetadata(createObjectOptions.getMetadata());
    StorageObject object =
        new StorageObject()
            .setName(resourceId.getObjectName())
            .setMetadata(rewrittenMetadata)
            .setContentEncoding(createObjectOptions.getContentEncoding());

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
    logger.atFine().log(
        "listStorageObjectsAndPrefixes(%s, %s, %s, %s, %d)",
        bucketName, objectNamePrefix, delimiter, includeTrailingDelimiter, maxResults);

    checkArgument(
        listedObjects != null && listedObjects.isEmpty(),
        "Must provide a non-null empty container for listedObjects.");
    checkArgument(
        listedPrefixes != null && listedPrefixes.isEmpty(),
        "Must provide a non-null empty container for listedPrefixes.");

    Storage.Objects.List listObject =
        createListRequest(
            bucketName, objectNamePrefix, delimiter, includeTrailingDelimiter, maxResults);

    String pageToken = null;
    do {
      if (pageToken != null) {
        logger.atFine().log("listStorageObjectsAndPrefixes: next page %s", pageToken);
        listObject.setPageToken(pageToken);
      }
      pageToken =
          listStorageObjectsAndPrefixesPage(listObject, maxResults, listedObjects, listedPrefixes);
    } while (pageToken != null
        && getMaxRemainingResults(maxResults, listedPrefixes, listedObjects) > 0);
  }

  private String listStorageObjectsAndPrefixesPage(
      Storage.Objects.List listObject,
      long maxResults,
      List<StorageObject> listedObjects,
      List<String> listedPrefixes)
      throws IOException {
    logger.atFine().log("listStorageObjectsAndPrefixesPage(%s, %d)", listObject, maxResults);

    checkNotNull(listedObjects, "Must provide a non-null container for listedObjects.");
    checkNotNull(listedPrefixes, "Must provide a non-null container for listedPrefixes.");

    // Deduplicate prefixes and items, because if 'includeTrailingDelimiter' set to true
    // then returned items will contain "prefix objects" too.
    Set<String> prefixes = new LinkedHashSet<>(listedPrefixes);

    Objects items;
    try {
      items = listObject.execute();
    } catch (IOException e) {
      String resource = StringPaths.fromComponents(listObject.getBucket(), listObject.getPrefix());
      if (errorExtractor.itemNotFound(e)) {
        logger.atFine().withCause(e).log(
            "listStorageObjectsAndPrefixesPage(%s, %d): item not found", resource, maxResults);
        return null;
      }
      throw new IOException("Error listing " + resource, e);
    }

    // Add prefixes (if any).
    List<String> pagePrefixes = items.getPrefixes();
    if (pagePrefixes != null) {
      logger.atFine().log("listed %s prefixes", pagePrefixes.size());
      long maxRemainingResults = getMaxRemainingResults(maxResults, prefixes, listedObjects);
      // Do not cast 'maxRemainingResults' to int here, it could overflow
      long maxPrefixes = Math.min(maxRemainingResults, (long) pagePrefixes.size());
      prefixes.addAll(pagePrefixes.subList(0, (int) maxPrefixes));
    }

    // Add object names (if any).
    List<StorageObject> objects = items.getItems();
    if (objects != null) {
      logger.atFine().log("listed %s objects", objects.size());

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
      String objectNamePrefix = listObject.getPrefix();
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

    listedPrefixes.clear();
    listedPrefixes.addAll(prefixes);

    return items.getNextPageToken();
  }

  private Storage.Objects.List createListRequest(
      String bucketName,
      String objectNamePrefix,
      String delimiter,
      boolean includeTrailingDelimiter,
      long maxResults)
      throws IOException {
    logger.atFine().log(
        "createListRequest(%s, %s, %s, %s, %d)",
        bucketName, objectNamePrefix, delimiter, includeTrailingDelimiter, maxResults);
    checkArgument(!Strings.isNullOrEmpty(bucketName), "bucketName must not be null or empty");

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

    return listObject;
  }

  private static long getMaxRemainingResults(
      long maxResults, Collection<String> prefixes, List<StorageObject> objects) {
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
    logger.atFine().log(
        "listObjectNames(%s, %s, %s, %s)", bucketName, objectNamePrefix, delimiter, maxResults);

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
   * See {@link GoogleCloudStorage#listObjectInfo(String, String, String)} for details about
   * expected behavior.
   */
  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter) throws IOException {
    return listObjectInfo(bucketName, objectNamePrefix, delimiter, MAX_RESULTS_UNLIMITED);
  }

  /**
   * See {@link GoogleCloudStorage#listObjectInfo(String, String, String, long)} for details about
   * expected behavior.
   */
  @Override
  public List<GoogleCloudStorageItemInfo> listObjectInfo(
      String bucketName, String objectNamePrefix, String delimiter, long maxResults)
      throws IOException {
    logger.atFine().log(
        "listObjectInfo(%s, %s, %s, %s)", bucketName, objectNamePrefix, delimiter, maxResults);

    // Helper will handle going through pages of list results and accumulating them.
    List<StorageObject> listedObjects = new ArrayList<>();
    List<String> listedPrefixes = new ArrayList<>();
    listStorageObjectsAndPrefixes(
        bucketName,
        objectNamePrefix,
        delimiter,
        /* includeTrailingDelimiter= */ true,
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

    if (listedPrefixes.isEmpty()) {
      return objectInfos;
    }

    handlePrefixes(bucketName, listedPrefixes, objectInfos);

    return objectInfos;
  }

  @Override
  public ListPage<GoogleCloudStorageItemInfo> listObjectInfoPage(
      String bucketName, String objectNamePrefix, String delimiter, String pageToken)
      throws IOException {
    logger.atFine().log(
        "listObjectInfoPage(%s, %s, %s, %s)", bucketName, objectNamePrefix, delimiter, pageToken);

    Storage.Objects.List listObject =
        createListRequest(
            bucketName,
            objectNamePrefix,
            delimiter,
            /* includeTrailingDelimiter= */ true,
            MAX_RESULTS_UNLIMITED);
    if (pageToken != null) {
      logger.atFine().log("listObjectInfoPage: next page %s", pageToken);
      listObject.setPageToken(pageToken);
    }

    // Helper will handle going through pages of list results and accumulating them.
    List<StorageObject> listedObjects = new ArrayList<>();
    List<String> listedPrefixes = new ArrayList<>();
    String nextPageToken =
        listStorageObjectsAndPrefixesPage(
            listObject, MAX_RESULTS_UNLIMITED, listedObjects, listedPrefixes);

    // For the listedObjects, we simply parse each item into a GoogleCloudStorageItemInfo without
    // further work.
    List<GoogleCloudStorageItemInfo> objectInfos = new ArrayList<>(listedObjects.size());
    for (StorageObject obj : listedObjects) {
      objectInfos.add(
          createItemInfoForStorageObject(new StorageResourceId(bucketName, obj.getName()), obj));
    }

    if (!listedPrefixes.isEmpty()) {
      handlePrefixes(bucketName, listedPrefixes, objectInfos);
    }

    return new ListPage<>(objectInfos, nextPageToken);
  }

  /** Handle prefixes without prefix objects. */
  private void handlePrefixes(
      String bucketName, List<String> prefixes, List<GoogleCloudStorageItemInfo> objectInfos) {
    if (storageOptions.isInferImplicitDirectoriesEnabled()) {
      for (String prefix : prefixes) {
        objectInfos.add(
            GoogleCloudStorageItemInfo.createInferredDirectory(
                new StorageResourceId(bucketName, prefix)));
      }
    } else {
      logger.atInfo().log(
          "Inferred directories are disabled, giving up on retrieving missing directories: %s",
          prefixes);
    }
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
    return new GoogleCloudStorageItemInfo(
        resourceId,
        bucket.getTimeCreated().getValue(),
        bucket.getUpdated().getValue(),
        /* size= */ 0,
        bucket.getLocation(),
        bucket.getStorageClass());
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
        "resourceId must be a StorageObject. resourceId: %s", resourceId);
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
        object.getTimeCreated().getValue(),
        object.getUpdated().getValue(),
        object.getSize().longValue(),
        /* location= */ null,
        /* storageClass= */ null,
        object.getContentType(),
        object.getContentEncoding(),
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
    return Maps.transformValues(metadata, GoogleCloudStorageImpl::encodeMetadataValues);
  }

  /**
   * Inverse function of {@link #encodeMetadata(Map)}.
   */
  @VisibleForTesting
  static Map<String, byte[]> decodeMetadata(Map<String, String> metadata) {
    return Maps.transformValues(metadata, GoogleCloudStorageImpl::decodeMetadataValues);
  }

  /** See {@link GoogleCloudStorage#getItemInfos(List)} for details about expected behavior. */
  @Override
  public List<GoogleCloudStorageItemInfo> getItemInfos(List<StorageResourceId> resourceIds)
      throws IOException {
    logger.atFine().log("getItemInfos(%s)", resourceIds);

    if (resourceIds.isEmpty()) {
      return new ArrayList<>();
    }

    final Map<StorageResourceId, GoogleCloudStorageItemInfo> itemInfos = new ConcurrentHashMap<>();
    final Set<IOException> innerExceptions = newConcurrentHashSet();
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(
            httpRequestInitializer,
            gcs,
            storageOptions.getMaxRequestsPerBatch(),
            resourceIds.size(),
            storageOptions.getBatchThreads());

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
                logger.atFine().log(
                    "getItemInfos: Successfully fetched bucket: %s for resourceId: %s",
                    bucket, resourceId);
                itemInfos.put(resourceId, createItemInfoForBucket(resourceId, bucket));
              }

              @Override
              public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
                GoogleJsonResponseException cause =
                    createJsonResponseException(jsonError, responseHeaders);
                if (errorExtractor.itemNotFound(cause)) {
                  logger.atFine().log(
                      "getItemInfos: bucket '%s' not found:%n%s",
                      resourceId.getBucketName(), jsonError);
                  itemInfos.put(resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
                } else {
                  innerExceptions.add(
                      new IOException(
                          String.format("Error getting '%s' bucket", resourceId.getBucketName()),
                          cause));
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
                logger.atFine().log(
                    "getItemInfos: Successfully fetched object '%s' for resourceId '%s'",
                    obj, resourceId);
                itemInfos.put(resourceId, createItemInfoForStorageObject(resourceId, obj));
              }

              @Override
              public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
                GoogleJsonResponseException cause =
                    createJsonResponseException(jsonError, responseHeaders);
                if (errorExtractor.itemNotFound(cause)) {
                  logger.atFine().log(
                      "getItemInfos: object '%s' not found:%n%s", resourceId, jsonError);
                  itemInfos.put(resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
                } else {
                  innerExceptions.add(
                      new IOException(
                          String.format("Error getting '%s' object", resourceId), cause));
                }
              }
            });
      }
    }

    batchHelper.flush();

    if (!innerExceptions.isEmpty()) {
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
    logger.atFine().log("updateItems(%s)", itemInfoList);

    if (itemInfoList.isEmpty()) {
      return new ArrayList<>();
    }

    final Map<StorageResourceId, GoogleCloudStorageItemInfo> resultItemInfos =
        new ConcurrentHashMap<>();
    final Set<IOException> innerExceptions = newConcurrentHashSet();
    BatchHelper batchHelper =
        batchFactory.newBatchHelper(
            httpRequestInitializer,
            gcs,
            storageOptions.getMaxRequestsPerBatch(),
            itemInfoList.size(),
            storageOptions.getBatchThreads());

    for (UpdatableItemInfo itemInfo : itemInfoList) {
      Preconditions.checkArgument(
          !itemInfo.getStorageResourceId().isBucket() && !itemInfo.getStorageResourceId().isRoot(),
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
              logger.atFine().log(
                  "updateItems: Successfully updated object '%s' for resourceId '%s'",
                  obj, resourceId);
              resultItemInfos.put(resourceId, createItemInfoForStorageObject(resourceId, obj));
            }

            @Override
            public void onFailure(GoogleJsonError jsonError, HttpHeaders responseHeaders) {
              GoogleJsonResponseException cause =
                  createJsonResponseException(jsonError, responseHeaders);
              if (errorExtractor.itemNotFound(cause)) {
                logger.atFine().log("updateItems: object not found %s:%n%s", resourceId, jsonError);
                resultItemInfos.put(
                    resourceId, GoogleCloudStorageItemInfo.createNotFound(resourceId));
              } else {
                innerExceptions.add(
                    new IOException(
                        String.format("Error updating '%s' object", resourceId), cause));
              }
            }
          });
    }
    batchHelper.flush();

    if (!innerExceptions.isEmpty()) {
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
    logger.atFine().log("getItemInfo(%s)", resourceId);

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
    logger.atFine().log("getItemInfo: %s", itemInfo);
    return itemInfo;
  }

  /**
   * See {@link GoogleCloudStorage#close()} for details about expected behavior.
   */
  @Override
  public void close() {
    // Calling shutdown() is a no-op if it was already called earlier,
    // therefore no need to guard against that by setting threadPool to null.
    logger.atFine().log("close()");
    try {
      backgroundTasksThreadPool.shutdown();
      manualBatchingThreadPool.shutdown();
    } finally {
      backgroundTasksThreadPool = null;
      manualBatchingThreadPool = null;
    }
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
    logger.atFine().log("getBucket(%s)", bucketName);
    checkArgument(!Strings.isNullOrEmpty(bucketName), "bucketName must not be null or empty");
    Storage.Buckets.Get getBucket = configureRequest(gcs.buckets().get(bucketName), bucketName);
    try {
      return getBucket.execute();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        logger.atFine().withCause(e).log("getBucket(%s): not found", bucketName);
        return null;
      }
      throw new IOException("Error accessing Bucket " + bucketName, e);
    }
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
    logger.atFine().log("getWriteGeneration(%s, %s)", resourceId, overwritable);
    GoogleCloudStorageItemInfo info = getItemInfo(resourceId);
    if (!info.exists()) {
      return 0L;
    }
    if (info.exists() && overwritable) {
      long generation = info.getContentGeneration();
      Preconditions.checkState(generation != 0, "Generation should not be 0 for an existing item");
      return generation;
    }
    throw new FileAlreadyExistsException(String.format("Object %s already exists.", resourceId));
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
    logger.atFine().log("getObject(%s)", resourceId);
    Preconditions.checkArgument(
        resourceId.isStorageObject(), "Expected full StorageObject id, got %s", resourceId);
    String bucketName = resourceId.getBucketName();
    String objectName = resourceId.getObjectName();
    Storage.Objects.Get getObject =
        configureRequest(gcs.objects().get(bucketName, objectName), bucketName);
    try {
      return getObject.execute();
    } catch (IOException e) {
      if (errorExtractor.itemNotFound(e)) {
        logger.atFine().withCause(e).log("getObject(%s): not found", resourceId);
        return null;
      }
      throw new IOException("Error accessing " + resourceId, e);
    }
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
        || errorExtractor.internalServerError(exceptionOnCreate)) {
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

  @Override
  public void compose(
      final String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    logger.atFine().log("compose(%s, %s, %s, %s)", bucketName, sources, destination, contentType);
    List<StorageResourceId> sourceIds =
        Lists.transform(sources, objectName -> new StorageResourceId(bucketName, objectName));
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
    logger.atFine().log("composeObjects(%s, %s, %s)", sources, destination, options);
    for (StorageResourceId inputId : sources) {
      if (!destination.getBucketName().equals(inputId.getBucketName())) {
        throw new IOException(String.format(
            "Bucket doesn't match for source '%s' and destination '%s'!", inputId, destination));
      }
    }
    List<ComposeRequest.SourceObjects> sourceObjects =
        Lists.transform(
            // TODO(user): Maybe set generationIds for source objects as well here.
            sources, input -> new ComposeRequest.SourceObjects().setName(input.getObjectName()));
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
                                .setContentEncoding(options.getContentEncoding())
                                .setMetadata(encodeMetadata(options.getMetadata())))),
            destination.getBucketName());

    compose.setIfGenerationMatch(
        destination.hasGenerationId()
            ? destination.getGenerationId()
            : getWriteGeneration(destination, true));

    logger.atFine().log("composeObjects.execute()");
    GoogleCloudStorageItemInfo compositeInfo =
        createItemInfoForStorageObject(destination, compose.execute());
    logger.atFine().log("composeObjects() done, returning: %s", compositeInfo);
    return compositeInfo;
  }

  <RequestT extends StorageRequest<?>> RequestT configureRequest(
      RequestT request, String bucketName) {
    setRequesterPaysProject(request, bucketName);

    if (request instanceof Storage.Objects.Get || request instanceof Storage.Objects.Insert) {
      setEncryptionHeaders(request);
    }

    if (request instanceof Storage.Objects.Rewrite || request instanceof Storage.Objects.Copy) {
      setEncryptionHeaders(request);
      setDecryptionHeaders(request);
    }

    return request;
  }

  private <RequestT extends StorageRequest<?>> void setEncryptionHeaders(RequestT request) {
    if (storageOptions.getEncryptionKey() == null) {
      return;
    }

    request
        .getRequestHeaders()
        .set(
            "x-goog-encryption-algorithm",
            checkNotNull(
                storageOptions.getEncryptionAlgorithm(), "encryption algorithm must not be null"))
        .set(
            "x-goog-encryption-key",
            checkNotNull(storageOptions.getEncryptionKey(), "encryption key must not be null")
                .value())
        .set(
            "x-goog-encryption-key-sha256",
            checkNotNull(
                    storageOptions.getEncryptionKeyHash(), "encryption key hash must not be null")
                .value());
  }

  private <RequestT extends StorageRequest<?>> void setDecryptionHeaders(RequestT request) {
    if (storageOptions.getEncryptionKey() == null) {
      return;
    }

    request
        .getRequestHeaders()
        .set(
            "x-goog-copy-source-encryption-algorithm",
            checkNotNull(
                storageOptions.getEncryptionAlgorithm(), "encryption algorithm must not be null"))
        .set(
            "x-goog-copy-source-encryption-key",
            checkNotNull(storageOptions.getEncryptionKey(), "encryption key must not be null")
                .value())
        .set(
            "x-goog-copy-source-encryption-key-sha256",
            checkNotNull(
                    storageOptions.getEncryptionKeyHash(), "encryption key hash must not be null")
                .value());
  }

  private <RequestT extends StorageRequest<?>> void setRequesterPaysProject(
      RequestT request, String bucketName) {
    if (requesterShouldPay(bucketName)) {
      setUserProject(request, storageOptions.getRequesterPaysOptions().getProjectId());
    }
  }

  private boolean requesterShouldPay(String bucketName) {
    if (bucketName == null) {
      return false;
    }

    switch (storageOptions.getRequesterPaysOptions().getMode()) {
      case ENABLED:
        return true;
      case CUSTOM:
        return storageOptions.getRequesterPaysOptions().getBuckets().contains(bucketName);
      case AUTO:
        return autoBuckets.getUnchecked(bucketName);
      default:
        return false;
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
