/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio.cooplock;

import static com.google.cloud.hadoop.gcsio.CreateObjectOptions.EMPTY_METADATA;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao.LOCK_DIRECTORY;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.client.util.ExponentialBackOff;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.UriPaths;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * DAO class for operation lock metadata (persisted in {@code gs://<BUCKET>/_lock/<OPERATION>.lock}
 * file) and operation logs (persisted in {@code gs://<BUCKET>/_lock/<OPERATION>.log} file)
 */
public class CoopLockOperationDao {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final String OPERATION_LOG_FILE_FORMAT = "%s_%s_%s.log";
  private static final String OPERATION_LOCK_FILE_FORMAT = "%s_%s_%s.lock";

  private static final CreateObjectOptions CREATE_OBJECT_OPTIONS =
      new CreateObjectOptions(/* overwriteExisting= */ false, "application/text", EMPTY_METADATA);
  private static final CreateObjectOptions UPDATE_OBJECT_OPTIONS =
      new CreateObjectOptions(/* overwriteExisting= */ true, "application/text", EMPTY_METADATA);

  private static final int LOCK_MODIFY_RETRY_BACK_OFF_MILLIS = 1_100;

  private static final int MAX_LOCK_RENEW_TIMEOUT_MILLIS = LOCK_MODIFY_RETRY_BACK_OFF_MILLIS * 10;

  private static final DateTimeFormatter LOCK_FILE_DATE_TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSXXX").withZone(ZoneOffset.UTC);

  private static final Gson GSON = CoopLockRecordsDao.createGson();

  private final ScheduledExecutorService scheduledThreadPool =
      Executors.newScheduledThreadPool(
          /* corePoolSize= */ 0,
          new ThreadFactoryBuilder().setNameFormat("coop-lock-thread-%d").setDaemon(true).build());

  private final GoogleCloudStorage gcs;
  private final CooperativeLockingOptions options;

  public CoopLockOperationDao(GoogleCloudStorage gcs) {
    this.gcs = gcs;
    this.options = gcs.getOptions().getCooperativeLockingOptions();
  }

  public Future<?> persistDeleteOperation(
      String operationId,
      Instant operationInstant,
      StorageResourceId resourceId,
      List<FileInfo> itemsToDelete,
      List<FileInfo> bucketsToDelete)
      throws IOException {
    URI operationLockPath =
        writeOperationFile(
            resourceId.getBucketName(),
            OPERATION_LOCK_FILE_FORMAT,
            CREATE_OBJECT_OPTIONS,
            CoopLockOperationType.DELETE,
            operationId,
            operationInstant,
            ImmutableList.of(
                GSON.toJson(
                    new DeleteOperation()
                        .setLockExpiration(
                            Instant.now().plusMillis(options.getLockExpirationTimeoutMilli()))
                        .setResource(resourceId.toString()))));
    List<String> logRecords =
        Streams.concat(itemsToDelete.stream(), bucketsToDelete.stream())
            .map(i -> i.getItemInfo().getResourceId().toString())
            .collect(toImmutableList());
    writeOperationFile(
        resourceId.getBucketName(),
        OPERATION_LOG_FILE_FORMAT,
        CREATE_OBJECT_OPTIONS,
        CoopLockOperationType.DELETE,
        operationId,
        operationInstant,
        logRecords);
    // Schedule lock expiration update
    return scheduleLockUpdate(
        operationId,
        operationLockPath,
        DeleteOperation.class,
        (operation, updateInstant) ->
            operation.setLockExpiration(
                updateInstant.plusMillis(options.getLockExpirationTimeoutMilli())));
  }

  public Future<?> persistRenameOperation(
      String operationId,
      Instant operationInstant,
      StorageResourceId src,
      StorageResourceId dst,
      Map<FileInfo, URI> srcToDstItemNames,
      Map<FileInfo, URI> srcToDstMarkerItemNames)
      throws IOException {
    URI operationLockPath =
        writeOperationFile(
            dst.getBucketName(),
            OPERATION_LOCK_FILE_FORMAT,
            CREATE_OBJECT_OPTIONS,
            CoopLockOperationType.RENAME,
            operationId,
            operationInstant,
            ImmutableList.of(
                GSON.toJson(
                    new RenameOperation()
                        .setLockExpiration(
                            Instant.now().plusMillis(options.getLockExpirationTimeoutMilli()))
                        .setSrcResource(src.toString())
                        .setDstResource(dst.toString())
                        .setCopySucceeded(false))));
    List<String> logRecords =
        Streams.concat(
                srcToDstItemNames.entrySet().stream(), srcToDstMarkerItemNames.entrySet().stream())
            .map(e -> GSON.toJson(toRenameOperationLogRecord(e)))
            .collect(toImmutableList());
    writeOperationFile(
        dst.getBucketName(),
        OPERATION_LOG_FILE_FORMAT,
        CREATE_OBJECT_OPTIONS,
        CoopLockOperationType.RENAME,
        operationId,
        operationInstant,
        logRecords);
    // Schedule lock expiration update
    return scheduleLockUpdate(
        operationId,
        operationLockPath,
        RenameOperation.class,
        (o, updateInstant) ->
            o.setLockExpiration(updateInstant.plusMillis(options.getLockExpirationTimeoutMilli())));
  }

  public void checkpointRenameOperation(
      String bucketName, String operationId, Instant operationInstant, boolean copySucceeded)
      throws IOException {
    URI operationLockPath =
        getOperationFilePath(
            bucketName,
            OPERATION_LOCK_FILE_FORMAT,
            CoopLockOperationType.RENAME,
            operationId,
            operationInstant);
    ExponentialBackOff backOff = newLockModifyBackoff();
    for (int i = 0; i < 10; i++) {
      try {
        modifyOperationLock(
            operationId,
            operationLockPath,
            l -> {
              RenameOperation operation = GSON.fromJson(l, RenameOperation.class);
              operation
                  .setLockExpiration(
                      Instant.now().plusMillis(options.getLockExpirationTimeoutMilli()))
                  .setCopySucceeded(copySucceeded);
              return GSON.toJson(operation);
            });
        return;
      } catch (IOException e) {
        logger.atWarning().withCause(e).log(
            "Failed to checkpoint '%s' lock for %s operation, attempt #%d",
            operationLockPath, operationId, i + 1);
        sleepUninterruptibly(Duration.ofMillis(backOff.nextBackOffMillis()));
      }
    }
    throw new IOException(
        String.format(
            "Failed to checkpoint '%s' lock for %s operation", operationLockPath, operationId));
  }

  private void renewLockOrExit(
      String operationId,
      URI operationLockPath,
      Function<String, String> renewFn,
      Duration timeout) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    AtomicBoolean renewalSucceeded = new AtomicBoolean(false);
    ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
    Future<?> timeoutFuture =
        timeoutExecutor.schedule(
            () -> {
              if (renewalSucceeded.get()) {
                return;
              }
              logger.atSevere().log(
                  "Renewal of '%s' lock for %s operation timed out in %s, exiting",
                  operationLockPath, operationId, timeout);
              System.exit(1);
            },
            timeout.toMillis(),
            MILLISECONDS);

    int attempt = 1;
    ExponentialBackOff backoff = newLockModifyBackoff();
    try {
      do {
        try {
          logger.atFine().log(
              "Renewing '%s' lock for %s operation with %s timeout after %s, attempt %d",
              operationLockPath, operationId, timeout, stopwatch.elapsed(), attempt);
          modifyOperationLock(operationId, operationLockPath, renewFn);
          renewalSucceeded.set(true);
          return;
        } catch (IOException e) {
          logger.atWarning().withCause(e).log(
              "Failed to renew '%s' lock for %s operation with %s timeout after %s, attempt #%d",
              operationLockPath, operationId, timeout, stopwatch.elapsed(), attempt++);
        }
        sleepUninterruptibly(Duration.ofMillis(backoff.nextBackOffMillis()));
      } while (timeout.compareTo(stopwatch.elapsed()) > 0);
      logger.atSevere().log(
          "Renewal of '%s' lock for %s operation with %s timeout in %s, exiting",
          operationLockPath, operationId, timeout, stopwatch.elapsed());
      System.exit(1);
    } catch (Exception e) {
      logger.atSevere().withCause(e).log(
          "Failed to renew '%s' lock for %s operation with %s timeout in %s, exiting",
          operationLockPath, operationId, timeout, stopwatch.elapsed());
      System.exit(1);
    } finally {
      timeoutFuture.cancel(/* mayInterruptIfRunning= */ true);
      timeoutExecutor.shutdownNow();
    }
    System.exit(1);
  }

  private void modifyOperationLock(
      String operationId, URI operationLockPath, Function<String, String> modifyFn)
      throws IOException {
    StorageResourceId lockId =
        StorageResourceId.fromUriPath(operationLockPath, /* allowEmptyObjectName= */ false);
    GoogleCloudStorageItemInfo lockInfo = gcs.getItemInfo(lockId);
    checkState(lockInfo.exists(), "lock file for %s operation should exist", operationId);

    String lock;
    try (BufferedReader reader =
        new BufferedReader(Channels.newReader(gcs.open(lockId), UTF_8.name()))) {
      lock = reader.lines().collect(Collectors.joining());
    }

    lock = modifyFn.apply(lock);
    StorageResourceId lockIdWithGeneration =
        new StorageResourceId(
            lockId.getBucketName(), lockId.getObjectName(), lockInfo.getContentGeneration());
    writeOperation(lockIdWithGeneration, UPDATE_OBJECT_OPTIONS, ImmutableList.of(lock));
  }

  private URI writeOperationFile(
      String bucket,
      String fileNameFormat,
      CreateObjectOptions createObjectOptions,
      CoopLockOperationType operationType,
      String operationId,
      Instant operationInstant,
      List<String> records)
      throws IOException {
    URI path =
        getOperationFilePath(bucket, fileNameFormat, operationType, operationId, operationInstant);
    StorageResourceId resourceId =
        StorageResourceId.fromUriPath(path, /* allowEmptyObjectName= */ false);
    writeOperation(resourceId, createObjectOptions, records);
    return path;
  }

  private URI getOperationFilePath(
      String bucket,
      String fileNameFormat,
      CoopLockOperationType operationType,
      String operationId,
      Instant operationInstant) {
    String date = LOCK_FILE_DATE_TIME_FORMAT.format(operationInstant);
    String file = String.format(LOCK_DIRECTORY + fileNameFormat, date, operationType, operationId);
    return UriPaths.fromStringPathComponents(bucket, file, /* allowEmptyObjectName= */ false);
  }

  private void writeOperation(
      StorageResourceId resourceId, CreateObjectOptions createObjectOptions, List<String> records)
      throws IOException {
    try (WritableByteChannel channel = gcs.create(resourceId, createObjectOptions)) {
      for (String record : records) {
        channel.write(ByteBuffer.wrap(record.getBytes(UTF_8)));
        channel.write(ByteBuffer.wrap(new byte[] {'\n'}));
      }
    }
  }

  public <T> Future<?> scheduleLockUpdate(
      String operationId, URI operationLockPath, Class<T> clazz, BiConsumer<T, Instant> renewFn) {
    long lockRenewalPeriodMilli = options.getLockExpirationTimeoutMilli() / 2;
    long lockRenewTimeoutMilli =
        Math.min(options.getLockExpirationTimeoutMilli() / 4, MAX_LOCK_RENEW_TIMEOUT_MILLIS);
    return scheduledThreadPool.scheduleAtFixedRate(
        () ->
            renewLockOrExit(
                operationId,
                operationLockPath,
                l -> {
                  T operation = GSON.fromJson(l, clazz);
                  renewFn.accept(operation, Instant.now());
                  return GSON.toJson(operation);
                },
                Duration.ofMillis(lockRenewTimeoutMilli)),
        /* initialDelay= */ lockRenewalPeriodMilli,
        /* period= */ lockRenewalPeriodMilli,
        MILLISECONDS);
  }

  private static RenameOperationLogRecord toRenameOperationLogRecord(
      Map.Entry<FileInfo, URI> record) {
    return new RenameOperationLogRecord()
        .setSrc(record.getKey().getItemInfo().getResourceId().toString())
        .setDst(record.getValue().toString());
  }

  private static ExponentialBackOff newLockModifyBackoff() {
    return new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(LOCK_MODIFY_RETRY_BACK_OFF_MILLIS)
        .setMultiplier(1.1)
        .setRandomizationFactor(0.2)
        .setMaxIntervalMillis((int) (LOCK_MODIFY_RETRY_BACK_OFF_MILLIS * 1.25))
        .setMaxElapsedTimeMillis(Integer.MAX_VALUE)
        .build();
  }
}
