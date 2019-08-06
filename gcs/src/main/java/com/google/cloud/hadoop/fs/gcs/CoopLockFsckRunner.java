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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_COOPERATIVE_LOCKING_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_COOPERATIVE_LOCKING_EXPIRATION_TIMEOUT_MS;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationDao.RENAME_LOG_RECORD_SEPARATOR;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationDao;
import com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecord;
import com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao;
import com.google.cloud.hadoop.gcsio.cooplock.DeleteOperation;
import com.google.cloud.hadoop.gcsio.cooplock.RenameOperation;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.flogger.GoogleLogger;
import com.google.common.io.ByteSource;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Cooperative locking FSCK tool runner that contains logic for all FSCK commands */
class CoopLockFsckRunner {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final Gson GSON = new Gson();

  private static final Splitter RENAME_LOG_RECORD_SPLITTER =
      Splitter.on(RENAME_LOG_RECORD_SEPARATOR);

  private final Instant operationExpirationInstant = Instant.now();

  private final Configuration conf;
  private final String bucketName;
  private final String command;

  private final GoogleHadoopFileSystem ghfs;
  private final GoogleCloudStorageFileSystem gcsFs;
  private final GoogleCloudStorageImpl gcs;
  private final CoopLockRecordsDao lockRecordsDao;
  private final CoopLockOperationDao lockOperationDao;

  public CoopLockFsckRunner(Configuration conf, URI bucketUri, String command) throws IOException {
    // Disable cooperative locking to prevent blocking
    conf.setBoolean(GCS_COOPERATIVE_LOCKING_ENABLE.getKey(), false);

    this.conf = conf;
    this.bucketName = bucketUri.getAuthority();
    this.command = command;

    this.ghfs = (GoogleHadoopFileSystem) FileSystem.get(bucketUri, conf);
    this.gcsFs = ghfs.getGcsFs();
    this.gcs = (GoogleCloudStorageImpl) gcsFs.getGcs();
    this.lockRecordsDao = new CoopLockRecordsDao(gcs);
    this.lockOperationDao = new CoopLockOperationDao(gcs, gcsFs.getPathCodec());
  }

  public int run() throws IOException {
    Set<CoopLockRecord> lockedOperations = lockRecordsDao.getLockedOperations(bucketName);
    if (lockedOperations.isEmpty()) {
      logger.atInfo().log("No expired operation locks");
      return 0;
    }

    Map<FileStatus, CoopLockRecord> expiredOperations =
        lockedOperations.stream()
            .map(this::getOperationLockIfExpiredUnchecked)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (CoopLockFsck.COMMAND_CHECK.equals(command)) {
      return 0;
    }

    Function<Map.Entry<FileStatus, CoopLockRecord>, Boolean> operationRecovery =
        expiredOperation -> {
          FileStatus operationStatus = expiredOperation.getKey();
          CoopLockRecord operation = expiredOperation.getValue();
          String operationId = getOperationId(operationStatus);
          try {
            switch (operation.getOperationType()) {
              case DELETE:
                repairDeleteOperation(operationStatus, operation, operationId);
                break;
              case RENAME:
                repairRenameOperation(operationStatus, operation, operationId);
                break;
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to recover operation: " + operation, e);
          }
          return true;
        };

    for (Map.Entry<FileStatus, CoopLockRecord> expiredOperation : expiredOperations.entrySet()) {
      long start = System.currentTimeMillis();
      try {
        boolean succeeded = operationRecovery.apply(expiredOperation);
        long finish = System.currentTimeMillis();
        if (succeeded) {
          logger.atInfo().log(
              "Operation %s successfully %s in %dms",
              expiredOperation,
              CoopLockFsck.COMMAND_ROLL_FORWARD.equals(command) ? "rolled forward" : "rolled back",
              finish - start);
        } else {
          logger.atSevere().log(
              "Operation %s failed to %s in %dms",
              expiredOperation,
              CoopLockFsck.COMMAND_ROLL_FORWARD.equals(command) ? "rolled forward" : "rolled back",
              finish - start);
        }
      } catch (Exception e) {
        long finish = System.currentTimeMillis();
        logger.atSevere().withCause(e).log(
            "Operation %s failed to roll forward in %dms", expiredOperation, finish - start);
      }
    }
    return 0;
  }

  private void repairDeleteOperation(
      FileStatus operationStatus, CoopLockRecord operation, String operationId)
      throws IOException, URISyntaxException {
    if (CoopLockFsck.COMMAND_ROLL_BACK.equals(command)) {
      logger.atInfo().log(
          "Rolling back delete operations (%s) not supported, skipping.",
          operationStatus.getPath());
    } else {
      logger.atInfo().log("Repairing FS after %s delete operation.", operationStatus.getPath());
      DeleteOperation operationObject = getOperationObject(operationStatus, DeleteOperation.class);
      lockRecordsDao.relockOperation(
          bucketName, operationId, operation.getClientId(), operation.getLockEpochMilli());
      Future<?> lockUpdateFuture =
          lockOperationDao.scheduleLockUpdate(
              operationId,
              new URI(operationStatus.getPath().toString()),
              DeleteOperation.class,
              (o, i) -> o.setLockEpochMilli(i.toEpochMilli()));
      try {
        List<String> loggedResources = getOperationLog(operationStatus, l -> l);
        deleteResource(operationObject.getResource(), loggedResources);
        lockRecordsDao.unlockPaths(
            operationId, StorageResourceId.fromObjectName(operationObject.getResource()));
      } finally {
        lockUpdateFuture.cancel(/* mayInterruptIfRunning= */ true);
      }
    }
  }

  private void repairRenameOperation(
      FileStatus operationStatus, CoopLockRecord operation, String operationId)
      throws IOException, URISyntaxException {
    RenameOperation operationObject = getOperationObject(operationStatus, RenameOperation.class);
    lockRecordsDao.relockOperation(
        bucketName, operationId, operation.getClientId(), operation.getLockEpochMilli());
    Future<?> lockUpdateFuture =
        lockOperationDao.scheduleLockUpdate(
            operationId,
            new URI(operationStatus.getPath().toString()),
            RenameOperation.class,
            (o, i) -> o.setLockEpochMilli(i.toEpochMilli()));
    try {
      LinkedHashMap<String, String> loggedResources =
          getOperationLog(
                  operationStatus,
                  l -> {
                    List<String> srcToDst = RENAME_LOG_RECORD_SPLITTER.splitToList(l);
                    checkState(srcToDst.size() == 2);
                    return new AbstractMap.SimpleEntry<>(srcToDst.get(0), srcToDst.get(1));
                  })
              .stream()
              .collect(
                  toMap(
                      AbstractMap.SimpleEntry::getKey,
                      AbstractMap.SimpleEntry::getValue,
                      (e1, e2) -> {
                        throw new RuntimeException(
                            String.format("Found entries with duplicate keys: %s and %s", e1, e2));
                      },
                      LinkedHashMap::new));
      if (operationObject.getCopySucceeded()) {
        if (CoopLockFsck.COMMAND_ROLL_BACK.equals(command)) {
          deleteAndRenameToRepairRenameOperation(
              operationStatus,
              operation,
              operationObject,
              operationObject.getDstResource(),
              new ArrayList<>(loggedResources.values()),
              operationObject.getSrcResource(),
              "source",
              new ArrayList<>(loggedResources.keySet()),
              /* copySucceeded= */ false);
        } else {
          deleteToRepairRenameOperation(
              operationStatus,
              operationObject.getSrcResource(),
              "source",
              loggedResources.keySet());
        }
      } else {
        if (CoopLockFsck.COMMAND_ROLL_BACK.equals(command)) {
          deleteToRepairRenameOperation(
              operationStatus,
              operationObject.getDstResource(),
              "destination",
              loggedResources.values());
        } else {
          deleteAndRenameToRepairRenameOperation(
              operationStatus,
              operation,
              operationObject,
              operationObject.getSrcResource(),
              new ArrayList<>(loggedResources.keySet()),
              operationObject.getDstResource(),
              "destination",
              new ArrayList<>(loggedResources.values()),
              /* copySucceeded= */ true);
        }
      }
      lockRecordsDao.unlockPaths(
          operationId,
          StorageResourceId.fromObjectName(operationObject.getSrcResource()),
          StorageResourceId.fromObjectName(operationObject.getDstResource()));
    } finally {
      lockUpdateFuture.cancel(/* mayInterruptIfRunning= */ true);
    }
  }

  private void deleteToRepairRenameOperation(
      FileStatus operationLock,
      String operationResource,
      String deleteResourceType,
      Collection<String> loggedResources)
      throws IOException {
    logger.atInfo().log(
        "Repairing FS after %s rename operation (deleting %s (%s)).",
        operationLock.getPath(), deleteResourceType, operationResource);
    deleteResource(operationResource, loggedResources);
  }

  private void deleteAndRenameToRepairRenameOperation(
      FileStatus operationLock,
      CoopLockRecord operation,
      RenameOperation operationObject,
      String srcResource,
      List<String> loggedSrcResources,
      String dstResource,
      String dstResourceType,
      List<String> loggedDstResources,
      boolean copySucceeded)
      throws IOException {
    logger.atInfo().log(
        "Repairing FS after %s rename operation (deleting %s (%s) and renaming (%s -> %s)).",
        operationLock.getPath(), dstResourceType, dstResource, srcResource, dstResource);
    deleteResource(dstResource, loggedDstResources);
    gcs.copy(bucketName, toNames(loggedSrcResources), bucketName, toNames(loggedDstResources));

    // Update rename operation checkpoint before proceeding to allow repair of failed repair
    lockOperationDao.checkpointRenameOperation(
        StorageResourceId.fromObjectName(operationObject.getSrcResource()),
        StorageResourceId.fromObjectName(operationObject.getDstResource()),
        operation.getOperationId(),
        Instant.ofEpochMilli(operation.getOperationEpochMilli()),
        copySucceeded);

    deleteResource(srcResource, loggedSrcResources);
  }

  private static List<String> toNames(List<String> resources) {
    return resources.stream()
        .map(r -> StorageResourceId.fromObjectName(r).getObjectName())
        .collect(toList());
  }

  private Optional<Map.Entry<FileStatus, CoopLockRecord>> getOperationLockIfExpiredUnchecked(
      CoopLockRecord operation) {
    try {
      return getOperationLockIfExpired(bucketName, operation);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to check if %s operation expired", operation), e);
    }
  }

  private Optional<Map.Entry<FileStatus, CoopLockRecord>> getOperationLockIfExpired(
      String bucketName, CoopLockRecord operation) throws IOException {
    String operationId = operation.getOperationId();
    String globPath = CoopLockRecordsDao.LOCK_DIRECTORY + "*" + operationId + "*.lock";
    URI globUri =
        gcsFs.getPathCodec().getPath(bucketName, globPath, /* allowEmptyObjectName= */ false);
    FileStatus[] operationLocks = ghfs.globStatus(new Path(globUri));
    checkState(
        operationLocks.length < 2,
        "operation %s should not have more than one lock file",
        operationId);

    // Lock file not created - nothing to repair
    if (operationLocks.length == 0) {
      logger.atInfo().log(
          "Operation %s for %s resources doesn't have lock file, unlocking",
          operation.getOperationId(), operation.getResources());
      StorageResourceId[] lockedResources =
          operation.getResources().stream()
              .map(resource -> new StorageResourceId(bucketName, resource))
              .toArray(StorageResourceId[]::new);
      lockRecordsDao.unlockPaths(operation.getOperationId(), lockedResources);
      return Optional.empty();
    }

    FileStatus operationStatus = operationLocks[0];

    Instant lockInstant = Instant.ofEpochMilli(operation.getLockEpochMilli());
    if (isLockExpired(lockInstant)
        && isLockExpired(getLockRenewedInstant(operationStatus, operation))) {
      logger.atInfo().log("Operation %s expired.", operationStatus.getPath());
      return Optional.of(new AbstractMap.SimpleEntry<>(operationStatus, operation));
    }

    logger.atInfo().log("Operation %s not expired.", operationStatus.getPath());
    return Optional.empty();
  }

  private void deleteResource(String resource, Collection<String> loggedResources)
      throws IOException {
    Path lockedResource = new Path(resource);
    Set<String> allObjects =
        Arrays.stream(ghfs.listStatus(lockedResource))
            .map(s -> s.getPath().toString())
            .collect(toSet());
    List<StorageResourceId> objectsToDelete = new ArrayList<>(loggedResources.size());
    for (String loggedObject : loggedResources) {
      if (allObjects.contains(loggedObject)) {
        objectsToDelete.add(StorageResourceId.fromObjectName(loggedObject));
      }
    }
    GoogleCloudStorage gcs = ghfs.getGcsFs().getGcs();
    gcs.deleteObjects(objectsToDelete);

    // delete directory if empty
    allObjects.removeAll(loggedResources);
    if (allObjects.isEmpty() && ghfs.exists(lockedResource)) {
      ghfs.delete(lockedResource, /* recursive= */ false);
    }
  }

  private boolean isLockExpired(Instant lockInstant) {
    return lockInstant
        .plusMillis(GCS_COOPERATIVE_LOCKING_EXPIRATION_TIMEOUT_MS.get(conf, conf::getLong))
        .isBefore(operationExpirationInstant);
  }

  private Instant getLockRenewedInstant(FileStatus operationStatus, CoopLockRecord operation)
      throws IOException {
    switch (operation.getOperationType()) {
      case DELETE:
        return Instant.ofEpochMilli(
            getOperationObject(operationStatus, DeleteOperation.class).getLockEpochMilli());
      case RENAME:
        return Instant.ofEpochMilli(
            getOperationObject(operationStatus, RenameOperation.class).getLockEpochMilli());
    }
    throw new IllegalStateException("Unknown operation type: " + operationStatus.getPath());
  }

  private <T> T getOperationObject(FileStatus operation, Class<T> clazz) throws IOException {
    ByteSource operationByteSource =
        new ByteSource() {
          @Override
          public InputStream openStream() throws IOException {
            return ghfs.open(operation.getPath());
          }
        };
    String operationContent = operationByteSource.asCharSource(UTF_8).read();
    return GSON.fromJson(operationContent, clazz);
  }

  private <T> List<T> getOperationLog(FileStatus operation, Function<String, T> logRecordFn)
      throws IOException {
    List<T> log = new ArrayList<>();
    Path operationLog = new Path(operation.getPath().toString().replace(".lock", ".log"));
    try (BufferedReader in =
        new BufferedReader(new InputStreamReader(ghfs.open(operationLog), UTF_8))) {
      String line;
      while ((line = in.readLine()) != null) {
        log.add(logRecordFn.apply(line));
      }
    }
    return log;
  }

  private static String getOperationId(FileStatus operation) {
    List<String> fileParts = Splitter.on('_').splitToList(operation.getPath().toString());
    return Iterables.get(Splitter.on('.').split(Iterables.getLast(fileParts)), 0);
  }
}
