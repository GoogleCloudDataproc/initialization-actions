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

import static com.google.cloud.hadoop.fs.gcs.CoopLockFsck.ARGUMENT_ALL_OPERATIONS;
import static com.google.cloud.hadoop.fs.gcs.CoopLockFsck.COMMAND_CHECK;
import static com.google.cloud.hadoop.fs.gcs.CoopLockFsck.COMMAND_ROLL_FORWARD;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_COOPERATIVE_LOCKING_ENABLE;
import static com.google.common.base.Preconditions.checkArgument;
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
import com.google.cloud.hadoop.gcsio.cooplock.CooperativeLockingOptions;
import com.google.cloud.hadoop.gcsio.cooplock.DeleteOperation;
import com.google.cloud.hadoop.gcsio.cooplock.RenameOperation;
import com.google.cloud.hadoop.gcsio.cooplock.RenameOperationLogRecord;
import com.google.common.collect.ImmutableMap;
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

  private static final Gson GSON = CoopLockRecordsDao.createGson();

  private final Instant operationExpirationInstant = Instant.now();

  private final String bucketName;
  private final String command;
  private final String fsckOperationId;

  private final GoogleHadoopFileSystem ghfs;
  private final GoogleCloudStorageFileSystem gcsFs;
  private final GoogleCloudStorageImpl gcs;
  private final CooperativeLockingOptions options;
  private final CoopLockRecordsDao lockRecordsDao;
  private final CoopLockOperationDao lockOperationDao;

  public CoopLockFsckRunner(Configuration conf, URI bucketUri, String command, String operationId)
      throws IOException {
    // Disable cooperative locking to prevent blocking
    conf.setBoolean(GCS_COOPERATIVE_LOCKING_ENABLE.getKey(), false);

    this.bucketName = bucketUri.getAuthority();
    this.command = command;
    this.fsckOperationId = operationId;

    this.ghfs = (GoogleHadoopFileSystem) FileSystem.get(bucketUri, conf);
    this.gcsFs = ghfs.getGcsFs();
    this.gcs = (GoogleCloudStorageImpl) gcsFs.getGcs();
    this.options = gcs.getOptions().getCooperativeLockingOptions();
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

    // If this is a check command then return after operations status was printed.
    if (CoopLockFsck.COMMAND_CHECK.equals(command)) {
      return 0;
    }

    if (!ARGUMENT_ALL_OPERATIONS.equals(fsckOperationId)) {
      Optional<Map.Entry<FileStatus, CoopLockRecord>> operationEntry =
          expiredOperations.entrySet().stream()
              .filter(e -> e.getValue().getOperationId().equals(fsckOperationId))
              .findAny();
      checkArgument(operationEntry.isPresent(), "%s operation not found", fsckOperationId);
      expiredOperations =
          ImmutableMap.of(operationEntry.get().getKey(), operationEntry.get().getValue());
    }

    expiredOperations.forEach(
        (operationStatus, operationRecord) -> {
          long start = System.currentTimeMillis();
          try {
            repairOperation(operationStatus, operationRecord);
            logger.atInfo().log(
                "Operation %s successfully %s in %dms",
                operationStatus.getPath(),
                COMMAND_ROLL_FORWARD.equals(command) ? "rolled forward" : "rolled back",
                System.currentTimeMillis() - start);
          } catch (Exception e) {
            logger.atSevere().withCause(e).log(
                "Operation %s failed to %s in %dms",
                operationStatus.getPath(),
                COMMAND_ROLL_FORWARD.equals(command) ? "roll forward" : "roll back",
                System.currentTimeMillis() - start);
          }
        });
    return 0;
  }

  private void repairOperation(FileStatus operationStatus, CoopLockRecord operationRecord)
      throws IOException, URISyntaxException {
    switch (operationRecord.getOperationType()) {
      case DELETE:
        repairDeleteOperation(operationStatus, operationRecord);
        return;
      case RENAME:
        repairRenameOperation(operationStatus, operationRecord);
        return;
    }
    throw new IllegalStateException(
        String.format(
            "Unknown %s operation type: %s",
            operationRecord.getOperationId(), operationRecord.getOperationType()));
  }

  private void repairDeleteOperation(FileStatus operationStatus, CoopLockRecord operationRecord)
      throws IOException, URISyntaxException {
    if (CoopLockFsck.COMMAND_ROLL_BACK.equals(command)) {
      logger.atInfo().log(
          "Rolling back delete operations (%s) not supported, skipping.",
          operationStatus.getPath());
      return;
    }

    logger.atInfo().log("Repairing FS after %s delete operation.", operationStatus.getPath());
    DeleteOperation operation = getOperation(operationStatus, DeleteOperation.class);
    lockRecordsDao.relockOperation(bucketName, operationRecord);
    Future<?> lockUpdateFuture =
        lockOperationDao.scheduleLockUpdate(
            operationRecord.getOperationId(),
            new URI(operationStatus.getPath().toString()),
            DeleteOperation.class,
            (o, updateInstant) ->
                o.setLockExpiration(
                    updateInstant.plusMillis(options.getLockExpirationTimeoutMilli())));
    try {
      List<String> loggedResources = getOperationLog(operationStatus, l -> l);
      deleteResource(operation.getResource(), loggedResources);
      lockRecordsDao.unlockPaths(
          operationRecord.getOperationId(),
          StorageResourceId.fromObjectName(operation.getResource()));
    } finally {
      lockUpdateFuture.cancel(/* mayInterruptIfRunning= */ false);
    }
  }

  private void repairRenameOperation(FileStatus operationStatus, CoopLockRecord operationRecord)
      throws IOException, URISyntaxException {
    RenameOperation operation = getOperation(operationStatus, RenameOperation.class);
    lockRecordsDao.relockOperation(bucketName, operationRecord);
    Future<?> lockUpdateFuture =
        lockOperationDao.scheduleLockUpdate(
            operationRecord.getOperationId(),
            new URI(operationStatus.getPath().toString()),
            RenameOperation.class,
            (o, updateInstant) ->
                o.setLockExpiration(
                    updateInstant.plusMillis(options.getLockExpirationTimeoutMilli())));
    try {
      LinkedHashMap<String, String> loggedResources =
          getOperationLog(operationStatus, l -> GSON.fromJson(l, RenameOperationLogRecord.class))
              .stream()
              .collect(
                  toMap(
                      RenameOperationLogRecord::getSrc,
                      RenameOperationLogRecord::getDst,
                      (e1, e2) -> {
                        throw new RuntimeException(
                            String.format("Found entries with duplicate keys: %s and %s", e1, e2));
                      },
                      LinkedHashMap::new));
      if (operation.getCopySucceeded()) {
        if (CoopLockFsck.COMMAND_ROLL_BACK.equals(command)) {
          deleteAndRenameToRepairRenameOperation(
              operationStatus,
              operationRecord,
              operation.getDstResource(),
              new ArrayList<>(loggedResources.values()),
              operation.getSrcResource(),
              "source",
              new ArrayList<>(loggedResources.keySet()),
              /* copySucceeded= */ false);
        } else {
          deleteToRepairRenameOperation(
              operationStatus, operation.getSrcResource(), "source", loggedResources.keySet());
        }
      } else {
        if (CoopLockFsck.COMMAND_ROLL_BACK.equals(command)) {
          deleteToRepairRenameOperation(
              operationStatus, operation.getDstResource(), "destination", loggedResources.values());
        } else {
          deleteAndRenameToRepairRenameOperation(
              operationStatus,
              operationRecord,
              operation.getSrcResource(),
              new ArrayList<>(loggedResources.keySet()),
              operation.getDstResource(),
              "destination",
              new ArrayList<>(loggedResources.values()),
              /* copySucceeded= */ true);
        }
      }
      lockRecordsDao.unlockPaths(
          operationRecord.getOperationId(),
          StorageResourceId.fromObjectName(operation.getSrcResource()),
          StorageResourceId.fromObjectName(operation.getDstResource()));
    } finally {
      lockUpdateFuture.cancel(/* mayInterruptIfRunning= */ false);
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
        bucketName, operation.getOperationId(), operation.getOperationTime(), copySucceeded);

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
      String bucketName, CoopLockRecord operationRecord) throws IOException {
    String globPath =
        CoopLockRecordsDao.LOCK_DIRECTORY + "*" + operationRecord.getOperationId() + "*.lock";
    URI globUri =
        gcsFs.getPathCodec().getPath(bucketName, globPath, /* allowEmptyObjectName= */ false);
    FileStatus[] operationLocks = ghfs.globStatus(new Path(globUri));
    checkState(
        operationLocks.length < 2,
        "operation %s should not have more than one lock file",
        operationRecord.getOperationId());

    // Lock file not created - nothing to repair
    if (operationLocks.length == 0) {
      // Release lock if this is not a "check" command
      // and if it processes this specific operation or "all" operations
      if (!COMMAND_CHECK.equals(command)
          && (ARGUMENT_ALL_OPERATIONS.equals(fsckOperationId)
              || fsckOperationId.equals(operationRecord.getOperationId()))) {
        logger.atInfo().log(
            "Operation %s for %s resources doesn't have lock file, unlocking",
            operationRecord.getOperationId(), operationRecord.getResources());
        StorageResourceId[] lockedResources =
            operationRecord.getResources().stream()
                .map(resource -> new StorageResourceId(bucketName, resource))
                .toArray(StorageResourceId[]::new);
        lockRecordsDao.unlockPaths(operationRecord.getOperationId(), lockedResources);
      } else {
        logger.atInfo().log(
            "Operation %s for %s resources doesn't have lock file, skipping",
            operationRecord.getOperationId(), operationRecord.getResources());
      }
      return Optional.empty();
    }

    FileStatus operationStatus = operationLocks[0];

    if (operationRecord.getLockExpiration().isBefore(operationExpirationInstant)
        && getRenewedLockExpiration(operationStatus, operationRecord)
            .isBefore(operationExpirationInstant)) {
      logger.atInfo().log("Operation %s expired.", operationStatus.getPath());
      return Optional.of(new AbstractMap.SimpleEntry<>(operationStatus, operationRecord));
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

  private Instant getRenewedLockExpiration(
      FileStatus operationStatus, CoopLockRecord operationRecord) throws IOException {
    switch (operationRecord.getOperationType()) {
      case DELETE:
        return getOperation(operationStatus, DeleteOperation.class).getLockExpiration();
      case RENAME:
        return getOperation(operationStatus, RenameOperation.class).getLockExpiration();
    }
    throw new IllegalStateException(
        String.format(
            "Unknown %s operation type: %s",
            operationStatus.getPath(), operationRecord.getOperationType()));
  }

  private <T> T getOperation(FileStatus operationStatus, Class<T> clazz) throws IOException {
    ByteSource operationByteSource =
        new ByteSource() {
          @Override
          public InputStream openStream() throws IOException {
            return ghfs.open(operationStatus.getPath());
          }
        };
    String operationContent = operationByteSource.asCharSource(UTF_8).read();
    return GSON.fromJson(operationContent, clazz);
  }

  private <T> List<T> getOperationLog(FileStatus operationStatus, Function<String, T> logRecordFn)
      throws IOException {
    List<T> log = new ArrayList<>();
    Path operationLog = new Path(operationStatus.getPath().toString().replace(".lock", ".log"));
    try (BufferedReader in =
        new BufferedReader(new InputStreamReader(ghfs.open(operationLog), UTF_8))) {
      String line;
      while ((line = in.readLine()) != null) {
        log.add(logRecordFn.apply(line));
      }
    }
    return log;
  }
}
