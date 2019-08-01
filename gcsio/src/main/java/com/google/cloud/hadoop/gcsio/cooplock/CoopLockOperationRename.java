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

import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationType.RENAME;
import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockUtils.normalizeLockedResource;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.ForwardingGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.PathCodec;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

public class CoopLockOperationRename {

  private final String operationId = UUID.randomUUID().toString();
  private final Instant operationInstant = Instant.now();

  private final StorageResourceId srcResourceId;
  private final StorageResourceId dstResourceId;

  private final CoopLockRecordsDao coopLockRecordsDao;
  private final CoopLockOperationDao coopLockOperationDao;

  private Future<?> lockUpdateFuture;

  private CoopLockOperationRename(
      GoogleCloudStorageImpl gcs,
      PathCodec pathCodec,
      StorageResourceId srcResourceId,
      StorageResourceId dstResourceId) {
    this.srcResourceId = srcResourceId;
    this.dstResourceId = dstResourceId;
    this.coopLockRecordsDao = new CoopLockRecordsDao(gcs);
    this.coopLockOperationDao = new CoopLockOperationDao(gcs, pathCodec);
  }

  public static CoopLockOperationRename create(
      GoogleCloudStorage gcs, PathCodec pathCodec, URI src, URI dst) {
    while (gcs instanceof ForwardingGoogleCloudStorage) {
      gcs = ((ForwardingGoogleCloudStorage) gcs).getDelegate();
    }
    checkArgument(
        gcs instanceof GoogleCloudStorageImpl,
        "gcs should be instance of %s, but was %s",
        GoogleCloudStorageImpl.class,
        gcs.getClass());
    return new CoopLockOperationRename(
        (GoogleCloudStorageImpl) gcs,
        pathCodec,
        pathCodec.validatePathAndGetId(
            normalizeLockedResource(src), /* allowEmptyObjectName= */ true),
        pathCodec.validatePathAndGetId(
            normalizeLockedResource(dst), /* allowEmptyObjectName= */ true));
  }

  public void lock() {
    try {
      coopLockRecordsDao.lockPaths(
          operationId, operationInstant, RENAME, srcResourceId, dstResourceId);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to acquire lock %s operation", this), e);
    }
  }

  public void persistAndScheduleRenewal(
      Map<FileInfo, URI> srcToDstItemNames, Map<FileInfo, URI> srcToDstMarkerItemNames) {
    try {
      lockUpdateFuture =
          coopLockOperationDao.persistRenameOperation(
              operationId,
              operationInstant,
              srcResourceId,
              dstResourceId,
              srcToDstItemNames,
              srcToDstMarkerItemNames);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to persist %s operation", this), e);
    }
  }

  public void checkpoint() {
    try {
      coopLockOperationDao.checkpointRenameOperation(
          srcResourceId, dstResourceId, operationId, operationInstant, /* copySucceeded= */ true);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to checkpoint %s operation", this), e);
    }
  }

  public void unlock() {
    try {
      coopLockRecordsDao.unlockPaths(operationId, srcResourceId, dstResourceId);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to release unlock for %s operation", this), e);
    }
  }

  public void cancelRenewal() {
    lockUpdateFuture.cancel(/* mayInterruptIfRunning= */ true);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("operationId", operationId)
        .add("operationInstant", operationInstant)
        .add("srcResourceId", srcResourceId)
        .add("dstResourceId", dstResourceId)
        .toString();
  }
}
