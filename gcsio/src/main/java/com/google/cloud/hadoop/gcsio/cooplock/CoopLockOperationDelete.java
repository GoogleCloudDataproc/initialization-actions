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

import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockOperationType.DELETE;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

public class CoopLockOperationDelete {

  private final String operationId = UUID.randomUUID().toString();
  private final Instant operationInstant = Instant.now();

  private final StorageResourceId resourceId;

  private final CoopLockRecordsDao coopLockRecordsDao;
  private final CoopLockOperationDao coopLockOperationDao;

  private Future<?> lockUpdateFuture;

  private CoopLockOperationDelete(
      GoogleCloudStorageImpl gcs, PathCodec pathCodec, StorageResourceId resourceId) {
    this.resourceId = resourceId;
    this.coopLockRecordsDao = new CoopLockRecordsDao(gcs);
    this.coopLockOperationDao = new CoopLockOperationDao(gcs, pathCodec);
  }

  public static CoopLockOperationDelete create(
      GoogleCloudStorage gcs, PathCodec pathCodec, URI path) {
    while (gcs instanceof ForwardingGoogleCloudStorage) {
      gcs = ((ForwardingGoogleCloudStorage) gcs).getDelegate();
    }
    checkArgument(
        gcs instanceof GoogleCloudStorageImpl,
        "gcs should be instance of %s, but was %s",
        GoogleCloudStorageImpl.class,
        gcs.getClass());
    return new CoopLockOperationDelete(
        (GoogleCloudStorageImpl) gcs,
        pathCodec,
        pathCodec.validatePathAndGetId(
            normalizeLockedResource(path), /* allowEmptyObjectName= */ true));
  }

  public void lock() {
    try {
      coopLockRecordsDao.lockPaths(operationId, operationInstant, DELETE, resourceId);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to acquire lock for %s operation", this), e);
    }
  }

  public void persistAndScheduleRenewal(
      List<FileInfo> itemsToDelete, List<FileInfo> bucketsToDelete) {
    try {
      lockUpdateFuture =
          coopLockOperationDao.persistDeleteOperation(
              operationId, operationInstant, resourceId, itemsToDelete, bucketsToDelete);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to persist %s operation", this), e);
    }
  }

  public void unlock() {
    try {
      coopLockRecordsDao.unlockPaths(operationId, resourceId);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to release lock for %s operation", this), e);
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
        .add("resourceId", resourceId)
        .toString();
  }
}
