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

import com.google.common.base.MoreObjects;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * A data class that represent a single lock record that corresponds to the specific cooperative
 * locking operation. This record is used to aqcuire and release lock atomcially.
 */
public class CoopLockRecord {
  private String clientId;
  private String operationId;
  private Instant operationTime;
  private CoopLockOperationType operationType;
  private Instant lockExpiration;
  private Set<String> resources = new TreeSet<>();

  public String getClientId() {
    return clientId;
  }

  public CoopLockRecord setClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  public String getOperationId() {
    return operationId;
  }

  public CoopLockRecord setOperationId(String operationId) {
    this.operationId = operationId;
    return this;
  }

  public Instant getOperationTime() {
    return operationTime;
  }

  public CoopLockRecord setOperationTime(Instant operationTime) {
    this.operationTime = operationTime;
    return this;
  }

  public CoopLockOperationType getOperationType() {
    return operationType;
  }

  public CoopLockRecord setOperationType(CoopLockOperationType operationType) {
    this.operationType = operationType;
    return this;
  }

  public Instant getLockExpiration() {
    return lockExpiration;
  }

  public CoopLockRecord setLockExpiration(Instant lockExpiration) {
    this.lockExpiration = lockExpiration;
    return this;
  }

  public Set<String> getResources() {
    return resources;
  }

  public CoopLockRecord setResources(Set<String> resources) {
    this.resources = new TreeSet<>(resources);
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj
        || (obj != null && getClass() == obj.getClass() && equalsInternal((CoopLockRecord) obj));
  }

  private boolean equalsInternal(CoopLockRecord other) {
    return Objects.equals(clientId, other.clientId)
        && Objects.equals(operationId, other.operationId)
        && Objects.equals(operationTime, other.operationTime)
        && operationType == other.operationType
        && Objects.equals(lockExpiration, other.lockExpiration)
        && Objects.equals(resources, other.resources);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        clientId, operationId, operationTime, operationType, lockExpiration, resources);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("clientId", clientId)
        .add("operationId", operationId)
        .add("operationTime", operationTime)
        .add("operationType", operationType)
        .add("lockExpiration", lockExpiration)
        .add("resources", resources)
        .toString();
  }
}
