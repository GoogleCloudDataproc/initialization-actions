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
import java.util.Set;
import java.util.TreeSet;

/** Class that represent cooperative locking operation */
public class CoopLockRecord {
  private String clientId;
  private String operationId;
  private long operationEpochSeconds;
  private CoopLockOperationType operationType;
  private long lockEpochSeconds;
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

  public long getOperationEpochSeconds() {
    return operationEpochSeconds;
  }

  public CoopLockRecord setOperationEpochSeconds(long operationEpochSeconds) {
    this.operationEpochSeconds = operationEpochSeconds;
    return this;
  }

  public CoopLockOperationType getOperationType() {
    return operationType;
  }

  public CoopLockRecord setOperationType(CoopLockOperationType operationType) {
    this.operationType = operationType;
    return this;
  }

  public long getLockEpochSeconds() {
    return lockEpochSeconds;
  }

  public CoopLockRecord setLockEpochSeconds(long lockEpochSeconds) {
    this.lockEpochSeconds = lockEpochSeconds;
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
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("clientId", clientId)
        .add("operationId", operationId)
        .add("operationEpochSeconds", operationEpochSeconds)
        .add("operationType", operationType)
        .add("lockEpochSeconds", lockEpochSeconds)
        .add("resources", resources)
        .toString();
  }
}
