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
  private long operationEpochMilli;
  private CoopLockOperationType operationType;
  private long lockEpochMilli;
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

  public long getOperationEpochMilli() {
    return operationEpochMilli;
  }

  public CoopLockRecord setOperationEpochMilli(long operationEpochMilli) {
    this.operationEpochMilli = operationEpochMilli;
    return this;
  }

  public CoopLockOperationType getOperationType() {
    return operationType;
  }

  public CoopLockRecord setOperationType(CoopLockOperationType operationType) {
    this.operationType = operationType;
    return this;
  }

  public long getLockEpochMilli() {
    return lockEpochMilli;
  }

  public CoopLockRecord setLockEpochMilli(long lockEpochMilli) {
    this.lockEpochMilli = lockEpochMilli;
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
        .add("operationEpochMilli", operationEpochMilli)
        .add("operationType", operationType)
        .add("lockEpochMilli", lockEpochMilli)
        .add("resources", resources)
        .toString();
  }
}
