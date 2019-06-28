package com.google.cloud.hadoop.gcsio.cooplock;

import com.google.common.base.MoreObjects;
import java.util.Set;
import java.util.TreeSet;

/** Class that represent cooperative locking operation */
public class CoopLockRecord {
  private String operationId;
  private long lockEpochSeconds;
  private Set<String> resources = new TreeSet<>();

  public String getOperationId() {
    return operationId;
  }

  public CoopLockRecord setOperationId(String operationId) {
    this.operationId = operationId;
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
        .add("operationId", operationId)
        .add("lockEpochSeconds", lockEpochSeconds)
        .add("resources", resources)
        .toString();
  }
}
