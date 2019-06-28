package com.google.cloud.hadoop.gcsio.cooplock;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class DeleteOperation {
  private long lockEpochSeconds;
  private String resource = null;

  public long getLockEpochSeconds() {
    return lockEpochSeconds;
  }

  public DeleteOperation setLockEpochSeconds(long lockEpochSeconds) {
    this.lockEpochSeconds = lockEpochSeconds;
    return this;
  }

  public String getResource() {
    return resource;
  }

  public DeleteOperation setResource(String resource) {
    this.resource = resource;
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof DeleteOperation && equalsInternal((DeleteOperation) obj);
  }

  private boolean equalsInternal(DeleteOperation other) {
    return Objects.equals(lockEpochSeconds, other.lockEpochSeconds)
        && Objects.equals(resource, other.resource);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lockEpochSeconds, resource);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("lockEpochSeconds", lockEpochSeconds)
        .add("resource", resource)
        .toString();
  }
}
