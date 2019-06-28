package com.google.cloud.hadoop.gcsio.cooplock;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class RenameOperation {
  private long lockEpochSeconds;
  private String srcResource;
  private String dstResource;
  private boolean copySucceeded;

  public long getLockEpochSeconds() {
    return lockEpochSeconds;
  }

  public RenameOperation setLockEpochSeconds(long lockEpochSeconds) {
    this.lockEpochSeconds = lockEpochSeconds;
    return this;
  }

  public String getSrcResource() {
    return srcResource;
  }

  public RenameOperation setSrcResource(String srcResource) {
    this.srcResource = srcResource;
    return this;
  }

  public String getDstResource() {
    return dstResource;
  }

  public RenameOperation setDstResource(String dstResource) {
    this.dstResource = dstResource;
    return this;
  }

  public boolean getCopySucceeded() {
    return copySucceeded;
  }

  public RenameOperation setCopySucceeded(boolean copySucceeded) {
    this.copySucceeded = copySucceeded;
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof RenameOperation && equalsInternal((RenameOperation) obj);
  }

  private boolean equalsInternal(RenameOperation other) {
    return Objects.equals(lockEpochSeconds, other.lockEpochSeconds)
        && Objects.equals(srcResource, other.srcResource)
        && Objects.equals(dstResource, other.dstResource)
        && Objects.equals(copySucceeded, other.copySucceeded);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lockEpochSeconds, srcResource, dstResource, copySucceeded);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("lockEpochSeconds", lockEpochSeconds)
        .add("srcResource", srcResource)
        .add("dstResource", dstResource)
        .add("copySucceeded", copySucceeded)
        .toString();
  }
}
