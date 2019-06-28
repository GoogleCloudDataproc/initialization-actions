package com.google.cloud.hadoop.gcsio.cooplock;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

public class CoopLockRecords {
  /** Supported version of operation locks */
  public static final long FORMAT_VERSION = 1;

  private long formatVersion = -1;
  private Set<CoopLockRecord> locks =
      new TreeSet<>(Comparator.comparing(CoopLockRecord::getOperationId));

  public long getFormatVersion() {
    return formatVersion;
  }

  public CoopLockRecords setFormatVersion(long formatVersion) {
    this.formatVersion = formatVersion;
    return this;
  }

  public Set<CoopLockRecord> getLocks() {
    checkState(
        FORMAT_VERSION == formatVersion,
        "%s operation lock version is not supported, supported version is %s",
        formatVersion,
        FORMAT_VERSION);
    return locks;
  }

  public CoopLockRecords setLocks(Set<CoopLockRecord> locks) {
    this.locks = new TreeSet<>(Comparator.comparing(CoopLockRecord::getOperationId));
    this.locks.addAll(locks);
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("formatVersion", formatVersion)
        .add("operations", locks)
        .toString();
  }
}
