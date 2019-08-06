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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * A data class that represents lock records for all cooperative locking operations. This records
 * file is used to aqcuire and release locks atomcially.
 */
public class CoopLockRecords {
  /**
   * Supported version of operation locks persistent objects format.
   *
   * <p>When making any changes to cooperative locking persistent objects format (adding, renaming
   * or removing fields), then you need to increase this version number to prevent corruption.
   */
  public static final long FORMAT_VERSION = 3;

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
