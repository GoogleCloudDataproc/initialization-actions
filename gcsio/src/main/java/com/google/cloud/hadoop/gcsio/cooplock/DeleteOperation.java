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
