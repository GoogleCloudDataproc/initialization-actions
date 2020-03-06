/*
 * Copyright 2017 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.util;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import javax.annotation.Nullable;

/** Options for the GCS Requester Pays feature. */
@AutoValue
public abstract class RequesterPaysOptions {

  /** Operational modes of GCS Requester Pays feature. */
  public static enum RequesterPaysMode {
    AUTO,
    CUSTOM,
    DISABLED,
    ENABLED
  }

  /** Default value for {@link RequesterPaysOptions#getMode}. */
  public static final RequesterPaysMode REQUESTER_PAYS_MODE_DEFAULT = RequesterPaysMode.DISABLED;

  /** Default value for {@link RequesterPaysOptions#getBuckets}. */
  public static final Set<String> REQUESTER_PAYS_BUCKETS_DEFAULT = ImmutableSet.of();

  public static final RequesterPaysOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new AutoValue_RequesterPaysOptions.Builder()
        .setMode(REQUESTER_PAYS_MODE_DEFAULT)
        .setBuckets(REQUESTER_PAYS_BUCKETS_DEFAULT);
  }

  public abstract RequesterPaysMode getMode();

  @Nullable
  public abstract String getProjectId();

  public abstract Set<String> getBuckets();

  public abstract Builder toBuilder();

  /** Builder for {@link RequesterPaysOptions} */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setMode(RequesterPaysMode mode);

    public abstract Builder setProjectId(@Nullable String projectId);

    public abstract Builder setBuckets(Set<String> value);

    public abstract RequesterPaysOptions build();
  }
}
