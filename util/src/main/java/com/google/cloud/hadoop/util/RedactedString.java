/*
 * Copyright 2020 Google LLC. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/**
 * Holder class for string values that should not be logged and displayed when {@code toString}
 * method called. For example, it should be used for credentials.
 */
@AutoValue
public abstract class RedactedString {

  @Nullable
  public static RedactedString create(@Nullable String value) {
    return isNullOrEmpty(value) ? null : new AutoValue_RedactedString(value);
  }

  public abstract String value();

  @Override
  public String toString() {
    return "<redacted>";
  }
}
