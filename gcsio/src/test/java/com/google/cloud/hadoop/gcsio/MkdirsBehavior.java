/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

/**
 * MkdirsBehavior distills out the fine-grained expected behavior of a FileSystem in various
 * edge/error cases; implementations associated with each possible FileSystem thus encapsulate
 * the minor differences in behavior in mkdirs() calls.
 */
public interface MkdirsBehavior {
  /**
   * Returns the MethodOutcome of trying to mkdirs root.
   */
  MethodOutcome mkdirsRootOutcome();

  /**
   * Returns the MethodOutcome of trying mkdirs if a file of the same name already exists.
   */
  MethodOutcome fileAlreadyExistsOutcome();
}
