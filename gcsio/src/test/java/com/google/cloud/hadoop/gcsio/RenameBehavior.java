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
 * RenameBehavior distills out the fine-grained expected behavior of a FileSystem in various
 * edge/error cases; implementations associated with each possible FileSystem thus encapsulate
 * the minor differences in behavior in rename() calls.
 */
public interface RenameBehavior {
  /**
   * Returns the MethodOutcome of trying to rename an existing file into the root directory.
   */
  MethodOutcome renameFileIntoRootOutcome();

  /**
   * Returns the MethodOutcome of trying to rename the root.
   */
  MethodOutcome renameRootOutcome();

  /**
   * Returns the MethodOutcome when the src doesn't exist.
   */
  MethodOutcome nonExistentSourceOutcome();

  /**
   * Returns the MethodOutcome when dst already exists, is a file, and src is also a file.
   */
  MethodOutcome destinationFileExistsSrcIsFileOutcome();

  /**
   * Returns the MethodOutcome when dst already exists, is a file, and src is a directory.
   */
  MethodOutcome destinationFileExistsSrcIsDirectoryOutcome();

  /**
   * Returns the MethodOutcome when a parent of file dst doesn't exist.
   */
  MethodOutcome nonExistentDestinationFileParentOutcome();

  /**
   * Returns the MethodOutcome when a parent of directory dst doesn't exist.
   */
  MethodOutcome nonExistentDestinationDirectoryParentOutcome();
}
