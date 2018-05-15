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

package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.gcsio.DeletionBehavior;
import com.google.cloud.hadoop.gcsio.MethodOutcome;
import com.google.cloud.hadoop.gcsio.MkdirsBehavior;
import com.google.cloud.hadoop.gcsio.RenameBehavior;

import java.io.IOException;

/**
 * Defines behavior of HDFS operations.
 *
 * HDFS tests use an instance of this class as-is.
 * Other tests (eg, GHFS tests) may override parts of the behavior methods if the corresponding
 * FS' behavior differs from that of HDFS. It allows us to easily identify deviation of behavior of
 * a file system from that of HDFS.
 */
public class HdfsBehavior
    implements DeletionBehavior, MkdirsBehavior, RenameBehavior {

  // -----------------------------------------------------------------
  // Deletion behavior.
  // -----------------------------------------------------------------

  /**
   * Returns the MethodOutcome of trying non-recursive deletes for non-empty directories.
   */
  @Override
  public MethodOutcome nonEmptyDeleteOutcome() {
    return new MethodOutcome(MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
  }

  /**
   * Returns the MethodOutcome of trying to delete a non-existent filename.
   */
  @Override
  public MethodOutcome nonExistentDeleteOutcome() {
    return new MethodOutcome(MethodOutcome.Type.RETURNS_FALSE);
  }

  // -----------------------------------------------------------------
  // Mkdirs behavior.
  // -----------------------------------------------------------------

  /**
   * Returns the MethodOutcome of trying to mkdirs root.
   */
  @Override
  public MethodOutcome mkdirsRootOutcome() {
    return new MethodOutcome(MethodOutcome.Type.RETURNS_TRUE);
  }

  /**
   * Returns the MethodOutcome of trying mkdirs if a file of the same name already exists.
   */
  @Override
  public MethodOutcome fileAlreadyExistsOutcome() {
    return new MethodOutcome(MethodOutcome.Type.THROWS_EXCEPTION, IOException.class);
  }

  // -----------------------------------------------------------------
  // Rename behavior.
  // -----------------------------------------------------------------

  /**
   * Returns the MethodOutcome of trying to rename an existing file into the root directory.
   */
  @Override
  public MethodOutcome renameFileIntoRootOutcome() {
    return new MethodOutcome(MethodOutcome.Type.RETURNS_FALSE);
  }

  /**
   * Returns the MethodOutcome of trying to rename the root.
   */
  @Override
  public MethodOutcome renameRootOutcome() {
    return new MethodOutcome(MethodOutcome.Type.RETURNS_FALSE);
  }

  /**
   * Returns the MethodOutcome when the src doesn't exist.
   */
  @Override
  public MethodOutcome nonExistentSourceOutcome() {
    return new MethodOutcome(MethodOutcome.Type.RETURNS_FALSE);
  }

  /**
   * Returns the MethodOutcome when dst already exists, is a file, and src is also a file.
   */
  @Override
  public MethodOutcome destinationFileExistsSrcIsFileOutcome() {
    return new MethodOutcome(MethodOutcome.Type.RETURNS_FALSE);
  }

  /**
   * Returns the MethodOutcome when dst already exists, is a file, and src is a directory.
   */
  @Override
  public MethodOutcome destinationFileExistsSrcIsDirectoryOutcome() {
    return new MethodOutcome(MethodOutcome.Type.RETURNS_FALSE);
  }

  /**
   * Returns the MethodOutcome when a parent of file dst doesn't exist.
   */
  @Override
  public MethodOutcome nonExistentDestinationFileParentOutcome() {
    return new MethodOutcome(MethodOutcome.Type.RETURNS_FALSE);
  }

  /**
   * Returns the MethodOutcome when a parent of directory dst doesn't exist.
   */
  @Override
  public MethodOutcome nonExistentDestinationDirectoryParentOutcome() {
    return new MethodOutcome(MethodOutcome.Type.RETURNS_FALSE);
  }
}
