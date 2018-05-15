/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.fs.gcs;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class WebHdfsIntegrationHelper
  extends HadoopFileSystemIntegrationHelper {

  public WebHdfsIntegrationHelper(
      FileSystem ghfs, FileSystemDescriptor ghfsFileSystemDescriptor) throws IOException {
    super(ghfs, ghfsFileSystemDescriptor);
  }

  /**
   * Lists status of file(s) at the given path.
   */
  @Override
  protected FileStatus[] listStatus(Path hadoopPath)
      throws IOException {
    FileStatus[] status = null;
    try {
      status = ghfs.listStatus(hadoopPath);
    } catch (FileNotFoundException e) {
      // Catch and swallow FileNotFoundException to keep status == null.
    }
    return status;
  }
}
