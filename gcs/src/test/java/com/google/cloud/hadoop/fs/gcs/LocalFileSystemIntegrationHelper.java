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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.fs.FileSystem;

public class LocalFileSystemIntegrationHelper
  extends HadoopFileSystemIntegrationHelper {

  public LocalFileSystemIntegrationHelper(
      FileSystem ghfs, FileSystemDescriptor ghfsFileSystemDescriptor) throws IOException {
    super(ghfs, ghfsFileSystemDescriptor);
  }

  @Override
  public long getExpectedObjectSize(String objectName, boolean expectedToExist)
      throws UnsupportedEncodingException {
    // For file:/ scheme directories which are expected to exist, we have no idea what the
    // filesystem will report; usually it's 4096 but memfs in /tmp can be any number of possible
    // sizes; return Long.MIN_VALUE for "don't know". Otherwise, delegate to superclass.
    boolean isDir = objectName == null || objectName.endsWith("/");
    if (expectedToExist && isDir) {
      return Long.MIN_VALUE;
    } else {
      return super.getExpectedObjectSize(objectName, expectedToExist);
    }
  }
}
