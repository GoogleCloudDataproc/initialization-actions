/*
 * Copyright 2013 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs;

import org.apache.hadoop.fs.Path;

/**
 * FileSystemDescriptor defines the interface containing all the methods which are necessary to
 * differentiate the behavior of different Hadoop FileSystem implementations, including the URI
 * scheme with which an implementation is identified.
 */
public interface FileSystemDescriptor {
  /**
   * Returns the Hadoop path representing the root of the FileSystem associated with this
   * FileSystemDescriptor.
   */
  Path getFileSystemRoot();

  /**
   * Returns the URI scheme for the Hadoop FileSystem associated with this FileSystemDescriptor.
   */
  String getScheme();
}
