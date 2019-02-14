/*
 * Copyright 2018 Google LLC
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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Hadoop 2 specific implementation of {@link FileSystem}.
 *
 * @see GoogleHadoopFileSystemBase
 */
abstract class GoogleHadoopFileSystemBaseSpecific extends FileSystem {

  static String getPassword(Configuration config, String name) {
    char[] value;
    try {
      value = config.getPassword(name);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return value == null ? null : String.valueOf(value);
  }

  /** @see GoogleHadoopFileSystemBase#getGcsPath(Path) */
  public abstract URI getGcsPath(Path hadoopPath);

  /** @see GoogleHadoopFileSystemBase#getGcsFs() */
  public abstract GoogleCloudStorageFileSystem getGcsFs();

  /** {@inheritDoc} */
  @Override
  public FSDataOutputStream createNonRecursive(
      Path hadoopPath,
      FsPermission permission,
      EnumSet<org.apache.hadoop.fs.CreateFlag> flags,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    URI gcsPath = getGcsPath(checkNotNull(hadoopPath, "hadoopPath must not be null"));
    URI parentGcsPath = getGcsFs().getParentPath(gcsPath);
    GoogleCloudStorageItemInfo parentInfo = getGcsFs().getFileInfo(parentGcsPath).getItemInfo();
    if (!parentInfo.isRoot() && !parentInfo.isBucket() && !parentInfo.exists()) {
      throw new FileNotFoundException(
          String.format(
              "Can not create '%s' file, because parent folder does not exist: %s",
              gcsPath, parentGcsPath));
    }
    return create(
        hadoopPath,
        permission,
        flags.contains(org.apache.hadoop.fs.CreateFlag.OVERWRITE),
        bufferSize,
        replication,
        blockSize,
        progress);
  }

  /** {@inheritDoc} */
  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flags)
      throws IOException {
    checkArgument(flags != null && !flags.isEmpty(), "flags should not be null or empty");
    boolean create = flags.contains(XAttrSetFlag.CREATE);
    boolean replace = flags.contains(XAttrSetFlag.REPLACE);
    setXAttrInternal(path, name, value, create, replace);
  }

  abstract void setXAttrInternal(
      Path path, String name, byte[] value, boolean create, boolean replace) throws IOException;
}
