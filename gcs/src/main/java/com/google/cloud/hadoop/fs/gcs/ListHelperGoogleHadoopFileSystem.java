/**
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.cloud.hadoop.gcsio.FileInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.MetadataReadOnlyGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * ListHelperGoogleHadoopFileSystem overrides the behavior of GoogleHadoopFileSystem to manifest
 * a temporary FileSystem suitable only for list/get methods for retrieving file metadata, based
 * on an initial collection of file metadata provided at construction time. Can be used as a
 * heavyweight cache which behaves just like a normal filesystem for such metadata-read operations
 * and lives in the context of a single complex top-level method call, like globStatus.
 * <p>
 * Note that this class is *not* intended to be used as a general-usage FileSystem.
 */
class ListHelperGoogleHadoopFileSystem
    extends GoogleHadoopFileSystem {

  /**
   * Factory method for constructing and initializing an instance of
   * ListHelperGoogleHadoopFileSystem which is ready to list/get FileStatus entries corresponding
   * to {@code fileInfos}.
   */
  public static GoogleHadoopFileSystem createInstance(Collection<FileInfo> fileInfos)
      throws IOException {
    Preconditions.checkState(!fileInfos.isEmpty(),
        "Cannot construct ListHelperGoogleHadoopFileSystem with empty fileInfos list!");
    List<GoogleCloudStorageItemInfo> infos = new ArrayList<>();
    URI rootUri = null;
    for (FileInfo info : fileInfos) {
      infos.add(info.getItemInfo());
      if (rootUri == null) {
        // Set the root URI to the first path in the collection.
        rootUri = info.getPath();
      }
    }

    // Add in placeholder bucket info, since the bucket info won't be relevant for our listObject
    // operations.
    String tempBucket = rootUri.getAuthority();
    infos.add(new GoogleCloudStorageItemInfo(new StorageResourceId(tempBucket), 0, 0, "", ""));

    MetadataReadOnlyGoogleCloudStorage tempGcs = new MetadataReadOnlyGoogleCloudStorage(infos);
    GoogleCloudStorageFileSystem tempGcsFs = new GoogleCloudStorageFileSystem(tempGcs);
    GoogleHadoopFileSystem tempGhfs = new ListHelperGoogleHadoopFileSystem(tempGcsFs);

    Configuration tempConfig = new Configuration();
    tempConfig.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, tempBucket);
    tempConfig.setBoolean(GoogleHadoopFileSystemBase.GCS_CREATE_SYSTEM_BUCKET_KEY, false);
    tempConfig.set(GoogleHadoopFileSystemBase.GCS_WORKING_DIRECTORY_KEY, "/");
    // Set initSuperclass == false to avoid screwing up FileSystem statistics.
    tempGhfs.initialize(rootUri, tempConfig, false);
    return tempGhfs;
  }

  /**
   * Constructs an instance of GoogleHadoopFileSystem using the provided
   * GoogleCloudStorageFileSystem; initialize() will not re-initialize it.
   */
  public ListHelperGoogleHadoopFileSystem(GoogleCloudStorageFileSystem gcsfs) {
    super(gcsfs);
  }

  /**
   * Do not inherit the ability to use flat globbing so that we don't end up with an infinite
   * stack of ListHelperGoogleHadoopFileSystem instances.
   */
  @Override
  boolean shouldUseFlatGlob(Path fixedPath) {
    return false;
  }
}
