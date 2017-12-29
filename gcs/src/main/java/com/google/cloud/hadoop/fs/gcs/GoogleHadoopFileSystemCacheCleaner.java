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

import com.google.cloud.hadoop.gcsio.CacheEntry;
import com.google.cloud.hadoop.gcsio.DirectoryListCache;
import com.google.cloud.hadoop.gcsio.FileSystemBackedDirectoryListCache;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool that will perform GC on {@link FileSystemBackedDirectoryListCache} backing stores.
 */
public class GoogleHadoopFileSystemCacheCleaner {
  public static final Logger LOG =
      LoggerFactory.getLogger(GoogleHadoopFileSystemCacheCleaner.class);

  /**
   * Iterates over items in {@code cache}, object-first then buckets, allowing the list operations
   * to perform cache-expiration as they run.
   */
  public static void cleanCache(DirectoryListCache cache) throws IOException {
    for (CacheEntry bucket : cache.getRawBucketList()) {
      String bucketName = bucket.getResourceId().getBucketName();
      LOG.info("Performing GC on cache bucket {}", bucketName);

      cache.getObjectList(bucketName, "", null, null);
    }

    // After having cleared out the objects/subdirectories, go over the top-level bucket list once
    // to potentially garbage-collect newly emptied buckets.
    cache.getBucketList();
  }

  public static void main(String[] args) throws IOException {
    GenericOptionsParser parser = new GenericOptionsParser(args);
    Configuration configuration = parser.getConfiguration();

    // TODO: Wire out constants and defaults through GoogleHadoopFileSystemBase once submitted.
    if ("FILESYSTEM_BACKED".equals(configuration.get("fs.gs.metadata.cache.type", "IN_MEMORY"))) {
      String fsStringPath = configuration.get("fs.gs.metadata.cache.directory", "");
      Preconditions.checkState(!Strings.isNullOrEmpty(fsStringPath));
      LOG.info("Performing GC on cache directory {}", fsStringPath);

      Path path = Paths.get(fsStringPath);
      if (Files.exists(path)) {
        FileSystemBackedDirectoryListCache cache =
            new FileSystemBackedDirectoryListCache(fsStringPath);
        cleanCache(cache);
      }
    }

    LOG.info("Done with GC.");
  }
}
