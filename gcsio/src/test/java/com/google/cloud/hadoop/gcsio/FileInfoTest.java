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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileInfoTest {

  PathCodec simplePathCodec = new PathCodec() {
    @Override
    public StorageResourceId validatePathAndGetId(URI path, boolean allowEmptyObjectName) {
      return new StorageResourceId(path.getAuthority(), path.getPath());
    }

    @Override
    public URI getPath(String bucketName, String objectName, boolean allowEmptyObjectName) {
      return URI.create(String.format("gs://%s/%s", bucketName, objectName));
    }
  };

  @Test
  public void nullModificationTimeResultInCreationTimeBeingReturned() {
    // ImmutableMap doesn't play well with null values:
    Map<String, byte[]> metadata = new HashMap<>();
    metadata.put(FileInfo.FILE_MODIFICATION_TIMESTAMP_KEY, null);
    GoogleCloudStorageItemInfo itemInfo =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId("testBucket", "testObject"),
            10L /* creation timestamp */,
            200L /* size */,
            "location",
            "storage class",
            "text/plain",
            metadata,
            0L,
            0L);

    FileInfo fileInfo = FileInfo.fromItemInfo(simplePathCodec, itemInfo);

    assertThat(fileInfo.getModificationTime()).isEqualTo(10L);
  }

  @Test
  public void modificationTimeParsingFailuresResultInCreationTimeBeingReturned() {
    // Failures occur when there aren't 8 bytes for modification timestamp
    Map<String, byte[]> metadata =
        ImmutableMap.of(FileInfo.FILE_MODIFICATION_TIMESTAMP_KEY, new byte[2]);
    GoogleCloudStorageItemInfo itemInfo =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId("testBucket", "testObject"),
            10L /* creation timestamp */,
            200L /* size */,
            "location",
            "storage class",
            "text/plain",
            metadata,
            0L,
            0L);

    FileInfo fileInfo = FileInfo.fromItemInfo(simplePathCodec, itemInfo);

    assertThat(fileInfo.getModificationTime()).isEqualTo(10L);
  }
}
