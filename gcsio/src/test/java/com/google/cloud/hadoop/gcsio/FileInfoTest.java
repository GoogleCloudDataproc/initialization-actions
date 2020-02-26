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
import com.google.common.truth.Correspondence;
import java.net.URI;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileInfoTest {

  @Test
  public void fromItemInfo() throws Exception {
    GoogleCloudStorageItemInfo itemInfo =
        new GoogleCloudStorageItemInfo(
            new StorageResourceId("foo-test-bucket", "bar/test/object"),
            /* creationTime= */ 10L,
            /* modificationTime= */ 15L,
            /* size= */ 200L,
            "us-east1",
            "nearline",
            "text/plain",
            /* contentEncoding= */ "lzma",
            /* metadata= */ ImmutableMap.of("foo-meta", new byte[] {5, 66, 56}),
            /* contentGeneration= */ 312432L,
            /* metaGeneration= */ 2L);

    FileInfo fileInfo = FileInfo.fromItemInfo(itemInfo);

    assertThat(fileInfo.getPath()).isEqualTo(new URI("gs://foo-test-bucket/bar/test/object"));
    assertThat(fileInfo.getCreationTime()).isEqualTo(10L);
    assertThat(fileInfo.getModificationTime()).isEqualTo(15L);
    assertThat(fileInfo.getSize()).isEqualTo(200);
    assertThat(fileInfo.getAttributes())
        .<byte[], byte[]>comparingValuesUsing(Correspondence.from(Arrays::equals, "Arrays.equals"))
        .containsExactly("foo-meta", new byte[] {5, 66, 56});
    assertThat(fileInfo.getItemInfo()).isEqualTo(itemInfo);
  }
}
