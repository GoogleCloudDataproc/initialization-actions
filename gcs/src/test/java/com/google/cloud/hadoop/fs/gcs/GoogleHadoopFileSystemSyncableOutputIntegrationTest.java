/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.OutputStreamType;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import java.net.URI;
import java.util.Arrays;
import java.util.Random;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemSyncableOutputIntegrationTest
    extends GoogleHadoopFileSystemIntegrationTest {

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(
          GoogleHadoopFileSystemSyncableOutputIntegrationTest.class) {
        @Override
        public void before() throws Throwable {
          GoogleHadoopFileSystemIntegrationTest.storageResource.before();
          ghfs.getConf()
              .set(GCS_OUTPUT_STREAM_TYPE.getKey(), OutputStreamType.SYNCABLE_COMPOSITE.name());
        }

        @Override
        public void after() {
          GoogleHadoopFileSystemIntegrationTest.storageResource.after();
        }
      };

  @Test
  @Override
  public void testHsync() throws Exception {
    internalTestHsync();
  }

  @Test
  public void testSyncComposite() throws Exception {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);

    // test composing of 5 1-byte writes into 5-byte object
    byte[] expected = new byte[5];
    new Random().nextBytes(expected);

    try (FSDataOutputStream fout = ghfs.create(hadoopPath)) {
      for (int i = 0; i < expected.length; ++i) {
        fout.write(expected, i, 1);
        fout.hsync();

        // validate partly composed data
        int composedLength = i + 1;
        if (composedLength % 2 == 0) {
          FileStatus status = ghfs.getFileStatus(hadoopPath);
          assertThat(status.getLen()).isEqualTo(composedLength);

          byte[] actualComposed = new byte[composedLength];
          try (FSDataInputStream fin = ghfs.open(hadoopPath)) {
            fin.readFully(0, actualComposed);
          }
          assertThat(actualComposed).isEqualTo(Arrays.copyOf(expected, composedLength));
        }
      }
    }

    FileStatus status = ghfs.getFileStatus(hadoopPath);
    assertThat(status.getLen()).isEqualTo(expected.length);

    byte[] actual = new byte[expected.length];
    try (FSDataInputStream fin = ghfs.open(hadoopPath)) {
      fin.readFully(0, actual);
    }

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void append_shouldAppendNewData() throws Exception {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);

    ghfsHelper.writeTextFile(path.getAuthority(), path.getPath(), "original-content");

    // Test appending three 9-character strings to existing object using 20 bytes buffer size
    try (FSDataOutputStream os = ghfs.append(hadoopPath, 20, /* progress= */ () -> {})) {
      os.write("_append-1".getBytes(UTF_8));

      // Validate that file content didn't change after write call
      assertThat(ghfsHelper.readTextFile(hadoopPath)).isEqualTo("original-content");

      os.hsync();

      // Validate that hsync persisted data
      assertThat(ghfsHelper.readTextFile(hadoopPath)).isEqualTo("original-content_append-1");

      os.write("_append-2".getBytes(UTF_8));
      os.write("_append-3".getBytes(UTF_8));
    }

    String expectedContent = "original-content_append-1_append-2_append-3";

    assertThat(ghfsHelper.readTextFile(hadoopPath)).isEqualTo(expectedContent);

    // Check if file after appending has right size
    assertThat(ghfs.getFileStatus(hadoopPath).getLen()).isEqualTo(expectedContent.length());
  }

  @Test
  public void append_shouldFail_whenFileDoesNotExist() throws Exception {
    URI path = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path hadoopPath = ghfsHelper.castAsHadoopPath(path);

    // Test appending three 9-character strings to existing object using 20 bytes buffer size
    FSDataOutputStream fsos = ghfs.append(hadoopPath, 20, /* progress= */ () -> {});
    fsos.write("_append-1".getBytes(UTF_8));

    assertThrows(GoogleJsonResponseException.class, fsos::hsync);

    assertThrows(NullPointerException.class, fsos::close);

    // Validate that file wasn't created
    assertThat(ghfs.exists(hadoopPath)).isFalse();
  }
}
