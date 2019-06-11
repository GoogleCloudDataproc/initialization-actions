/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import java.io.EOFException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleHadoopFSInputStreamIntegrationTest {
  private static GoogleCloudStorageFileSystemIntegrationHelper gcsFsIHelper;

  @BeforeClass
  public static void beforeClass() throws Exception {
    gcsFsIHelper =
        GoogleCloudStorageFileSystemIntegrationHelper.create(
            GoogleHadoopFileSystemIntegrationHelper.APP_NAME);
    gcsFsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() {
    gcsFsIHelper.afterAllTests();
  }

  @Test
  public void testSeek_illegalArgument() throws Exception {
    StorageResourceId testFile =
        new StorageResourceId(
            gcsFsIHelper.sharedBucketName1, "GHFSInputStream_testSeek_illegalArgument");
    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            testFile.toString(), GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(testFile.getBucketName(), testFile.getObjectName(), testContent);

    GoogleHadoopFSInputStream in =
        new GoogleHadoopFSInputStream(
            ghfs,
            new URI(testFile.toString()),
            GoogleCloudStorageReadOptions.DEFAULT,
            new FileSystem.Statistics(ghfs.getScheme()));

    Throwable exception = assertThrows(EOFException.class, () -> in.seek(testContent.length()));
    assertThat(exception).hasMessageThat().contains("Invalid seek offset");
  }

  @Test
  public void testRead() throws Exception {
    StorageResourceId testFile =
        new StorageResourceId(gcsFsIHelper.sharedBucketName1, "GHFSInputStream_testRead");
    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            testFile.toString(), GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(testFile.getBucketName(), testFile.getObjectName(), testContent);

    GoogleHadoopFSInputStream in =
        new GoogleHadoopFSInputStream(
            ghfs,
            new URI(testFile.toString()),
            GoogleCloudStorageReadOptions.DEFAULT,
            new FileSystem.Statistics(ghfs.getScheme()));

    byte[] value = new byte[2];
    byte[] expected = Arrays.copyOf(testContent.getBytes(StandardCharsets.UTF_8), 2);

    assertThat(in.read(value, 0, 1)).isEqualTo(1);
    assertThat(in.read(1, value, 1, 1)).isEqualTo(1);
    assertThat(value).isEqualTo(expected);
  }

  @Test
  public void testAvailable() throws Exception {
    StorageResourceId testFile =
        new StorageResourceId(gcsFsIHelper.sharedBucketName1, "GHFSInputStream_testAvailable");
    GoogleHadoopFileSystem ghfs =
        GoogleHadoopFileSystemIntegrationHelper.createGhfs(
            testFile.toString(), GoogleHadoopFileSystemIntegrationHelper.getTestConfig());

    String testContent = "test content";
    gcsFsIHelper.writeTextFile(testFile.getBucketName(), testFile.getObjectName(), testContent);

    GoogleHadoopFSInputStream in =
        new GoogleHadoopFSInputStream(
            ghfs,
            new URI(testFile.toString()),
            GoogleCloudStorageReadOptions.DEFAULT,
            new FileSystem.Statistics(ghfs.getScheme()));

    assertThat(in.available()).isEqualTo(0);
    in.close();
    assertThrows(ClosedChannelException.class, in::available);
  }
}
