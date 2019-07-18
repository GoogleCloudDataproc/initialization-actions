/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.BLOCK_SIZE;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryNotEmptyException;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link GoogleHadoopFS} class. */
@RunWith(JUnit4.class)
public class GoogleHadoopFSIntegrationTest {

  private static GoogleCloudStorageFileSystemIntegrationHelper gcsFsIHelper;
  private static URI initUri;

  @BeforeClass
  public static void beforeClass() throws Exception {
    gcsFsIHelper =
        GoogleCloudStorageFileSystemIntegrationHelper.create(
            GoogleHadoopFileSystemIntegrationHelper.APP_NAME);
    gcsFsIHelper.beforeAllTests();
    initUri = new URI("gs://" + gcsFsIHelper.sharedBucketName1);
  }

  @AfterClass
  public static void afterClass() {
    gcsFsIHelper.afterAllTests();
  }

  @After
  public void after() throws IOException {
    FileSystem.closeAll();
  }

  @Test
  public void testInitializationWithUriAndConf_shouldGiveFsStatusWithNotUsedMemory()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    assertThat(ghfs.getFsStatus().getUsed()).isEqualTo(0);
  }

  @Test
  public void testInitializationWithGhfsUriAndConf_shouldGiveFsStatusWithNotUsedMemory()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(new GoogleHadoopFileSystem(), initUri, config);

    assertThat(ghfs.getFsStatus().getUsed()).isEqualTo(0);
  }

  @Test
  public void testCreateInternal_shouldCreateParent() throws Exception {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    Path filePath =
        new Path(initUri.resolve("/testCreateInternal_shouldCreateParent/dir/file").toString());

    try (FSDataOutputStream stream =
        ghfs.createInternal(
            filePath,
            EnumSet.of(CreateFlag.CREATE),
            /* absolutePermission= */ null,
            /* bufferSize= */ 128,
            /* replication= */ (short) 1,
            /* blockSize= */ 32,
            () -> {},
            new Options.ChecksumOpt(),
            /* createParent= */ true)) {
      stream.write(1);

      assertThat(stream.size()).isEqualTo(1);
    }

    FileStatus parentStatus = ghfs.getFileStatus(filePath.getParent());
    assertThat(parentStatus.getModificationTime()).isGreaterThan(0L);
  }

  @Test
  public void testCreateInternal_shouldNotCreateParent() throws Exception {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    Path filePath =
        new Path(initUri.resolve("/testCreateInternal_shouldNotCreateParent/dir/file").toString());

    try (FSDataOutputStream stream =
        ghfs.createInternal(
            filePath,
            EnumSet.of(CreateFlag.CREATE),
            /* absolutePermission= */ null,
            /* bufferSize= */ 128,
            /* replication= */ (short) 1,
            /* blockSize= */ 32,
            () -> {},
            new Options.ChecksumOpt(),
            /* createParent= */ false)) {
      stream.write(1);

      assertThat(stream.size()).isEqualTo(1);
    }

    // GoogleHadoopFS ignores 'createParent' flag and always creates parent
    FileStatus parentStatus = ghfs.getFileStatus(filePath.getParent().getParent());
    assertThat(parentStatus.getModificationTime()).isGreaterThan(0L);
  }

  @Test
  public void testGetUriDefaultPort_shouldBeEqualToGhfsDefaultPort()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    assertThat(ghfs.getUriDefaultPort()).isEqualTo(-1);
  }

  @Test
  public void testGetUri_shouldBeEqualToGhfsUri() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    assertThat(ghfs.getUri()).isEqualTo(initUri.resolve("/"));
  }

  @Test
  public void testValidName_shouldNotContainPoints() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    assertThat(ghfs.isValidName("gs://test//")).isTrue();
    assertThat(ghfs.isValidName("hdfs://test//")).isTrue();
    assertThat(ghfs.isValidName("gs//test/../")).isFalse();
    assertThat(ghfs.isValidName("gs//test//.")).isFalse();
  }

  @Test
  public void testCheckPath_shouldThrowExceptionForMismatchingBucket()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    Path testPath = new Path("gs://fake/file");

    InvalidPathException e =
        assertThrows(InvalidPathException.class, () -> ghfs.checkPath(testPath));

    assertThat(e).hasMessageThat().startsWith("Invalid path");
  }

  @Test
  public void getServerDefaults_shouldReturnSpecifiedConfiguration() throws Exception {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    config.setLong(BLOCK_SIZE.getKey(), 1);
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    FsServerDefaults defaults = ghfs.getServerDefaults();

    assertThat(defaults.getBlockSize()).isEqualTo(1);
  }

  @Test
  public void testMkdirs_shouldReturnDefaultFilePermissions()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    config.set("fs.gs.reported.permissions", "357");
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    FsPermission permission = new FsPermission("000");
    FsPermission expectedPermission = new FsPermission("357");

    Path path = new Path(initUri.resolve("/testMkdirs_shouldRespectFilePermissions").toString());
    ghfs.mkdir(path, permission, /* createParent= */ true);

    assertThat(ghfs.getFileStatus(path).getPermission()).isEqualTo(expectedPermission);
  }

  @Test
  public void testDeleteRecursive_shouldDeleteAllInPath() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    FsPermission permission = new FsPermission("000");

    URI parentDir = initUri.resolve("/testDeleteRecursive_shouldDeleteAllInPath");
    Path testDir = new Path(parentDir.resolve("test_dir").toString());
    URI testFile = parentDir.resolve("test_file");
    Path testFilePath = new Path(testFile.toString());

    ghfs.mkdir(testDir, permission, /* createParent= */ true);
    gcsFsIHelper.writeTextFile(initUri.getAuthority(), testFile.getPath(), "file data");

    assertThat(ghfs.getFileStatus(testDir)).isNotNull();
    assertThat(ghfs.getFileStatus(testFilePath)).isNotNull();
    assertThat(ghfs.getFileStatus(testDir.getParent())).isNotNull();

    ghfs.delete(testDir.getParent(), /* recursive= */ true);

    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(testDir));
    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(testFilePath));
    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(testDir.getParent()));
  }

  @Test
  public void testDeleteNotRecursive_shouldBeAppliedToHierarchyOfDirectories()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    FsPermission permission = new FsPermission("000");

    URI parentDir = initUri.resolve("/testDeleteRecursive_shouldDeleteAllInPath");
    Path testDir = new Path(parentDir.resolve("test_dir").toString());

    ghfs.mkdir(testDir, permission, /* createParent= */ true);

    assertThrows(
        DirectoryNotEmptyException.class,
        () -> ghfs.delete(testDir.getParent(), /* recursive= */ false));
  }

  @Test
  public void testGetFileStatus_shouldReturnDetails() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    URI testFile = initUri.resolve("/testGetFileStatus_shouldReturnDetails");
    Path testFilePath = new Path(testFile.toString());

    gcsFsIHelper.writeTextFile(testFile.getAuthority(), testFile.getPath(), "file content");

    FileStatus fileStatus = ghfs.getFileStatus(testFilePath);
    assertThat(fileStatus.getReplication()).isEqualTo(3);
  }

  @Test
  public void testGetFileBlockLocations_shouldReturnLocalhost()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    URI testFile = initUri.resolve("/testGetFileBlockLocations_shouldReturnLocalhost");
    Path testFilePath = new Path(testFile.toString());

    gcsFsIHelper.writeTextFile(testFile.getAuthority(), testFile.getPath(), "file content");

    BlockLocation[] fileBlockLocations = ghfs.getFileBlockLocations(testFilePath, 1, 1);
    assertThat(fileBlockLocations).hasLength(1);
    assertThat(fileBlockLocations[0].getHosts()).isEqualTo(new String[] {"localhost"});
  }

  @Test
  public void testListStatus_shouldReturnOneStatus() throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    URI testFile = initUri.resolve("/testListStatus_shouldReturnOneStatus");
    Path testFilePath = new Path(testFile.toString());

    assertThrows(FileNotFoundException.class, () -> ghfs.listStatus(testFilePath));

    gcsFsIHelper.writeTextFile(testFile.getAuthority(), testFile.getPath(), "file content");

    assertThat(ghfs.listStatus(testFilePath)).hasLength(1);
  }

  @Test
  public void testRenameInternal_shouldMakeOldPathNotFound()
      throws IOException, URISyntaxException {
    Configuration config = GoogleHadoopFileSystemIntegrationHelper.getTestConfig();
    GoogleHadoopFS ghfs = new GoogleHadoopFS(initUri, config);

    URI srcFile = initUri.resolve("/testRenameInternal_shouldMakeOldPathNotFound/src");
    Path srcPath = new Path(srcFile);
    URI dstFile = initUri.resolve("/testRenameInternal_shouldMakeOldPathNotFound/dst");
    Path dstPath = new Path(dstFile);

    gcsFsIHelper.writeTextFile(srcFile.getAuthority(), srcFile.getPath(), "file content");

    assertThat(ghfs.listStatus(srcPath)).hasLength(1);
    assertThrows(FileNotFoundException.class, () -> ghfs.listStatus(dstPath));

    ghfs.renameInternal(srcPath, dstPath);

    assertThrows(FileNotFoundException.class, () -> ghfs.listStatus(srcPath));
    assertThat(ghfs.listStatus(dstPath)).hasLength(1);
  }
}
