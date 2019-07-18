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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_EMAIL;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_KEY_FILE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_LAZY_INITIALIZATION_ENABLE;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.PATH_CODEC;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertThrows;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for GoogleHadoopFileSystem class.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemNewUriFormatIntegrationTest
    extends GoogleHadoopFileSystemIntegrationTest {

  @ClassRule
  public static NotInheritableExternalResource storageResource =
      new NotInheritableExternalResource(GoogleHadoopFileSystemNewUriFormatIntegrationTest.class) {
        @Override
        public void before() throws Throwable {
          GoogleHadoopFileSystem testInstance = new GoogleHadoopFileSystem();
          ghfs = testInstance;
          ghfsFileSystemDescriptor = testInstance;

          // loadConfig needs ghfsHelper, which is normally created in
          // postCreateInit. Create one here for it to use.
          ghfsHelper = new HadoopFileSystemIntegrationHelper(ghfs, ghfsFileSystemDescriptor);
          Configuration conf = loadConfig();
          conf.set(PATH_CODEC.getKey(), GoogleHadoopFileSystemBase.PATH_CODEC_USE_URI_ENCODING);
          ghfs.initialize(getInitUri(), conf);

          if (GCS_LAZY_INITIALIZATION_ENABLE.get(ghfs.getConf(), ghfs.getConf()::getBoolean)) {
            testInstance.getGcsFs();
          }

          HadoopFileSystemTestBase.postCreateInit();
        }

        @Override
        public void after() {
          GoogleHadoopFileSystemTestBase.storageResource.after();
        }
      };

  @Before
  public void clearFileSystemCache() throws IOException {
    FileSystem.closeAll();
  }

  @Test
  public void testGlobStatusWithNewUriScheme() throws IOException {
    Path globRoot = new Path("/newuriencoding_globs/");
    ghfs.mkdirs(globRoot);
    ghfs.mkdirs(new Path("/newuriencoding_globs/subdirectory1"));
    ghfs.mkdirs(new Path("/newuriencoding_globs/#this#is#a&subdir/"));

    byte[] data = new byte[10];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
    }

    createFile(new Path("/newuriencoding_globs/subdirectory1/file1"), data);
    createFile(new Path("/newuriencoding_globs/subdirectory1/file2"), data);
    createFile(new Path("/newuriencoding_globs/#this#is#a&subdir/file1"), data);
    createFile(new Path("/newuriencoding_globs/#this#is#a&subdir/file2"), data);
    createFile(new Path("/newuriencoding_globs/#this#is#a&subdir/file2"), data);

    FileStatus[] rootDirectories =
        ghfs.globStatus(new Path("/new*"));
    assertThat(rootDirectories).hasLength(1);
    assertThat(rootDirectories[0].getPath().getName()).isEqualTo("newuriencoding_globs");

    FileStatus[] subDirectories =
        ghfs.globStatus(new Path("/newuriencoding_globs/s*"));
    assertThat(subDirectories).hasLength(1);
    assertThat(subDirectories[0].getPath().getName()).isEqualTo("subdirectory1");

    FileStatus[] subDirectories2 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this*"));
    assertThat(subDirectories2).hasLength(1);
    assertThat(subDirectories2[0].getPath().getName()).isEqualTo("#this#is#a&subdir");

    FileStatus[] subDirectories3 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this?is?a&*"));
    assertThat(subDirectories3).hasLength(1);
    assertThat(subDirectories3[0].getPath().getName()).isEqualTo("#this#is#a&subdir");

    FileStatus[] subDirectory1Files =
        ghfs.globStatus(new Path("/newuriencoding_globs/subdirectory1/*"));
    assertThat(subDirectory1Files).hasLength(2);
    assertThat(subDirectory1Files[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory1Files[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/f*"));
    assertThat(subDirectory2Files).hasLength(2);
    assertThat(subDirectory2Files[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files2 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/file?"));
    assertThat(subDirectory2Files2).hasLength(2);
    assertThat(subDirectory2Files2[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files2[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files3 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/file[0-9]"));
    assertThat(subDirectory2Files3).hasLength(2);
    assertThat(subDirectory2Files3[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files3[1].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files4 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/file[^1]"));
    assertThat(subDirectory2Files4).hasLength(1);
    assertThat(subDirectory2Files4[0].getPath().getName()).isEqualTo("file2");

    FileStatus[] subDirectory2Files5 =
        ghfs.globStatus(new Path("/newuriencoding_globs/#this#is#a&subdir/file{1,2}"));
    assertThat(subDirectory2Files5).hasLength(2);
    assertThat(subDirectory2Files5[0].getPath().getName()).isEqualTo("file1");
    assertThat(subDirectory2Files5[1].getPath().getName()).isEqualTo("file2");

    ghfs.delete(globRoot, true);
  }

  @Test
  public void testPathsOnlyValidInNewUriScheme() throws IOException {
    GoogleHadoopFileSystem typedFs = (GoogleHadoopFileSystem) ghfs;

    Path directory = new Path(
        String.format(
            "gs://%s/testPathsOnlyValidInNewUriScheme/", typedFs.getRootBucketName()));
    Path p = new Path(directory, "foo#bar#baz");
    assertThrows(FileNotFoundException.class, () -> ghfs.getFileStatus(p));

    ghfsHelper.writeFile(p, "SomeText", 100, /* overwrite= */ false);

    FileStatus status = ghfs.getFileStatus(p);
    assertThat(status.getPath()).isEqualTo(p);
    ghfs.delete(directory, true);
  }

  @Test
  public void testPathsAreCompatibleWhenPossible() throws IOException {
    GoogleHadoopFileSystem uriPathEncodedFS = (GoogleHadoopFileSystem) ghfs;
    String rootBucketName = uriPathEncodedFS.getRootBucketName();

    GoogleHadoopFileSystem legacyEncodedFS = new GoogleHadoopFileSystem();
    Configuration conf = uriPathEncodedFS.getConf();
    conf.set(PATH_CODEC.getKey(), GoogleHadoopFileSystemBase.PATH_CODEC_USE_LEGACY_ENCODING);
    legacyEncodedFS.initialize(URI.create("gs://" + rootBucketName), conf);

    Path compatTestRoot =
        new Path(String.format("gs://%s/testPathsAreCompatibleWhenPossible/", rootBucketName));

    Path compatPath = new Path(compatTestRoot, "simple!@$().foo");
    verifyCompat(uriPathEncodedFS, legacyEncodedFS, compatPath);

    ghfs.delete(compatTestRoot, true);
  }

  private static void verifyCompat(
      GoogleHadoopFileSystem newUriEncodingFS, GoogleHadoopFileSystem legacyEncodingFS, Path path)
      throws IOException {
    String fileContent = "TestText" + UUID.randomUUID();
    writeFile(newUriEncodingFS, path, fileContent);

    assertWithMessage("When checking compat path %s, fileContent was not read.", path)
        .that(readFile(legacyEncodingFS, path))
        .isEqualTo(fileContent);
  }

  @Override
  public void testGetGcsPath() throws URISyntaxException {
    GoogleHadoopFileSystem myghfs = (GoogleHadoopFileSystem) ghfs;
    URI gcsPath = new URI("gs://" + myghfs.getRootBucketName() + "/dir/obj");
    URI convertedPath = myghfs.getGcsPath(new Path(gcsPath));
    assertThat(convertedPath).isEqualTo(gcsPath);

    // When using the LegacyPathCodec this will fail, but it's perfectly fine to encode
    // this in the UriPathCodec. Note that new Path("/buck^et", "object")
    // isn't actually using bucket as a bucket, but instead as a part of the path...
    myghfs.getGcsPath(new Path("/buck^et", "object"));

    // Validate that authorities can't be crazy:
    Path invalidBucketPath = new Path("gs://buck^et/object");
    assertThrows(IllegalArgumentException.class, () -> myghfs.getGcsPath(invalidBucketPath));
  }

  @Test
  public void testLegacyPathCodecCanBeChosen() throws IOException, URISyntaxException {
    GoogleHadoopFileSystem lazyGhfs = new GoogleHadoopFileSystem();
    Configuration conf = new Configuration(ghfs.getConf());
    conf.set(PATH_CODEC.getKey(), GoogleHadoopFileSystemBase.PATH_CODEC_USE_LEGACY_ENCODING);
    URI initUri = getInitUri();
    lazyGhfs.initialize(initUri, conf);

    Path filePath = new Path(URI.create(getInitUri() + "/testLegacyPathCodecCanBeChosen"));
    String fileContent = "testLegacyPathCodecCanBeChosen-test-content";
    writeFile(lazyGhfs, filePath, fileContent);

    assertThat(readFile(lazyGhfs, filePath)).isEqualTo(fileContent);

    lazyGhfs.delete(filePath, /* recursive= */ false);
  }

  @Test
  public void testUnknownPathCodecCanBeSet() throws IOException, URISyntaxException {
    GoogleHadoopFileSystem testGhfs = new GoogleHadoopFileSystem();
    Configuration conf = new Configuration(ghfs.getConf());
    conf.set(PATH_CODEC.getKey(), "unknown");
    testGhfs.initialize(getInitUri(), conf);

    Path filePath = new Path(URI.create(getInitUri() + "/testUnknownPathCodecCanBeSet"));
    String fileContent = "testUnknownPathCodecCanBeSet-test-content";
    writeFile(testGhfs, filePath, fileContent);

    assertThat(readFile(testGhfs, filePath)).isEqualTo(fileContent);

    testGhfs.delete(filePath, /* recursive= */ true);
  }

  @Test
  public void testGcsLazyConfigurationEnabled() throws IOException, URISyntaxException {
    GoogleHadoopFileSystem lazyGhfs = new GoogleHadoopFileSystem();
    Configuration conf = new Configuration(ghfs.getConf());
    conf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), true);
    lazyGhfs.initialize(getInitUri(), conf);

    Path filePath = new Path(URI.create(getInitUri() + "/testGcsLazyConfigurationEnabled"));
    String fileContent = "testGcsLazyConfigurationEnabled-test-content";
    writeFile(lazyGhfs, filePath, fileContent);

    assertThat(readFile(lazyGhfs, filePath)).isEqualTo(fileContent);

    lazyGhfs.delete(filePath, /* recursive= */ true);
  }

  @Test
  public void testExceptionIsThrownDuringConfigurationWhenCannotCreateGcsFs()
      throws IOException, URISyntaxException {
    GoogleHadoopFileSystem lazyGhfs = new GoogleHadoopFileSystem();
    Configuration conf = new Configuration();
    conf.setBoolean(GCS_LAZY_INITIALIZATION_ENABLE.getKey(), true);
    conf.setBoolean(AUTH_SERVICE_ACCOUNT_ENABLE.getKey(), true);
    conf.set(AUTH_SERVICE_ACCOUNT_EMAIL.getKey(), "account-email@example.com");
    conf.set(AUTH_SERVICE_ACCOUNT_KEY_FILE.getKey(), "not-existent.key");
    lazyGhfs.initialize(getInitUri(), conf);

    RuntimeException e = assertThrows(RuntimeException.class, lazyGhfs::getGcsFs);

    assertThat(e).hasMessageThat().contains("Failed to create GCS FS");
  }

  private static void writeFile(FileSystem fs, Path path, String content) throws IOException {
    try (FSDataOutputStream outputStream = fs.create(path, /* overwrite= */ false)) {
      outputStream.write(content.getBytes(UTF_8));
    }
  }

  private static String readFile(FileSystem fs, Path path) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path), UTF_8))) {
      return reader.lines().collect(joining());
    }
  }

  private static URI getInitUri() throws URISyntaxException {
    return new URI("gs://" + ghfsHelper.getUniqueBucketName("init"));
  }
}
