/**
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.truth.Truth;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for GoogleHadoopFileSystem class.
 */
@RunWith(JUnit4.class)
public class GoogleHadoopFileSystemNewUriFormatIntegrationTest
    extends GoogleHadoopFileSystemIntegrationTest {

  @BeforeClass
  public static void beforeAllTests()
      throws IOException {
    GoogleHadoopFileSystem testInstance = new GoogleHadoopFileSystem();
    ghfs = testInstance;
    ghfsFileSystemDescriptor = testInstance;
    URI initUri;
    try {
      initUri = new URI("gs:/");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }

    // loadConfig needs ghfsHelper, which is normally created in
    // postCreateInit. Create one here for it to use.
    ghfsHelper = new HadoopFileSystemIntegrationHelper(
        ghfs, ghfsFileSystemDescriptor);
    Configuration conf = loadConfig();
    conf.set(
        GoogleHadoopFileSystemBase.PATH_CODEC_KEY,
        GoogleHadoopFileSystemBase.PATH_CODEC_USE_URI_ENCODING);
    ghfs.initialize(initUri, conf);
    HadoopFileSystemTestBase.postCreateInit();
  }

  @Before
  public void clearFileSystemCache() throws IOException {
    FileSystem.closeAll();
  }

  @AfterClass
  public static void afterAllTests()
      throws IOException {
    GoogleHadoopFileSystemTestBase.afterAllTests();
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

    ghfsHelper.writeFile(p, "SomeText", 100, false);

    FileStatus status = ghfs.getFileStatus(p);
    assertThat(status.getPath()).isEqualTo(p);
    ghfs.delete(directory, true);
  }

  @Test
  public void testPathsAreCompatibleWhenPossible() throws IOException {
    GoogleHadoopFileSystem uriPathEncodedFS = (GoogleHadoopFileSystem) ghfs;
    GoogleHadoopFileSystem legacyEncodedFS = new GoogleHadoopFileSystem();
    Configuration conf = uriPathEncodedFS.getConf();
    conf.set(
        GoogleHadoopFileSystemBase.PATH_CODEC_KEY,
        GoogleHadoopFileSystemBase.PATH_CODEC_USE_LEGACY_ENCODING);
    legacyEncodedFS.initialize(URI.create("gs:/"), conf);

    Path compatTestRoot = new Path(
        String.format(
            "gs://%s/testPathsAreCompatibleWhenPossible/", uriPathEncodedFS.getRootBucketName()));

    Path compatPath3 = new Path(compatTestRoot, "simple!@$().foo");
    verifyCompat(uriPathEncodedFS, legacyEncodedFS,  compatPath3);
    ghfs.delete(compatTestRoot, true);
  }

  private static void verifyCompat(
      GoogleHadoopFileSystem newUriEncodingFS,
      GoogleHadoopFileSystem legacyEncodingFS,
      Path compatPath) throws IOException {

    String testText = "TestText" + UUID.randomUUID();
    try (OutputStream os = newUriEncodingFS.create(compatPath, false /* don't overwrite */);
         PrintWriter pw = new PrintWriter(os)) {
      pw.write(testText);
    }

    String line;
    try (InputStream is = legacyEncodingFS.open(compatPath)) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      line = reader.readLine();
    }

    Truth
        .assertWithMessage("When checking compat path %s, testText was not read.", compatPath)
        .that(line)
        .isEqualTo(testText);
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
    assertThrows(
        IllegalArgumentException.class, () -> myghfs.getGcsPath(new Path("gs://buck^et/object")));
  }
}
