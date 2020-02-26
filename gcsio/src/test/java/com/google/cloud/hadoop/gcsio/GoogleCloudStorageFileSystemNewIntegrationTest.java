/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;

import static com.google.cloud.hadoop.gcsio.FileInfo.fromItemInfo;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo.createInferredDirectory;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.copyRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.uploadRequestString;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link GoogleCloudStorageFileSystem} class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemNewIntegrationTest {

  private static GoogleCloudStorageOptions gcsOptions;
  private static RetryHttpInitializer httpRequestsInitializer;
  private static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void before() throws Throwable {
    String projectId =
        checkNotNull(TestConfiguration.getInstance().getProjectId(), "projectId can not be null");
    String appName = GoogleCloudStorageIntegrationHelper.APP_NAME;
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    gcsOptions =
        GoogleCloudStorageOptions.builder().setAppName(appName).setProjectId(projectId).build();
    httpRequestsInitializer =
        new RetryHttpInitializer(
            credential,
            gcsOptions.getAppName(),
            gcsOptions.getMaxHttpRequestRetries(),
            gcsOptions.getHttpRequestConnectTimeout(),
            gcsOptions.getHttpRequestReadTimeout(),
            gcsOptions.getHttpRequestHeaders());

    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem(
            credential,
            GoogleCloudStorageFileSystemOptions.builder()
                .setBucketDeleteEnabled(true)
                .setCloudStorageOptions(gcsOptions)
                .build());

    gcsfsIHelper = new GoogleCloudStorageFileSystemIntegrationHelper(gcsfs);
    gcsfsIHelper.beforeAllTests();
  }

  @AfterClass
  public static void afterClass() throws Throwable {
    gcsfsIHelper.afterAllTests();
    GoogleCloudStorageFileSystem gcsfs = gcsfsIHelper.gcsfs;
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName1))).isFalse();
    assertThat(gcsfs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName2))).isFalse();
  }

  @Test
  public void mkdirs_shouldCreateNewDirectory() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve(dirObject);

    gcsFs.mkdir(dirObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            uploadRequestString(bucketName, dirObject + "/", /* generationId= */ null));

    assertThat(gcsFs.exists(dirObjectUri)).isTrue();
    assertThat(gcsFs.getFileInfo(dirObjectUri).isDirectory()).isTrue();
  }

  @Test
  public void getFileInfo_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(dirObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject), getRequestString(bucketName, dirObject + "/"));

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getFileInfo_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(true).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(dirObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            getRequestString(bucketName, dirObject + "/"),
            listRequestString(
                bucketName, dirObject + "/", /* maxResults= */ 2, /* pageToken= */ null));

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getFileInfo_single_file_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String fileObject = getTestResource() + "/f1";
    URI fileObjectUri = new URI("gs://" + bucketName).resolve("/" + fileObject);

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, fileObject);

    FileInfo fileInfo = gcsFs.getFileInfo(fileObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(getRequestString(bucketName, fileObject));

    assertThat(fileInfo.exists()).isTrue();
    assertThat(fileInfo.getPath()).isEqualTo(fileObjectUri);
  }

  @Test
  public void getDirInfo_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject + "/"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(getRequestString(bucketName, dirObject + "/"));

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getDirInfo_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(true).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = getTestResource();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve("/" + dirObject);

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/file1", dirObject + "/file2", dirObject + "/file3");

    FileInfo dirInfo = gcsFs.getFileInfo(new URI("gs://" + bucketName).resolve(dirObject + "/"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/"),
            listRequestString(
                bucketName, dirObject + "/", /* maxResults= */ 2, /* pageToken= */ null));

    assertThat(dirInfo.exists()).isTrue();
    assertThat(dirInfo.getPath().toString()).isEqualTo(dirObjectUri + "/");
  }

  @Test
  public void getFileInfos_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/f1", dirObject + "/f2", dirObject + "/subdir/f3");

    List<FileInfo> fileInfos =
        gcsFs.getFileInfos(
            ImmutableList.of(
                bucketUri.resolve(dirObject + "/f1"),
                bucketUri.resolve(dirObject + "/f2"),
                bucketUri.resolve(dirObject + "/subdir/f3")));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/f1"),
            bucketUri.resolve(dirObject + "/f2"),
            bucketUri.resolve(dirObject + "/subdir/f3"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/f1"),
            getRequestString(bucketName, dirObject + "/f2"),
            getRequestString(bucketName, dirObject + "/subdir/f3"));
  }

  @Test
  public void getFileInfos_parallel() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs =
        newGcsFs(newGcsFsOptions().setStatusParallelEnabled(true).build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/f1", dirObject + "/f2", dirObject + "/subdir/f3");

    List<FileInfo> fileInfos =
        gcsFs.getFileInfos(
            ImmutableList.of(
                bucketUri.resolve(dirObject + "/f1"),
                bucketUri.resolve(dirObject + "/f2"),
                bucketUri.resolve(dirObject + "/subdir/f3")));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/f1"),
            bucketUri.resolve(dirObject + "/f2"),
            bucketUri.resolve(dirObject + "/subdir/f3"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(bucketName, dirObject + "/f1/", 2, null),
            getRequestString(bucketName, dirObject + "/f1"),
            getRequestString(bucketName, dirObject + "/f1/"),
            listRequestString(bucketName, dirObject + "/f2/", 2, null),
            getRequestString(bucketName, dirObject + "/f2"),
            getRequestString(bucketName, dirObject + "/f2/"),
            listRequestString(bucketName, dirObject + "/subdir/f3/", 2, null),
            getRequestString(bucketName, dirObject + "/subdir/f3"),
            getRequestString(bucketName, dirObject + "/subdir/f3/"));
  }

  @Test
  public void listFileNames() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/f1", dirObject + "/f2", dirObject + "/subdir/f3");

    List<URI> listedNames =
        gcsFs.listFileNames(inferredDirInfo(bucketName, dirObject), /* recursive= */ false);

    assertThat(listedNames)
        .containsExactly(
            bucketUri.resolve(dirObject + "/f1"),
            bucketUri.resolve(dirObject + "/f2"),
            bucketUri.resolve(dirObject + "/subdir/"));
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(listRequestString(bucketName, false, dirObject + "/", 1024, null));
  }

  @Test
  public void listFileNames_recursive() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(
        bucketName, dirObject + "/f1", dirObject + "/f2", dirObject + "/subdir/f3");

    List<URI> listedNames =
        gcsFs.listFileNames(inferredDirInfo(bucketName, dirObject), /* recursive= */ true);

    assertThat(listedNames)
        .containsExactly(
            bucketUri.resolve(dirObject + "/f1"),
            bucketUri.resolve(dirObject + "/f2"),
            bucketUri.resolve(dirObject + "/subdir/"),
            bucketUri.resolve(dirObject + "/subdir/f3"));
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(listRequestString(bucketName, dirObject + "/", 1024));
  }

  @Test
  public void listFileInfo_single_file_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String fileObject = getTestResource();
    URI fileObjectUri = new URI("gs://" + bucketName + "/").resolve(fileObject);

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, fileObject);

    List<FileInfo> fileInfos = gcsFs.listFileInfo(fileObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(getRequestString(bucketName, fileObject));

    assertThat(fileInfos).hasSize(1);
    FileInfo fileInfo = fileInfos.get(0);
    assertThat(fileInfo.exists()).isTrue();
    assertThat(fileInfo.getPath()).isEqualTo(fileObjectUri);
  }

  @Test
  public void listFileInfo_file_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(true).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String fileObject = getTestResource();
    URI fileObjectUri = new URI("gs://" + bucketName + "/").resolve(fileObject);

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, fileObject);

    List<FileInfo> fileInfos = gcsFs.listFileInfo(fileObjectUri);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, fileObject),
            getRequestString(bucketName, fileObject + "/"),
            listRequestString(
                bucketName,
                /* includeTrailingDelimiter= */ true,
                fileObject + "/",
                /* maxResults= */ 1024,
                /* pageToken= */ null));

    assertThat(fileInfos).hasSize(1);
    FileInfo fileInfo = fileInfos.get(0);
    assertThat(fileInfo.exists()).isTrue();
    assertThat(fileInfo.getPath()).isEqualTo(fileObjectUri);
  }

  @Test
  public void listFileInfo_directory_sequential() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(newGcsFsOptions().build(), gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/file1", dirObject + "/file2");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            getRequestString(bucketName, dirObject + "/"),
            listRequestString(
                bucketName,
                /* includeTrailingDelimiter= */ true,
                dirObject + "/",
                /* maxResults= */ 1024,
                /* pageToken= */ null));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/file1"), bucketUri.resolve(dirObject + "/file2"));
  }

  @Test
  public void listFileInfo_directory_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().setStatusParallelEnabled(true).build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/file1", dirObject + "/file2");

    List<FileInfo> fileInfos = gcsFs.listFileInfo(bucketUri.resolve(dirObject));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject),
            getRequestString(bucketName, dirObject + "/"),
            listRequestString(
                bucketName,
                /* includeTrailingDelimiter= */ true,
                dirObject + "/",
                /* maxResults= */ 1024,
                /* pageToken= */ null));

    assertThat(fileInfos.stream().map(FileInfo::exists).collect(toList()))
        .containsExactly(true, true);
    assertThat(fileInfos.stream().map(FileInfo::getPath).collect(toList()))
        .containsExactly(
            bucketUri.resolve(dirObject + "/file1"), bucketUri.resolve(dirObject + "/file2"));
  }

  @Test
  public void delete_file_sequential() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions()
            .build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.delete(bucketUri.resolve(dirObject + "/f1"), /* recursive= */ false);

    // This test performs additional POST request to dirObject when run singly, as result
    // generationId for delete operation need to be increased for DELETE request.
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/"),
            getRequestString(bucketName, dirObject + "/f1"),
            deleteRequestString(bucketName, dirObject + "/f1", 1));

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
  }

  @Test
  public void delete_file_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions()
            .setStatusParallelEnabled(true)
            .build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.delete(bucketUri.resolve(dirObject + "/f1"), /* recursive= */ false);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/"),
            listRequestString(bucketName, false, dirObject + "/", 2, null),
            getRequestString(bucketName, dirObject + "/f1"),
            getRequestString(bucketName, dirObject + "/f1/"),
            listRequestString(bucketName, false, dirObject + "/f1/", 2, null),
            deleteRequestString(bucketName, dirObject + "/f1", 1));

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
  }

  @Test
  public void rename_file_sequential() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions()
            .build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.rename(bucketUri.resolve(dirObject + "/f1"), bucketUri.resolve(dirObject + "/f2"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/f1"),
            getRequestString(bucketName, dirObject + "/f2"),
            getRequestString(bucketName, dirObject + "/f2/"),
            listRequestString(bucketName, false, dirObject + "/f2/", 2, null),
            getRequestString(bucketName, dirObject + "/"),
            getRequestString(bucketName, dirObject + "/f2/"),
            listRequestString(bucketName, false, dirObject + "/f2/", 2, null),
            getRequestString(bucketName, dirObject + "/"),
            copyRequestString(
                bucketName, dirObject + "/f1", bucketName, dirObject + "/f2", "copyTo"),
            deleteRequestString(bucketName, dirObject + "/f1", 1));

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f2"))).isTrue();
  }

  @Test
  public void rename_file_parallel() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions()
            .setStatusParallelEnabled(true)
            .build();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirObject = getTestResource();

    gcsfsIHelper.createObjectsWithSubdirs(bucketName, dirObject + "/f1");

    gcsFs.rename(bucketUri.resolve(dirObject + "/f1"), bucketUri.resolve(dirObject + "/f2"));

    // This test performs additional POST request to dirObject when run singly, as result
    // generationId for delete operation need to be increased for DELETE request.
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(bucketName, dirObject + "/f1"),
            getRequestString(bucketName, dirObject + "/f1/"),
            listRequestString(bucketName, false, dirObject + "/f1/", 2, null),
            getRequestString(bucketName, dirObject + "/f2"),
            getRequestString(bucketName, dirObject + "/f2/"),
            listRequestString(bucketName, false, dirObject + "/f2/", 2, null),
            getRequestString(bucketName, dirObject + "/"),
            listRequestString(bucketName, false, dirObject + "/", 2, null),
            getRequestString(bucketName, dirObject + "/f2/"),
            listRequestString(bucketName, false, dirObject + "/f2/", 2, null),
            getRequestString(bucketName, dirObject + "/"),
            listRequestString(bucketName, false, dirObject + "/", 2, null),
            copyRequestString(
                bucketName, dirObject + "/f1", bucketName, dirObject + "/f2", "copyTo"),
            deleteRequestString(bucketName, dirObject + "/f1", 1));

    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f1"))).isFalse();
    assertThat(gcsFs.exists(bucketUri.resolve(dirObject + "/f2"))).isTrue();
  }

  private String getTestResource() {
    return name.getMethodName() + "_" + UUID.randomUUID();
  }

  private static FileInfo inferredDirInfo(String bucketName, String dirObject) {
    StorageResourceId resourceId =
        new StorageResourceId(bucketName, dirObject.endsWith("/") ? dirObject : dirObject + "/");
    return fromItemInfo(createInferredDirectory(resourceId));
  }

  private static GoogleCloudStorageFileSystemOptions.Builder newGcsFsOptions() {
    return GoogleCloudStorageFileSystemOptions.builder().setCloudStorageOptions(gcsOptions);
  }

  private static GoogleCloudStorageFileSystem newGcsFs(
      GoogleCloudStorageFileSystemOptions gcsfsOptions,
      TrackingHttpRequestInitializer gcsRequestsTracker)
      throws IOException {
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);
    return new GoogleCloudStorageFileSystem(gcs, gcsfsOptions);
  }
}
