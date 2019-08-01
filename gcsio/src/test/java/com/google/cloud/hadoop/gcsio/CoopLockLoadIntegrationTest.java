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

import static com.google.cloud.hadoop.gcsio.cooplock.CoopLockRecordsDao.LOCK_DIRECTORY;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.stream.Collectors.toList;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for GoogleCloudStorageFileSystem class. */
@RunWith(JUnit4.class)
public class CoopLockLoadIntegrationTest {

  private static GoogleCloudStorageOptions gcsOptions;
  private static RetryHttpInitializer httpRequestInitializer;
  private static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @BeforeClass
  public static void before() throws Throwable {
    String projectId =
        checkNotNull(TestConfiguration.getInstance().getProjectId(), "projectId can not be null");
    String appName = GoogleCloudStorageIntegrationHelper.APP_NAME;
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    gcsOptions =
        GoogleCloudStorageOptions.builder().setAppName(appName).setProjectId(projectId).build();
    httpRequestInitializer =
        new RetryHttpInitializer(
            credential,
            gcsOptions.getAppName(),
            gcsOptions.getMaxHttpRequestRetries(),
            gcsOptions.getHttpRequestConnectTimeout(),
            gcsOptions.getHttpRequestReadTimeout());

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
    GoogleCloudStorageFileSystem gcsFs = gcsfsIHelper.gcsfs;
    assertThat(gcsFs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName1))).isFalse();
    assertThat(gcsFs.exists(new URI("gs://" + gcsfsIHelper.sharedBucketName2))).isFalse();
  }

  @Test
  public void moveDirectory_loadTest() throws Exception {
    GoogleCloudStorageFileSystemOptions gcsFsOptions =
        newGcsFsOptions().toBuilder()
            .setCloudStorageOptions(gcsOptions.toBuilder().setMaxHttpRequestRetries(0).build())
            .build();
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, httpRequestInitializer);

    String bucketName = gcsfsIHelper.createUniqueBucket("coop-load");
    URI bucketUri = new URI("gs://" + bucketName + "/");
    String dirName = "rename_" + UUID.randomUUID();
    String fileNamePrefix = "file_";
    URI srcFileUri = bucketUri.resolve(dirName + "_src");
    URI dstFileUri = bucketUri.resolve(dirName + "_dst");
    URI srcDirUri = bucketUri.resolve(dirName + "_src/");
    URI dstDirUri = bucketUri.resolve(dirName + "_dst/");

    int iterations = 10;

    // create file to rename
    for (int i = 0; i < iterations; i++) {
      gcsfsIHelper.writeTextFile(
          bucketName, srcDirUri.resolve(fileNamePrefix + i).getPath(), "file_content_" + i);
    }

    ExecutorService moveExecutor = Executors.newFixedThreadPool(iterations * 2);
    List<Future<?>> futures = new ArrayList<>(iterations * 2);
    for (int i = 0; i < iterations; i++) {
      URI srcUri1 = i % 4 == 0 ? srcFileUri : srcDirUri;
      URI dstUri1 = i % 4 == 1 ? dstFileUri : dstDirUri;
      futures.add(moveExecutor.submit(() -> renameUnchecked(gcsFs, srcUri1, dstUri1)));

      URI srcUri2 = i % 4 == 3 ? srcFileUri : srcDirUri;
      URI dstUri2 = i % 4 == 2 ? dstFileUri : dstDirUri;
      futures.add(moveExecutor.submit(() -> renameUnchecked(gcsFs, dstUri2, srcUri2)));
    }
    moveExecutor.shutdown();
    moveExecutor.awaitTermination(6, TimeUnit.MINUTES);
    assertWithMessage("Cooperative locking load test timed out")
        .that(moveExecutor.isTerminated())
        .isTrue();

    for (Future<?> f : futures) {
      f.get();
    }

    assertThat(gcsFs.exists(srcDirUri)).isTrue();
    assertThat(gcsFs.exists(dstDirUri)).isFalse();
    assertThat(gcsFs.listFileInfo(srcDirUri)).hasSize(iterations);
    for (int i = 0; i < iterations; i++) {
      assertThat(gcsFs.exists(srcDirUri.resolve(fileNamePrefix + i))).isTrue();
    }

    // Validate lock files
    List<URI> lockFiles =
        gcsFs.listFileInfo(bucketUri.resolve(LOCK_DIRECTORY)).stream()
            .map(FileInfo::getPath)
            .collect(toList());

    assertThat(lockFiles).hasSize(iterations * 4);
  }

  private static void renameUnchecked(GoogleCloudStorageFileSystem gcsFs, URI src, URI dst) {
    do {
      try {
        gcsFs.rename(src, dst);
        break;
      } catch (FileNotFoundException e) {
        assertThat(e).hasMessageThat().matches("^Item not found: " + src + "/?$");
      } catch (IOException e) {
        assertThat(e)
            .hasMessageThat()
            .matches("^Cannot rename because path does not exist: " + src + "/?$");
      }
    } while (true);
  }

  private static GoogleCloudStorageFileSystemOptions newGcsFsOptions() {
    return newGcsFsOptions(CoopLockLoadIntegrationTest.gcsOptions);
  }

  private static GoogleCloudStorageFileSystemOptions newGcsFsOptions(
      GoogleCloudStorageOptions gcsOptions) {
    return GoogleCloudStorageFileSystemOptions.builder()
        .setCloudStorageOptions(gcsOptions)
        .setCooperativeLockingEnabled(true)
        .build();
  }

  private static GoogleCloudStorageFileSystem newGcsFs(
      GoogleCloudStorageFileSystemOptions gcsFsOptions, HttpRequestInitializer requestInitializer)
      throws IOException {
    GoogleCloudStorageImpl gcs =
        new GoogleCloudStorageImpl(gcsFsOptions.getCloudStorageOptions(), requestInitializer);
    return new GoogleCloudStorageFileSystem(gcs, gcsFsOptions);
  }
}
