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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.MultipartContent;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for GoogleCloudStorageFileSystem class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageFileSystemNewIntegrationTest {

  private static final String UPLOAD_REQUEST_FORMAT =
      "POST:https://www.googleapis.com/upload/storage/v1/b/%s/o?uploadType=multipart:%s";

  private static GoogleCloudStorageOptions gcsOptions;
  private static RetryHttpInitializer httpRequestsInitializer;
  private static GoogleCloudStorageFileSystemIntegrationHelper gcsfsIHelper;

  @BeforeClass
  public static void before() throws Throwable {
    String projectId =
        checkNotNull(TestConfiguration.getInstance().getProjectId(), "projectId can not be null");
    String appName = GoogleCloudStorageIntegrationHelper.APP_NAME;
    Credential credential =
        checkNotNull(GoogleCloudStorageTestHelper.getCredential(), "credential must not be null");

    gcsOptions =
        GoogleCloudStorageOptions.newBuilder().setAppName(appName).setProjectId(projectId).build();
    httpRequestsInitializer =
        new RetryHttpInitializer(
            credential,
            gcsOptions.getAppName(),
            gcsOptions.getMaxHttpRequestRetries(),
            gcsOptions.getHttpRequestConnectTimeout(),
            gcsOptions.getHttpRequestReadTimeout());

    GoogleCloudStorageFileSystem gcsfs =
        new GoogleCloudStorageFileSystem(
            credential,
            GoogleCloudStorageFileSystemOptions.newBuilder()
                .setEnableBucketDelete(true)
                .setCloudStorageOptionsBuilder(gcsOptions.toBuilder())
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
    GoogleCloudStorageFileSystemOptions gcsFsOptions = newGcsFsOptions();

    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorageFileSystem gcsFs = newGcsFs(gcsFsOptions, gcsRequestsTracker);

    String bucketName = gcsfsIHelper.sharedBucketName1;
    String dirObject = "mkdirs_shouldCreateNewDirectory_" + UUID.randomUUID();
    URI dirObjectUri = new URI("gs://" + bucketName).resolve(dirObject);

    gcsFs.mkdir(dirObjectUri);

    assertThat(
            gcsRequestsTracker.getAllRequests().stream()
                .map(this::requestToString)
                .collect(toImmutableList()))
        .containsExactly(String.format(UPLOAD_REQUEST_FORMAT, bucketName, dirObject + "/"));

    assertThat(gcsFs.exists(dirObjectUri)).isTrue();
    assertThat(gcsFs.getFileInfo(dirObjectUri).isDirectory()).isTrue();
  }

  private String requestToString(HttpRequest request) {
    String method = request.getRequestMethod();
    String url = request.getUrl().toString();
    String requestString = method + ":" + url;
    if ("POST".equals(method) && url.contains("uploadType=multipart")) {
      MultipartContent content = (MultipartContent) request.getContent();
      JsonHttpContent jsonRequest =
          (JsonHttpContent) Iterables.get(content.getParts(), 0).getContent();
      String objectName = ((StorageObject) jsonRequest.getData()).getName();
      requestString += ":" + objectName;
    }
    return requestString;
  }

  private GoogleCloudStorageFileSystemOptions newGcsFsOptions() {
    return GoogleCloudStorageFileSystemOptions.newBuilder()
        .setCloudStorageOptionsBuilder(gcsOptions.toBuilder())
        .build();
  }

  private GoogleCloudStorageFileSystem newGcsFs(
      GoogleCloudStorageFileSystemOptions gcsfsOptions,
      TrackingHttpRequestInitializer gcsRequestsTracker)
      throws IOException {
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);
    return new GoogleCloudStorageFileSystem(gcs, gcsfsOptions);
  }
}
