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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorage.PATH_DELIMITER;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestString;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for GoogleCloudStorageFileSystem class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageNewIntegrationTest {

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
    GoogleCloudStorage gcs = gcsfsIHelper.gcs;
    String bucketPath1 = "gs://" + gcsfsIHelper.sharedBucketName1;
    String bucketPath2 = "gs://" + gcsfsIHelper.sharedBucketName2;
    assertThat(gcs.getItemInfo(StorageResourceId.fromObjectName(bucketPath1)).exists()).isFalse();
    assertThat(gcs.getItemInfo(StorageResourceId.fromObjectName(bucketPath2)).exists()).isFalse();
  }

  @Test
  public void listObjectNames_withLimit_oneGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    // Create 3 objects:
    // - gs://<tesBucket>/<testDir>/f1
    // - gs://<tesBucket>/<testDir>/f2
    // - gs://<tesBucket>/<testDir>/f3
    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<String> listedObjects =
        gcs.listObjectNames(testBucket, testDir, PATH_DELIMITER, /* maxResults= */ 1);

    assertThat(listedObjects).containsExactly(testDir + "f1");

    int maxResults = 2;
    // Assert that only 1 GCS request was sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(listRequestString(testBucket, testDir, maxResults, /* pageToken= */ null));
  }

  @Test
  public void listObjectNames_withLimit_multipleGcsRequests() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    int maxResults = 1;
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxListItemsPerCall(maxResults).build(), gcsRequestsTracker);

    // Create 4 objects:
    // - gs://<tesBucket>/<testDir>/f1
    // - gs://<tesBucket>/<testDir>/f2
    // - gs://<tesBucket>/<testDir>/subDir/f3
    // - gs://<tesBucket>/<testDir>/subDir/f4
    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "subdir/f3", "subdir/f4");

    List<String> listedObjects = gcs.listObjectNames(testBucket, testDir, PATH_DELIMITER, 3);

    assertThat(listedObjects).containsExactly(testDir + "f1", testDir + "f2", testDir + "subdir/");

    // Assert that 4 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(testBucket, testDir, maxResults, /* pageToken= */ null),
            listRequestString(testBucket, testDir, maxResults, "token_1"),
            listRequestString(testBucket, testDir, maxResults, "token_2"),
            listRequestString(testBucket, testDir, maxResults, "token_3"));
  }

  @Test
  public void listObjectNames_withoutLimit() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    int maxResults = 1;
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxListItemsPerCall(maxResults).build(), gcsRequestsTracker);

    // Create 4 objects:
    // - gs://<tesBucket>/<testDir>/f1
    // - gs://<tesBucket>/<testDir>/f2
    // - gs://<tesBucket>/<testDir>/subDir/f3
    // - gs://<tesBucket>/<testDir>/subDir/f4
    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "subdir/f3", "subdir/f4");

    List<String> listedObjects = gcs.listObjectNames(testBucket, testDir, PATH_DELIMITER);

    assertThat(listedObjects).containsExactly(testDir + "f1", testDir + "f2", testDir + "subdir/");

    // Assert that 5 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(testBucket, testDir, maxResults, /* pageToken= */ null),
            listRequestString(testBucket, testDir, maxResults, "token_1"),
            listRequestString(testBucket, testDir, maxResults, "token_2"),
            listRequestString(testBucket, testDir, maxResults, "token_3"),
            listRequestString(testBucket, testDir, maxResults, "token_4"));
  }

  private String createObjectsInTestDir(String bucketName, String... objects) throws Exception {
    String testDir = name.getMethodName() + "_" + UUID.randomUUID() + "/";
    String[] objectPaths = Arrays.stream(objects).map(o -> testDir + o).toArray(String[]::new);
    gcsfsIHelper.createObjectsWithSubdirs(bucketName, objectPaths);
    return testDir;
  }
}
