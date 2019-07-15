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
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.batchRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.composeRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.copyRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.deleteRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getBucketRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.getRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.listRequestString;
import static com.google.cloud.hadoop.gcsio.TrackingHttpRequestInitializer.postRequestString;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<String> listedObjects =
        gcs.listObjectNames(testBucket, testDir, PATH_DELIMITER, /* maxResults= */ 1);

    assertThat(listedObjects).containsExactly(testDir + "f1");
    // Assert that only 1 GCS request was sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(testBucket, testDir, /* maxResults= */ 2, /* pageToken= */ null));
  }

  @Test
  public void listObjectNames_withLimit_multipleGcsRequests() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    int maxResultsPerRequest = 1;
    GoogleCloudStorageOptions options =
        gcsOptions.toBuilder().setMaxListItemsPerCall(maxResultsPerRequest).build();
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(options, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "subdir1/f3", "subdir2/f4");

    List<String> listedObjects = gcs.listObjectNames(testBucket, testDir, PATH_DELIMITER, 3);

    assertThat(listedObjects).containsExactly(testDir + "f1", testDir + "f2", testDir + "subdir1/");
    // Assert that 4 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(testBucket, testDir, maxResultsPerRequest, /* pageToken= */ null),
            listRequestString(testBucket, testDir, maxResultsPerRequest, "token_1"),
            listRequestString(testBucket, testDir, maxResultsPerRequest, "token_2"),
            listRequestString(testBucket, testDir, maxResultsPerRequest, "token_3"));
  }

  @Test
  public void listObjectNames_withoutLimit() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    int maxResultsPerRequest = 1;
    GoogleCloudStorageOptions options =
        gcsOptions.toBuilder().setMaxListItemsPerCall(maxResultsPerRequest).build();
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(options, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "subdir/f3", "subdir/f4");

    List<String> listedObjects = gcs.listObjectNames(testBucket, testDir, PATH_DELIMITER);

    assertThat(listedObjects).containsExactly(testDir + "f1", testDir + "f2", testDir + "subdir/");
    // Assert that 5 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(testBucket, testDir, maxResultsPerRequest, /* pageToken= */ null),
            listRequestString(testBucket, testDir, maxResultsPerRequest, "token_1"),
            listRequestString(testBucket, testDir, maxResultsPerRequest, "token_2"),
            listRequestString(testBucket, testDir, maxResultsPerRequest, "token_3"),
            listRequestString(testBucket, testDir, maxResultsPerRequest, "token_4"));
  }

  @Test
  public void listObjectInfo_withLimit_oneGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, PATH_DELIMITER, /* maxResults= */ 1);

    assertThat(toObjectNames(listedObjects)).containsExactly(testDir + "f1");
    // Assert that only 1 GCS request was sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                testBucket, true, testDir, /* maxResults= */ 2, /* pageToken= */ null));
  }

  @Test
  public void listObjectInfo_withLimit_multipleGcsRequests() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    int maxResultsPerRequest = 1;
    GoogleCloudStorageOptions options =
        gcsOptions.toBuilder().setMaxListItemsPerCall(maxResultsPerRequest).build();
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(options, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3", "f4");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, PATH_DELIMITER, /* maxResults= */ 2);

    assertThat(toObjectNames(listedObjects)).containsExactly(testDir + "f1", testDir + "f2");
    // Assert that 3 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                testBucket, true, testDir, maxResultsPerRequest, /* pageToken= */ null),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_1"),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_2"));
  }

  @Test
  public void listObjectInfo_withoutLimits() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    int maxResultsPerRequest = 1;
    GoogleCloudStorageOptions options =
        gcsOptions.toBuilder().setMaxListItemsPerCall(maxResultsPerRequest).build();
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(options, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<GoogleCloudStorageItemInfo> listedObjects =
        gcs.listObjectInfo(testBucket, testDir, PATH_DELIMITER);

    assertThat(toObjectNames(listedObjects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    // Assert that 4 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            listRequestString(
                testBucket, true, testDir, maxResultsPerRequest, /* pageToken= */ null),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_1"),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_2"),
            listRequestString(testBucket, true, testDir, maxResultsPerRequest, "token_3"));
  }

  @Test
  public void getItemInfo_oneGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1");

    GoogleCloudStorageItemInfo object =
        gcs.getItemInfo(new StorageResourceId(testBucket, testDir + "f1"));

    assertThat(object.getObjectName()).isEqualTo(testDir + "f1");
    // Assert that 1 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(getRequestString(testBucket, testDir + "f1"));
  }

  @Test
  public void getItemInfos_withoutLimits() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<StorageResourceId> resourceIdsList =
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2"),
            new StorageResourceId(testBucket, testDir + "f3"));

    List<GoogleCloudStorageItemInfo> objects = gcs.getItemInfos(resourceIdsList);

    assertThat(toObjectNames(objects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    // Assert that 4 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(testBucket, testDir + "f1"),
            getRequestString(testBucket, testDir + "f2"),
            getRequestString(testBucket, testDir + "f3"));
  }

  @Test
  public void getItemInfos_withLimit_zeroBatchGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxRequestsPerBatch(1).build(), gcsRequestsTracker);
    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<StorageResourceId> resourceIdsList =
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2"),
            new StorageResourceId(testBucket, testDir + "f3"));

    List<GoogleCloudStorageItemInfo> objects = gcs.getItemInfos(resourceIdsList);

    assertThat(toObjectNames(objects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    // Assert that 3 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(testBucket, testDir + "f1"),
            getRequestString(testBucket, testDir + "f2"),
            getRequestString(testBucket, testDir + "f3"));
  }

  @Test
  public void getItemInfos_withLimit_multipleBatchGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxRequestsPerBatch(2).build(), gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    List<StorageResourceId> resourceIdsList =
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2"),
            new StorageResourceId(testBucket, testDir + "f3"));

    List<GoogleCloudStorageItemInfo> objects = gcs.getItemInfos(resourceIdsList);

    assertThat(toObjectNames(objects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    // Assert that 5 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(testBucket, testDir + "f1"),
            getRequestString(testBucket, testDir + "f2"),
            batchRequestString(),
            getRequestString(testBucket, testDir + "f3"));
  }

  @Test
  public void updateItems_withoutLimits() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1");

    StorageResourceId resourceId = new StorageResourceId(testBucket, testDir + "f1");
    Map<String, byte[]> updatedMetadata = ImmutableMap.of("test-metadata", "test-value".getBytes());

    List<GoogleCloudStorageItemInfo> updatedObjects =
        gcs.updateItems(ImmutableList.of(new UpdatableItemInfo(resourceId, updatedMetadata)));

    assertThat(toObjectNames(updatedObjects)).containsExactly(testDir + "f1");
    assertThat(updatedObjects.get(0).getMetadata().keySet()).isEqualTo(updatedMetadata.keySet());

    // Assert that 2 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(postRequestString(testBucket, testDir + "f1"));
  }

  @Test
  public void updateItems_withLimits_MultipleBatchGcsRequests() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxRequestsPerBatch(2).build(), gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    Map<String, byte[]> updatedMetadata = ImmutableMap.of("test-metadata", "test-value".getBytes());

    List<GoogleCloudStorageItemInfo> updatedObjects =
        gcs.updateItems(
            ImmutableList.of(
                new UpdatableItemInfo(
                    new StorageResourceId(testBucket, testDir + "f1"), updatedMetadata),
                new UpdatableItemInfo(
                    new StorageResourceId(testBucket, testDir + "f2"), updatedMetadata),
                new UpdatableItemInfo(
                    new StorageResourceId(testBucket, testDir + "f3"), updatedMetadata)));

    assertThat(toObjectNames(updatedObjects))
        .containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
    assertThat(updatedObjects.get(0).getMetadata().keySet()).isEqualTo(updatedMetadata.keySet());

    // Assert that 5 GCS requests were sent
    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            postRequestString(testBucket, testDir + "f1"),
            postRequestString(testBucket, testDir + "f2"),
            batchRequestString(),
            postRequestString(testBucket, testDir + "f3"));
  }

  @Test
  public void copy_withoutLimits_withDisabledCopyWithRewrites() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket1 = gcsfsIHelper.sharedBucketName1;
    String testBucket2 = gcsfsIHelper.sharedBucketName2;
    String testDir = createObjectsInTestDir(testBucket1, "f1", "f2", "f3");

    gcs.copy(
        testBucket1,
        ImmutableList.of(testDir + "f1", testDir + "f2"),
        testBucket2,
        ImmutableList.of(testDir + "f4", testDir + "f5"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(testBucket1),
            getBucketRequestString(testBucket2),
            batchRequestString(),
            copyRequestString(testBucket1, testDir + "f1", testBucket2, testDir + "f4", "copyTo"),
            copyRequestString(testBucket1, testDir + "f2", testBucket2, testDir + "f5", "copyTo"));

    List<String> listedObjects = gcs.listObjectNames(testBucket2, testDir, PATH_DELIMITER);
    assertThat(listedObjects).containsExactly(testDir + "f4", testDir + "f5");
  }

  @Test
  public void copy_withoutLimits_withEnabledCopyWithRewrites() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setCopyWithRewriteEnabled(true).build(), gcsRequestsTracker);

    String testBucket1 = gcsfsIHelper.sharedBucketName1;
    String testBucket2 = gcsfsIHelper.sharedBucketName2;
    String testDir = createObjectsInTestDir(testBucket1, "f1", "f2", "f3");

    gcs.copy(
        testBucket1,
        ImmutableList.of(testDir + "f1", testDir + "f2"),
        testBucket2,
        ImmutableList.of(testDir + "f4", testDir + "f5"));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getBucketRequestString(testBucket1),
            getBucketRequestString(testBucket2),
            batchRequestString(),
            copyRequestString(
                testBucket1, testDir + "f1", testBucket2, testDir + "f4", "rewriteTo"),
            copyRequestString(
                testBucket1, testDir + "f2", testBucket2, testDir + "f5", "rewriteTo"));

    List<String> listedObjects = gcs.listObjectNames(testBucket2, testDir, PATH_DELIMITER);
    assertThat(listedObjects).containsExactly(testDir + "f4", testDir + "f5");
  }

  @Test
  public void deleteObjects_withoutLimit() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    gcs.deleteObjects(
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2")));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            batchRequestString(),
            getRequestString(testBucket, testDir + "f1"),
            getRequestString(testBucket, testDir + "f2"),
            batchRequestString(),
            deleteRequestString(testBucket, testDir + "f1", /* generationId= */ 4),
            deleteRequestString(testBucket, testDir + "f2", /* generationId= */ 5));

    List<String> listedObjects = gcs.listObjectNames(testBucket, testDir, PATH_DELIMITER);
    assertThat(listedObjects).containsExactly(testDir + "f3");
  }

  @Test
  public void deleteObjects_withLimit_zeroBatchGcsRequest() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs =
        new GoogleCloudStorageImpl(
            gcsOptions.toBuilder().setMaxRequestsPerBatch(1).build(), gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2", "f3");

    gcs.deleteObjects(
        ImmutableList.of(
            new StorageResourceId(testBucket, testDir + "f1"),
            new StorageResourceId(testBucket, testDir + "f2")));

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(testBucket, testDir + "f1"),
            deleteRequestString(testBucket, testDir + "f1", /* generationId= */ 1),
            getRequestString(testBucket, testDir + "f2"),
            deleteRequestString(testBucket, testDir + "f2", /* generationId= */ 3));

    List<String> listedObjects = gcs.listObjectNames(testBucket, testDir, PATH_DELIMITER);
    assertThat(listedObjects).containsExactly(testDir + "f3");
  }

  @Test
  public void composeObject_withoutLimit() throws Exception {
    TrackingHttpRequestInitializer gcsRequestsTracker =
        new TrackingHttpRequestInitializer(httpRequestsInitializer);
    GoogleCloudStorage gcs = new GoogleCloudStorageImpl(gcsOptions, gcsRequestsTracker);

    String testBucket = gcsfsIHelper.sharedBucketName1;
    String testDir = createObjectsInTestDir(testBucket, "f1", "f2");

    gcs.compose(testBucket, ImmutableList.of(testDir + "f1", testDir + "f2"), testDir + "f3", null);

    assertThat(gcsRequestsTracker.getAllRequestStrings())
        .containsExactly(
            getRequestString(testBucket, testDir + "f3"),
            composeRequestString(testBucket, testDir + "f3", /* generationId= */ 1));

    List<String> listedObjects = gcs.listObjectNames(testBucket, testDir, PATH_DELIMITER);
    assertThat(listedObjects).containsExactly(testDir + "f1", testDir + "f2", testDir + "f3");
  }

  private static List<String> toObjectNames(List<GoogleCloudStorageItemInfo> listedObjects) {
    return listedObjects.stream().map(GoogleCloudStorageItemInfo::getObjectName).collect(toList());
  }

  private String createObjectsInTestDir(String bucketName, String... objects) throws Exception {
    String testDir = name.getMethodName() + "_" + UUID.randomUUID() + "/";
    String[] objectPaths = Arrays.stream(objects).map(o -> testDir + o).toArray(String[]::new);
    gcsfsIHelper.createObjectsWithSubdirs(bucketName, objectPaths);
    return testDir;
  }
}
