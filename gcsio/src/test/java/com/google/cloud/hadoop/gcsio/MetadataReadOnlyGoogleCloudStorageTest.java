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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * UnitTests for MetadataReadOnlyGoogleCloudStorage class validating list/get methods against
 * a fixed in-memory list of items.
 */
@RunWith(JUnit4.class)
public class MetadataReadOnlyGoogleCloudStorageTest {
  private static final String BUCKET_NAME = "foo-bucket";

  // Test setup, initialized on demand depending on each test case.
  private List<GoogleCloudStorageItemInfo> initialInfos;
  private Map<StorageResourceId, GoogleCloudStorageItemInfo> initialMap;
  private MetadataReadOnlyGoogleCloudStorage gcs;
  private MetadataReadOnlyGoogleCloudStorage emptyGcs;

  /**
   * Helper to create a StorageResourceId without the verbosity of re-specifying a bucket each time
   * if we're willing to let all objects be in the same bucket.
   */
  static StorageResourceId createId(String objectName) {
    return new StorageResourceId(BUCKET_NAME, objectName);
  }

  /**
   * Helper to construct a general GoogleCloudStorageItemInfo.
   */
  static GoogleCloudStorageItemInfo createInfo(
      String objectName, long creationTime, long size) {
    return new GoogleCloudStorageItemInfo(createId(objectName), creationTime, size, null, null);
  }

  /**
   * Helper to construct a GoogleCloudStorageItemInfo which looks like a directory.
   */
  static GoogleCloudStorageItemInfo createDir(
      String objectName, long creationTime) {
    objectName = FileInfo.convertToDirectoryPath(objectName);
    // Directories have size == 0.
    return createInfo(objectName, creationTime, 0);
  }

  /**
   * Helper to generate a more easily accessible Map from StorageResourceIds to
   * GoogleCloudStorageItemInfos.
   */
  static Map<StorageResourceId, GoogleCloudStorageItemInfo> createMap(
      List<GoogleCloudStorageItemInfo> itemInfos) {
    // NB: We can't use TreeMap unless we implement Comparable in StorageResourceId.
    Map<StorageResourceId, GoogleCloudStorageItemInfo> lookupMap = new HashMap<>();
    for (GoogleCloudStorageItemInfo info : itemInfos) {
      lookupMap.put(info.getResourceId(), info);
    }
    return lookupMap;
  }

  @Before
  public void setUp() throws IOException {
    emptyGcs = new MetadataReadOnlyGoogleCloudStorage(new ArrayList<GoogleCloudStorageItemInfo>());
  }

  @Test
  public void testCreateIsUnsupported() throws IOException {
    StorageResourceId resourceId = new StorageResourceId("foo", "bar");
    assertThrows(UnsupportedOperationException.class, () -> emptyGcs.create(resourceId));
  }

  @Test
  public void testCreateEmptyObjectIsUnsupported() throws IOException {
    StorageResourceId resourceId = new StorageResourceId("foo", "bar");
    assertThrows(UnsupportedOperationException.class, () -> emptyGcs.createEmptyObject(resourceId));
  }

  @Test
  public void testCreateEmptyObjectsIsUnsupported() throws IOException {
    StorageResourceId resourceId = new StorageResourceId("foo", "bar");
    assertThrows(
        UnsupportedOperationException.class,
        () -> emptyGcs.createEmptyObjects(ImmutableList.of(resourceId)));
  }

  @Test
  public void testOpenIsUnsupported() throws IOException {
    StorageResourceId resourceId = new StorageResourceId("foo", "bar");
    assertThrows(UnsupportedOperationException.class, () -> emptyGcs.open(resourceId));
  }

  @Test
  public void testCreateBucketIsUnsupported() throws IOException {
    assertThrows(UnsupportedOperationException.class, () -> emptyGcs.create("bucketName"));
  }

  @Test
  public void testCreateBucketWithOptionsIsUnsupported() throws IOException {
    assertThrows(
        UnsupportedOperationException.class,
        () -> emptyGcs.create("bucketName", CreateBucketOptions.DEFAULT));
  }

  @Test
  public void testDeleteBucketsIsUnsupported() throws IOException {
    assertThrows(
        UnsupportedOperationException.class,
        () -> emptyGcs.deleteBuckets(ImmutableList.of("bucketName")));
  }

  @Test
  public void testDeleteObjectsIsUnsupported() throws IOException {
    StorageResourceId resourceId = new StorageResourceId("foo", "bar");
    assertThrows(
        UnsupportedOperationException.class,
        () -> emptyGcs.deleteObjects(ImmutableList.of(resourceId)));
  }

  @Test
  public void testCopyIsUnsupported() throws IOException {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            emptyGcs.copy(
                "bucket", ImmutableList.of("objSrc"), "bucket", ImmutableList.of("objDst")));
  }

  @Test
  public void testListBucketNamesIsUnsupported() throws IOException {
    assertThrows(UnsupportedOperationException.class, () -> emptyGcs.listBucketNames());
  }

  @Test
  public void testListBucketInfoIsUnsupported() throws IOException {
    assertThrows(UnsupportedOperationException.class, () -> emptyGcs.listBucketInfo());
  }

  @Test
  public void testWaitForBucketEmptyIsUnsupported() throws IOException {
    assertThrows(UnsupportedOperationException.class, () -> emptyGcs.waitForBucketEmpty("bucket"));
  }

  @Test
  public void testComposeIsUnsupported() throws IOException {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            emptyGcs.compose(
                "bucket",
                ImmutableList.of("object1", "object2"),
                "destination",
                CreateFileOptions.DEFAULT_CONTENT_TYPE));
  }

  @Test
  public void testCallingGcsCloseIsAllowed() throws IOException {
    GoogleCloudStorage gcsToClose = new MetadataReadOnlyGoogleCloudStorage(
        new ArrayList<GoogleCloudStorageItemInfo>());

    gcsToClose.close();
  }

  /**
   * Helper to set up our test objects with a basic list with no phantom directories.
   */
  protected void setupWithBasicInfoList() throws IOException {
    initialInfos = ImmutableList.of(
        createDir("foo/", 111),
        createDir("foo/bar/", 222),
        createInfo("foo/bar/data1.txt", 333, 1024),  // Directory containing a file.
        createDir("foo/baz/", 444),                  // Empty directory.
        createInfo("foo/data2.txt", 555, 1024));     // Plain file next to the other directories.
    initialMap = createMap(initialInfos);
    gcs = new MetadataReadOnlyGoogleCloudStorage(initialInfos);
  }

  @Test
  public void testBasicGetItemInfos()
      throws IOException {
    setupWithBasicInfoList();

    // Fetch each item individually.
    for (Map.Entry<StorageResourceId, GoogleCloudStorageItemInfo> entry : initialMap.entrySet()) {
      assertThat(gcs.getItemInfo(entry.getKey())).isEqualTo(entry.getValue());
    }

    // Fetch them all at once.
    List<GoogleCloudStorageItemInfo> fetchedInfos =
        gcs.getItemInfos(new ArrayList<StorageResourceId>(initialMap.keySet()));
    assertThat(fetchedInfos).hasSize(initialMap.size());
    for (GoogleCloudStorageItemInfo fetchedInfo : fetchedInfos) {
      assertThat(initialMap).containsEntry(fetchedInfo.getResourceId(), fetchedInfo);
    }
  }

  @Test
  public void testListObjectInfoNullPrefixNullDelimiter()
      throws IOException {
    setupWithBasicInfoList();

    // Should list everything.
    List<GoogleCloudStorageItemInfo> listedInfos = gcs.listObjectInfo(BUCKET_NAME, null, null);
    assertThat(listedInfos).hasSize(initialMap.size());
    for (GoogleCloudStorageItemInfo listedInfo : listedInfos) {
      assertThat(initialMap).containsEntry(listedInfo.getResourceId(), listedInfo);
    }

    List<String> listedNames = gcs.listObjectNames(BUCKET_NAME, null, null);
    assertThat(listedNames).hasSize(initialMap.size());
    for (String listedName : listedNames) {
      assertThat(initialMap).containsKey(createId(listedName));
    }

    // Empty prefix same as null prefix.
    listedInfos = gcs.listObjectInfo(BUCKET_NAME, "", null);
    assertThat(listedInfos).hasSize(initialMap.size());
    for (GoogleCloudStorageItemInfo listedInfo : listedInfos) {
      assertThat(initialMap).containsEntry(listedInfo.getResourceId(), listedInfo);
    }

    listedNames = gcs.listObjectNames(BUCKET_NAME, "", null);
    assertThat(listedNames).hasSize(initialMap.size());
    for (String listedName : listedNames) {
      assertThat(initialMap).containsKey(createId(listedName));
    }
  }

  @Test
  public void testListObjectInfoNullPrefixWithDelimiter()
      throws IOException {
    setupWithBasicInfoList();

    // Should only list "foo/".
    List<GoogleCloudStorageItemInfo> listedInfos = gcs.listObjectInfo(BUCKET_NAME, null, "/");
    assertThat(listedInfos).hasSize(1);
    assertThat(initialMap).containsEntry(createId("foo/"), listedInfos.get(0));
  }

  @Test
  public void testListObjectInfoEntireRootPrefixWithoutTrailingDelimiter()
      throws IOException {
    setupWithBasicInfoList();

    // Should only list "foo/".
    List<GoogleCloudStorageItemInfo> listedInfos = gcs.listObjectInfo(BUCKET_NAME, "foo", "/");
    assertThat(listedInfos).hasSize(1);
    assertThat(initialMap).containsEntry(createId("foo/"), listedInfos.get(0));
  }

  @Test
  public void testListObjectInfoTopLevelPrefixNullDelimiter()
      throws IOException {
    setupWithBasicInfoList();

    // Should list everything except "foo/".
    List<GoogleCloudStorageItemInfo> listedInfos = gcs.listObjectInfo(BUCKET_NAME, "foo/", null);

    initialMap.remove(createId("foo/"));
    assertThat(listedInfos).hasSize(initialMap.size());
    for (GoogleCloudStorageItemInfo listedInfo : listedInfos) {
      assertThat(initialMap).containsEntry(listedInfo.getResourceId(), listedInfo);
    }
  }

  @Test
  public void testListObjectInfoPartialPrefixNullDelimiter()
      throws IOException {
    setupWithBasicInfoList();

    // Everything except "foo/" and "foo/data2.txt".
    List<GoogleCloudStorageItemInfo> listedInfos = gcs.listObjectInfo(BUCKET_NAME, "foo/ba", null);

    initialMap.remove(createId("foo/"));
    initialMap.remove(createId("foo/data2.txt"));
    assertThat(listedInfos).hasSize(initialMap.size());
    for (GoogleCloudStorageItemInfo listedInfo : listedInfos) {
      assertThat(initialMap).containsEntry(listedInfo.getResourceId(), listedInfo);
    }
  }

  @Test
  public void testListObjectInfoPartialPrefixWithDelimiter()
      throws IOException {
    setupWithBasicInfoList();

    // Only lists foo/bar/ and foo/baz/.
    List<GoogleCloudStorageItemInfo> listedInfos = gcs.listObjectInfo(BUCKET_NAME, "foo/ba", "/");

    assertThat(listedInfos).hasSize(2);
    Map<StorageResourceId, GoogleCloudStorageItemInfo> listedMap = createMap(listedInfos);
    assertThat(listedMap).containsEntry(createId("foo/bar/"), initialMap.get(createId("foo/bar/")));
    assertThat(listedMap).containsEntry(createId("foo/baz/"), initialMap.get(createId("foo/baz/")));
  }

  @Test
  public void testListImplicitDirectories()
      throws IOException {
    initialInfos = ImmutableList.of(
        createDir("foo/bar/baz/", 111),
        createInfo("foo/bar2/data1.txt", 222, 1024));
    initialMap = createMap(initialInfos);
    gcs = new MetadataReadOnlyGoogleCloudStorage(initialInfos);

    // Parent directories don't exist; fetching their infos doesn't crash but does return a
    // !exists() info.
    assertThat(gcs.getItemInfo(createId("foo/")).exists()).isFalse();
    assertThat(gcs.getItemInfo(createId("foo/bar/")).exists()).isFalse();
    assertThat(gcs.getItemInfo(createId("foo/bar2/")).exists()).isFalse();

    assertThat(gcs.getItemInfo(createId("foo/bar/baz/")).exists()).isTrue();
    assertThat(gcs.getItemInfo(createId("foo/bar2/data1.txt")).exists()).isTrue();

    // Listing without delimiter doesn't "repair" the implicit directories.
    List<GoogleCloudStorageItemInfo> listedInfos = gcs.listObjectInfo(BUCKET_NAME, "foo/ba", null);
    assertThat(listedInfos).hasSize(2);
    for (GoogleCloudStorageItemInfo listedInfo : listedInfos) {
      assertThat(initialMap).containsEntry(listedInfo.getResourceId(), listedInfo);
    }

    assertThat(gcs.getItemInfo(createId("foo/")).exists()).isFalse();
    assertThat(gcs.getItemInfo(createId("foo/bar/")).exists()).isFalse();
    assertThat(gcs.getItemInfo(createId("foo/bar2/")).exists()).isFalse();

    // Listing with a delimiter repairs precisely the directories at the listing level.
    listedInfos = gcs.listObjectInfo(BUCKET_NAME, "foo/ba", "/");
    assertThat(listedInfos).hasSize(2);

    assertThat(gcs.getItemInfo(createId("foo/")).exists()).isFalse();
    assertThat(gcs.getItemInfo(createId("foo/bar/")).exists()).isTrue();
    assertThat(gcs.getItemInfo(createId("foo/bar2/")).exists()).isTrue();

    // The "repaired" items are directory objects with a creationTime == 0.
    Map<StorageResourceId, GoogleCloudStorageItemInfo> listedMap = createMap(listedInfos);
    assertThat(listedMap).containsEntry(createId("foo/bar/"), createDir("foo/bar/", 0));
    assertThat(listedMap).containsEntry(createId("foo/bar2/"), createDir("foo/bar2/", 0));

    // Listing again without a delimiter now finds the "repaired" directory objects as well.
    listedInfos = gcs.listObjectInfo(BUCKET_NAME, "foo/ba", null);
    assertThat(listedInfos).hasSize(4);

    listedMap = createMap(listedInfos);
    assertThat(listedMap).containsEntry(createId("foo/bar/"), createDir("foo/bar/", 0));
    assertThat(listedMap).containsEntry(createId("foo/bar2/"), createDir("foo/bar2/", 0));
    assertThat(listedMap)
        .containsEntry(createId("foo/bar/baz/"), initialMap.get(createId("foo/bar/baz/")));
    assertThat(listedMap)
        .containsEntry(
            createId("foo/bar2/data1.txt"), initialMap.get(createId("foo/bar2/data1.txt")));
  }
}
