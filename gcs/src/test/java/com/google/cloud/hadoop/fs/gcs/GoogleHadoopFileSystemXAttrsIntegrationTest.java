package com.google.cloud.hadoop.fs.gcs;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationTest;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.UpdatableItemInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class GoogleHadoopFileSystemXAttrsIntegrationTest {

  private static HadoopFileSystemIntegrationHelper ghfsHelper;
  private static FileSystem ghfs;

  @BeforeClass
  public static void setup() throws Throwable {
    GoogleHadoopFileSystemIntegrationTest.storageResource.before();
    ghfsHelper = GoogleHadoopFileSystemIntegrationTest.ghfsHelper;
    ghfs = GoogleHadoopFileSystemIntegrationTest.ghfs;
  }

  @AfterClass
  public static void cleanup() {
    ghfs = null;
    ghfsHelper = null;
    GoogleHadoopFileSystemIntegrationTest.storageResource.after();
  }

  @Test
  public void getXAttr_nonExistentAttr() throws Exception {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "obj-test-get-xattr", 1, /* overwrite= */ false);

    ghfs.setXAttr(filePath, "test-xattr-some", "test-xattr-value".getBytes(UTF_8));

    assertThat(ghfs.getXAttr(filePath, "test-xattr-non-existent")).isNull();
    assertThat(ghfs.getXAttrs(filePath, ImmutableList.of("test-xattr-non-existent"))).isEmpty();

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  @Test
  public void getXAttr_nonGhfsMetadata() throws Exception {
    GoogleCloudStorageFileSystem gcsFs = ((GoogleHadoopFileSystem) ghfs).getGcsFs();
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);

    ghfsHelper.writeFile(filePath, "obj-test-get-xattr-extra", 1, /* overwrite= */ false);

    UpdatableItemInfo updateInfo =
        new UpdatableItemInfo(
            StorageResourceId.fromObjectName(filePath.toString()),
            ImmutableMap.of("non-ghfs-xattr-key", "non-ghfs-xattr-value".getBytes(UTF_8)));
    gcsFs.getGcs().updateItems(ImmutableList.of(updateInfo));

    ghfs.setXAttr(filePath, "test-xattr-some", "test-xattr-value".getBytes(UTF_8));

    assertThat(toStringValuesMap(gcsFs.getFileInfo(filePath.toUri()).getAttributes()))
        .containsExactly(
            "non-ghfs-xattr-key", "non-ghfs-xattr-value",
            "GHFS_XATTR_test-xattr-some", "test-xattr-value");
    assertThat(toStringValuesMap(ghfs.getXAttrs(filePath)))
        .containsExactly("test-xattr-some", "test-xattr-value");

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  @Test
  public void setXAttr() throws Exception {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "obj-test-set-xattr", 1, /* overwrite= */ false);

    ghfs.setXAttr(filePath, "test-xattr-some", "test-xattr-value".getBytes(UTF_8));
    ghfs.setXAttr(filePath, "test-xattr-null", null);
    ghfs.setXAttr(filePath, "test-xattr-empty", new byte[0]);

    assertThat(ghfs.listXAttrs(filePath))
        .containsExactly("test-xattr-some", "test-xattr-null", "test-xattr-empty");
    assertThat(ghfs.getXAttr(filePath, "test-xattr-some"))
        .isEqualTo("test-xattr-value".getBytes(UTF_8));
    assertThat(ghfs.getXAttr(filePath, "test-xattr-null")).isEmpty();
    assertThat(ghfs.getXAttr(filePath, "test-xattr-empty")).isEmpty();
    assertThat(toStringValuesMap(ghfs.getXAttrs(filePath)))
        .containsExactly(
            "test-xattr-some", "test-xattr-value",
            "test-xattr-null", "",
            "test-xattr-empty", "");
    assertThat(
            toStringValuesMap(
                ghfs.getXAttrs(
                    filePath,
                    ImmutableList.of("test-xattr-empty", "test-xattr-some", "test-xattr-null"))))
        .containsExactly(
            "test-xattr-some", "test-xattr-value",
            "test-xattr-null", "",
            "test-xattr-empty", "");

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  @Test
  public void setXAttr_replace() throws Exception {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "obj-test-set-xattr-replace", 1, /* overwrite= */ false);

    ghfs.setXAttr(filePath, "test-xattr-some", "test-xattr-value".getBytes(UTF_8));

    assertThat(ghfs.getXAttr(filePath, "test-xattr-some"))
        .isEqualTo("test-xattr-value".getBytes(UTF_8));

    ghfs.setXAttr(filePath, "test-xattr-some", "test-xattr-value-new".getBytes(UTF_8));

    assertThat(ghfs.getXAttr(filePath, "test-xattr-some"))
        .isEqualTo("test-xattr-value-new".getBytes(UTF_8));

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  @Test
  public void setXAttr_create_fail() throws Exception {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "obj-test-set-xattr-create-fail", 1, /* overwrite= */ false);

    IOException e =
        assertThrows(
            IOException.class,
            () ->
                ghfs.setXAttr(
                    filePath, "test-key", "val".getBytes(UTF_8), EnumSet.of(XAttrSetFlag.REPLACE)));

    assertThat(e).hasMessageThat().startsWith("CREATE flag must be set to create XAttr");

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  @Test
  public void setXAttr_replace_fail() throws Exception {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "obj-test-set-xattr-replace-fail", 1, /* overwrite= */ false);

    ghfs.setXAttr(filePath, "test-key", "value".getBytes(UTF_8));

    IOException e =
        assertThrows(
            IOException.class,
            () ->
                ghfs.setXAttr(
                    filePath, "test-key", "new".getBytes(UTF_8), EnumSet.of(XAttrSetFlag.CREATE)));
    assertThat(e).hasMessageThat().startsWith("REPLACE flag must be set to update XAttr");

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  @Test
  public void removeXAttr() throws Exception {
    URI fileUri = GoogleCloudStorageFileSystemIntegrationTest.getTempFilePath();
    Path filePath = ghfsHelper.castAsHadoopPath(fileUri);
    ghfsHelper.writeFile(filePath, "obj-test-remove-xattr", 1, /* overwrite= */ false);

    ghfs.setXAttr(filePath, "test-xattr-some", "test-xattr-value-1".getBytes(UTF_8));
    ghfs.setXAttr(filePath, "test-xattr-to-remove", "test-xattr-value-2".getBytes(UTF_8));

    assertThat(toStringValuesMap(ghfs.getXAttrs(filePath)))
        .containsExactly(
            "test-xattr-some", "test-xattr-value-1",
            "test-xattr-to-remove", "test-xattr-value-2");

    ghfs.removeXAttr(filePath, "test-xattr-to-remove");

    assertThat(ghfs.getXAttr(filePath, "test-xattr-to-remove")).isNull();
    assertThat(toStringValuesMap(ghfs.getXAttrs(filePath)))
        .containsExactly("test-xattr-some", "test-xattr-value-1");

    // Cleanup.
    assertThat(ghfs.delete(filePath, true)).isTrue();
  }

  private static Map<String, String> toStringValuesMap(Map<String, byte[]> map) {
    return map.entrySet().stream()
        .map(
            e ->
                new AbstractMap.SimpleEntry<>(
                    e.getKey(), e.getValue() == null ? null : new String(e.getValue(), UTF_8)))
        .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);
  }
}
