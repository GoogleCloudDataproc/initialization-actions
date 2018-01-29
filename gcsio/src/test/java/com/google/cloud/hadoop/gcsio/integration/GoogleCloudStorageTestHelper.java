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

package com.google.cloud.hadoop.gcsio.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpTransport;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions.Builder;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for GCS integration tests.
 */
public class GoogleCloudStorageTestHelper {
  // Application name for OAuth.
  public static final String APP_NAME = "GHFS/test";

  protected static final Logger LOG =
      LoggerFactory.getLogger(GoogleCloudStorageTestHelper.class);

  public static Credential getCredential() throws IOException {
    String serviceAccount = TestConfiguration.getInstance().getServiceAccount();
    String privateKeyfile = TestConfiguration.getInstance().getPrivateKeyFile();

    Assert.assertNotNull("privateKeyfile must not be null", privateKeyfile);
    Assert.assertNotNull("serviceAccount must not be null", serviceAccount);
    Credential credential;
    try {
      CredentialFactory credentialFactory = new CredentialFactory();
      HttpTransport transport = HttpTransportFactory.newTrustedTransport();
      credential =
          credentialFactory.getCredentialFromPrivateKeyServiceAccount(
              serviceAccount, privateKeyfile, CredentialFactory.GCS_SCOPES, transport);
    } catch (GeneralSecurityException gse) {
      throw new IOException(gse);
    }
    return credential;
  }

  public static Builder getStandardOptionBuilder() {
    String projectId = TestConfiguration.getInstance().getProjectId();
    Assert.assertNotNull(projectId);
    GoogleCloudStorageOptions.Builder builder = GoogleCloudStorageOptions.newBuilder();
    builder.setAppName(GoogleCloudStorageTestHelper.APP_NAME)
        .setProjectId(projectId)
        .setMaxListItemsPerCall(50)
        .setMaxRequestsPerBatch(2);
    return builder;
  }

  public static void cleanupTestObjects(
      GoogleCloudStorage gcs,
      List<String> bucketNames,
      List<StorageResourceId> resources) throws IOException {
    List<StorageResourceId> objectsToDelete = new ArrayList<>();
    List<String> bucketsToDelete = new ArrayList<>(bucketNames);
    for (StorageResourceId resource : resources) {
      if (resource.isBucket()) {
        bucketsToDelete.add(resource.getBucketName());
      } else {
        objectsToDelete.add(resource);
      }
    }
    gcs.deleteObjects(objectsToDelete);
    try {
      gcs.deleteBuckets(bucketsToDelete);
    } catch (IOException ioe) {
      // We'll cleanup again in @After
    }
  }

  /**
   * More efficient version of checking byte arrays than using Assert.assertArrayEquals.
   */
  public static void assertByteArrayEquals(byte[] expected, byte[] actual) {
    if ((expected == null) ^ (actual == null)) {
      // Don't use a fancy toString(), just display which is null, which isn't, since the arrays
      // may be very large.
      fail(String.format("Expected was '%s', actual was '%s'", expected, actual));
    } else if (expected == null && actual == null) {
      return;
    }

    if (expected.length != actual.length) {
      fail(String.format(
          "Length mismatch: expected: %d, actual: %d", expected.length, actual.length));
    }

    for (int i = 0; i < expected.length; ++i) {
      if (expected[i] != actual[i]) {
        fail(String.format(
            "Mismatch at index %d. expected: 0x%02x, actual: 0x%02x", i, expected[i], actual[i]));
      }
    }
  }

  public static void assertObjectContent(GoogleCloudStorage storage, StorageResourceId object,
      byte[] expectedBytes) throws IOException {

    try (ReadableByteChannel channel = storage.open(object)) {
      ByteBuffer buffer = ByteBuffer.allocate(expectedBytes.length);
      int totalRead = 0;
      int read = channel.read(buffer);
      while (read > 0) {
        totalRead += read;
        read = channel.read(buffer);
      }
      assertEquals("Bytes read mismatch", expectedBytes.length, totalRead);
      buffer.flip();
      byte[] bytesRead = new byte[buffer.limit()];
      buffer.get(bytesRead);
      assertByteArrayEquals(expectedBytes, bytesRead);
    }
  }

  public static void fillBytes(byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) i;
    }
  }

  public static void readAndWriteLargeObject(StorageResourceId objectToCreate,
      GoogleCloudStorage storage) throws IOException {
    // We'll write the bytePattern repetitions times (totalling 65MB of data)
    int repetitions = 65 * 1024;
    int patternSize = 1024;
    byte[] bytePattern = new byte[patternSize];
    fillBytes(bytePattern);

    long startTime = System.currentTimeMillis();
    try (WritableByteChannel channel = storage.create(objectToCreate)) {
      for (int i = 0; i < repetitions; i++) {
        channel.write(ByteBuffer.wrap(bytePattern));
      }
    }
    long endTime = System.currentTimeMillis();
    LOG.info("Took {} milliseconds to write {}", (endTime - startTime), repetitions * patternSize);

    startTime = System.currentTimeMillis();
    try (ReadableByteChannel channel = storage.open(objectToCreate)) {
      byte[] destinationBytes = new byte[patternSize];
      ByteBuffer buffer = ByteBuffer.wrap(destinationBytes);
      for (int i = 0; i < repetitions; i++) {
        int read = channel.read(buffer);
        // Possible flake?
        assertEquals(patternSize, read);
        // Writing to the buffer will write tot he destination bytes array.
        assertByteArrayEquals(bytePattern, destinationBytes);
        // Set the buffer back to the beginning
        buffer.flip();
      }
    }
    endTime = System.currentTimeMillis();
    LOG.info("Took {} milliseconds to read {}", (endTime - startTime), repetitions * patternSize);
  }

  /**
   * Helper for dealing with buckets in GCS integration tests.
   */
  public static class TestBucketHelper {
    protected static final Logger LOG = LoggerFactory.getLogger(TestBucketHelper.class);
    protected final String testBucketPrefix;

    public TestBucketHelper(String bucketPrefix) {
      this.testBucketPrefix = makeBucketName(bucketPrefix);
    }

    public static String makeBucketName(String prefix) {
      String username = System.getProperty("user.name", "unknown");
      username = username.substring(0, Math.min(username.length(), 10));
      String uuidSuffix = UUID.randomUUID().toString().substring(0, 8);
      return String.format("%s_%s_%s", prefix, username, uuidSuffix);
    }

    public String getUniqueBucketName(String suffix) {
      return testBucketPrefix + "_" + suffix;
    }

    public String getTestBucketPrefix() {
      return testBucketPrefix;
    }

    public void cleanupTestObjects(GoogleCloudStorage storage) throws IOException {
      List<String> bucketsToDelete = new ArrayList<>();

      for (GoogleCloudStorageItemInfo itemInfo : storage.listBucketInfo()) {
        if (itemInfo.getBucketName().startsWith(testBucketPrefix)) {
          bucketsToDelete.add(itemInfo.getBucketName());
        }
      }

      List<StorageResourceId> allResourcesToDelete = new ArrayList<>();
      for (String bucketName : bucketsToDelete) {
        List<String> allObjects = storage.listObjectNames(bucketName, null, null);
        for (String objectName : allObjects) {
          allResourcesToDelete.add(new StorageResourceId(bucketName, objectName));
        }
      }

      try {
        storage.deleteObjects(allResourcesToDelete);
        storage.deleteBuckets(bucketsToDelete);
      } catch (IOException ioe) {
        // Stop the tests from failing on IOException (FNFE or composite) when trying to delete
        // objects or buckets after having been previously deleted, but list inconsistencies
        // make appear in the above lists).
        LOG.warn("Exception encountered when cleaning up test buckets / objects", ioe);
      }
    }
  }
}
