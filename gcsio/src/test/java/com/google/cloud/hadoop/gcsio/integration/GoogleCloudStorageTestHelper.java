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

import static com.google.common.truth.Truth.assertThat;
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
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper methods for GCS integration tests. */
public class GoogleCloudStorageTestHelper {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageTestHelper.class);

  // Application name for OAuth.
  public static final String APP_NAME = "GHFS/test";

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
    assertThat(projectId).isNotNull();
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
        assertThat(read).isEqualTo(patternSize);
        // Writing to the buffer will write tot he destination bytes array.
        assertByteArrayEquals(bytePattern, destinationBytes);
        // Set the buffer back to the beginning
        buffer.flip();
      }
    }
    endTime = System.currentTimeMillis();
    LOG.info("Took {} milliseconds to read {}", (endTime - startTime), repetitions * patternSize);
  }

  /** Helper for dealing with buckets in GCS integration tests. */
  public static class TestBucketHelper {

    private static final int MAX_CLEANUP_BUCKETS = 1000;

    private static final String DELIMITER = "_";

    // If bucket created before this time, it considered leaked
    private static final long LEAKED_BUCKETS_CUTOFF_TIME =
        Instant.now().minus(Duration.standardHours(6)).getMillis();

    private static final Function<GoogleCloudStorageItemInfo, StorageResourceId>
        INFO_TO_RESOURCE_ID_FN =
            new Function<GoogleCloudStorageItemInfo, StorageResourceId>() {
              @Override
              public StorageResourceId apply(GoogleCloudStorageItemInfo info) {
                return info.getResourceId();
              }
            };

    private final String bucketPrefix;
    private final String uniqueBucketPrefix;

    public TestBucketHelper(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix + DELIMITER;
      this.uniqueBucketPrefix = makeBucketName(bucketPrefix);
    }

    private static String makeBucketName(String prefix) {
      String username = System.getProperty("user.name", "unknown");
      username = username.substring(0, Math.min(username.length(), 10));
      String uuidSuffix = UUID.randomUUID().toString().substring(0, 8);
      return prefix + DELIMITER + username + DELIMITER + uuidSuffix;
    }

    public String getUniqueBucketName(String suffix) {
      return uniqueBucketPrefix + DELIMITER + suffix;
    }

    public String getUniqueBucketPrefix() {
      return uniqueBucketPrefix;
    }

    public void cleanup(GoogleCloudStorage storage) throws IOException {
      Stopwatch storageStopwatch = Stopwatch.createStarted();
      LOG.info("Cleaning up GCS buckets that start with {} prefix or leaked", uniqueBucketPrefix);

      List<String> bucketsToDelete = new ArrayList<>();
      for (GoogleCloudStorageItemInfo bucketInfo : storage.listBucketInfo()) {
        String bucketName = bucketInfo.getBucketName();
        if (bucketName.startsWith(bucketPrefix)
            && (bucketName.startsWith(uniqueBucketPrefix)
                || bucketInfo.getCreationTime() < LEAKED_BUCKETS_CUTOFF_TIME)) {
          bucketsToDelete.add(bucketName);
        }
      }
      if (bucketsToDelete.size() > MAX_CLEANUP_BUCKETS) {
        LOG.info("GCS has {} buckets to cleanup. It's too many, will cleanup only {} buckets: {}",
            bucketsToDelete.size(), MAX_CLEANUP_BUCKETS, bucketsToDelete);
        bucketsToDelete = bucketsToDelete.subList(0, MAX_CLEANUP_BUCKETS);
      } else {
        LOG.info("GCS has {} buckets to cleanup: {}", bucketsToDelete.size(), bucketsToDelete);
      }

      List<GoogleCloudStorageItemInfo> objectsToDelete = new ArrayList<>();
      for (String bucket : bucketsToDelete) {
        objectsToDelete.addAll(storage.listObjectInfo(bucket, null, null));
      }
      LOG.info("GCS has {} objects to cleanup: {}", objectsToDelete.size(), objectsToDelete);

      try {
        storage.deleteObjects(Lists.transform(objectsToDelete, INFO_TO_RESOURCE_ID_FN));
        storage.deleteBuckets(bucketsToDelete);
      } catch (IOException ioe) {
        LOG.warn("Caught exception during GCS buckets cleanup", storage, ioe);
      }

      LOG.info("GCS cleaned up in {} seconds", storageStopwatch.elapsed(TimeUnit.SECONDS));
    }
  }
}
