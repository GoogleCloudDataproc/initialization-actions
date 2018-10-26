/*
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.fail;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpTransport;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Helper methods for GCS integration tests. */
public class GoogleCloudStorageTestHelper {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  // Application name for OAuth.
  public static final String APP_NAME = "GHFS/test";

  private static final int BUFFER_SIZE_MAX_BYTES = 32 * 1024 * 1024;

  public static Credential getCredential() throws IOException {
    String serviceAccount = TestConfiguration.getInstance().getServiceAccount();
    String privateKeyfile = TestConfiguration.getInstance().getPrivateKeyFile();

    assertWithMessage("privateKeyfile must not be null").that(privateKeyfile).isNotNull();
    assertWithMessage("serviceAccount must not be null").that(serviceAccount).isNotNull();
    try {
      CredentialFactory credentialFactory = new CredentialFactory();
      HttpTransport transport = HttpTransportFactory.newTrustedTransport();
      return credentialFactory.getCredentialFromPrivateKeyServiceAccount(
          serviceAccount, privateKeyfile, CredentialFactory.GCS_SCOPES, transport);
    } catch (GeneralSecurityException gse) {
      throw new IOException(gse);
    }
  }

  public static GoogleCloudStorageOptions.Builder getStandardOptionBuilder() {
    return GoogleCloudStorageOptions.newBuilder()
        .setAppName(GoogleCloudStorageTestHelper.APP_NAME)
        .setProjectId(checkNotNull(TestConfiguration.getInstance().getProjectId()))
        .setMaxListItemsPerCall(50)
        .setMaxRequestsPerBatch(2);
  }

  /** More efficient version of checking byte arrays than using Assert.assertArrayEquals. */
  public static void assertByteArrayEquals(byte[] expected, byte[] actual) {
    if ((expected == null) ^ (actual == null)) {
      // Don't use a fancy toString(), just display which is null,
      // which isn't, since the arrays may be very large.
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

  public static void assertObjectContent(
      GoogleCloudStorage gcs, StorageResourceId resourceId, byte[] expectedBytes)
      throws IOException {
    assertObjectContent(gcs, resourceId, expectedBytes, /* expectedBytesCount= */ 1);
  }

  public static void assertObjectContent(
      GoogleCloudStorage gcs, StorageResourceId id, byte[] expectedBytes, int expectedBytesCount)
      throws IOException {
    checkArgument(expectedBytesCount > 0, "expectedBytesCount should be greater than 0");

    int expectedBytesLength = expectedBytes.length;
    long expectedBytesTotalLength = (long) expectedBytesLength * expectedBytesCount;
    ByteBuffer buffer = ByteBuffer.allocate(Math.min(BUFFER_SIZE_MAX_BYTES, expectedBytesLength));
    long totalRead = 0;
    try (ReadableByteChannel channel = gcs.open(id)) {
      int read = channel.read(buffer);
      while (read > 0) {
        totalRead += read;
        assertWithMessage("Bytes read mismatch").that(totalRead).isAtMost(expectedBytesTotalLength);

        buffer.flip();
        byte[] bytesRead = Arrays.copyOf(buffer.array(), buffer.limit());
        byte[] expectedBytesRead = getExpectedBytesRead(expectedBytes, totalRead, read);
        assertByteArrayEquals(expectedBytesRead, bytesRead);

        buffer.clear();
        read = channel.read(buffer);
      }
    }

    assertWithMessage("Bytes read mismatch").that(totalRead).isEqualTo(expectedBytesTotalLength);
  }

  private static byte[] getExpectedBytesRead(byte[] expectedBytes, long totalRead, int read) {
    int expectedBytesLength = expectedBytes.length;
    int expectedBytesStart = (int) ((totalRead - read) % expectedBytesLength);
    int expectedBytesEnd = (int) (totalRead % expectedBytesLength);
    if (expectedBytesStart < expectedBytesEnd) {
      return Arrays.copyOfRange(expectedBytes, expectedBytesStart, expectedBytesEnd);
    }
    // expectedBytesRead is not continuous in expectedBytes partition
    // and need to be copied with 2 method calls
    byte[] expectedBytesRead = new byte[read];
    int firstPartSize = expectedBytesLength - expectedBytesStart;
    System.arraycopy(expectedBytes, expectedBytesStart, expectedBytesRead, 0, firstPartSize);
    System.arraycopy(expectedBytes, 0, expectedBytesRead, firstPartSize, expectedBytesEnd);
    return expectedBytesRead;
  }

  public static void fillBytes(byte[] bytes) {
    new Random().nextBytes(bytes);
  }

  public static byte[] writeObject(
      GoogleCloudStorage gcs, StorageResourceId resourceId, int objectSize) throws IOException {
    return writeObject(gcs, resourceId, objectSize, /* partitionsCount= */ 1);
  }

  public static byte[] writeObject(
      GoogleCloudStorage gcs, StorageResourceId resourceId, int partitionSize, int partitionsCount)
      throws IOException {
    checkArgument(partitionsCount > 0, "partitionsCount should be greater than 0");

    byte[] partition = new byte[partitionSize];
    fillBytes(partition);

    long startTime = System.currentTimeMillis();
    try (WritableByteChannel channel = gcs.create(resourceId)) {
      for (int i = 0; i < partitionsCount; i++) {
        channel.write(ByteBuffer.wrap(partition));
      }
    }
    long endTime = System.currentTimeMillis();
    logger.atInfo().log(
        "Took %s milliseconds to write %s", (endTime - startTime), partitionsCount * partitionSize);
    return partition;
  }

  /** Helper for dealing with buckets in GCS integration tests. */
  public static class TestBucketHelper {
    private static final int MAX_CLEANUP_BUCKETS = 250;

    private static final String DELIMITER = "_";

    // If bucket created before this time, it considered leaked
    private static final long LEAKED_BUCKETS_CUTOFF_TIME =
        Instant.now().minus(Duration.ofHours(6)).toEpochMilli();

    private final String bucketPrefix;
    private final String uniqueBucketPrefix;

    public TestBucketHelper(String bucketPrefix) {
      this.bucketPrefix = bucketPrefix + DELIMITER;
      this.uniqueBucketPrefix = makeBucketName(bucketPrefix);
      checkState(
          this.uniqueBucketPrefix.startsWith(this.bucketPrefix),
          "uniqueBucketPrefix should start with bucketPrefix");
    }

    private static String makeBucketName(String prefix) {
      String username = System.getProperty("user.name", "unknown").replace("-", "");
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
      logger.atInfo().log(
          "Cleaning up GCS buckets that start with %s prefix or leaked", uniqueBucketPrefix);

      List<String> bucketsToDelete = new ArrayList<>();
      for (GoogleCloudStorageItemInfo bucketInfo : storage.listBucketInfo()) {
        String bucketName = bucketInfo.getBucketName();
        if (bucketName.startsWith(bucketPrefix)
            && (bucketName.startsWith(uniqueBucketPrefix)
                || bucketInfo.getCreationTime() < LEAKED_BUCKETS_CUTOFF_TIME)) {
          bucketsToDelete.add(bucketName);
        }
      }
      // randomize buckets order in case concurrent clean ups are running
      Collections.shuffle(bucketsToDelete);
      if (bucketsToDelete.size() > MAX_CLEANUP_BUCKETS) {
        logger.atInfo().log(
            "GCS has %s buckets to cleanup. It's too many, will cleanup only %s buckets: %s",
            bucketsToDelete.size(), MAX_CLEANUP_BUCKETS, bucketsToDelete);
        bucketsToDelete = bucketsToDelete.subList(0, MAX_CLEANUP_BUCKETS);
      } else {
        logger.atInfo().log(
            "GCS has %s buckets to cleanup: %s", bucketsToDelete.size(), bucketsToDelete);
      }

      List<GoogleCloudStorageItemInfo> objectsToDelete =
          bucketsToDelete
              .parallelStream()
              .flatMap(
                  bucket -> {
                    try {
                      return storage.listObjectInfo(bucket, null, null).stream();
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(toImmutableList());
      logger.atInfo().log(
          "GCS has %s objects to cleanup: %s", objectsToDelete.size(), objectsToDelete);

      try {
        storage.deleteObjects(
            Lists.transform(objectsToDelete, GoogleCloudStorageItemInfo::getResourceId));
        storage.deleteBuckets(bucketsToDelete);
      } catch (IOException ioe) {
        logger.atWarning().withCause(ioe).log(
            "Caught exception during GCS (%s) buckets cleanup", storage);
      }

      logger.atInfo().log("GCS cleaned up in %s seconds", storageStopwatch.elapsed().getSeconds());
    }
  }
}
