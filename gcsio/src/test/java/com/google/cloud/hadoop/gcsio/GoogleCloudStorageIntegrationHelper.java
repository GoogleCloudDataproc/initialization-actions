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

package com.google.cloud.hadoop.gcsio;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.MultipartContent;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Integration tests for GoogleCloudStorage class. */
public abstract class GoogleCloudStorageIntegrationHelper {

  // Application name for OAuth.
  public static final String APP_NAME = "GCS-test";

  // Prefix used for naming test buckets.
  private static final String TEST_BUCKET_NAME_PREFIX = "gcsio-test";

  private final TestBucketHelper bucketHelper = new TestBucketHelper(TEST_BUCKET_NAME_PREFIX);

  // Name of test buckets.
  public String sharedBucketName1;
  public String sharedBucketName2;

  final GoogleCloudStorage gcs;

  public GoogleCloudStorageIntegrationHelper(GoogleCloudStorage gcs) {
    this.gcs = gcs;
  }

  /** Perform initialization once before tests are run. */
  public void beforeAllTests() throws IOException {
    // Create a couple of buckets. The first one is used by most tests.
    // The second one is used by some tests (eg, copy()).
    sharedBucketName1 = createUniqueBucket("shared-1");
    sharedBucketName2 = createUniqueBucket("shared-2");
  }

  /** Perform clean-up once after all tests are turn. */
  public void afterAllTests() {
    try {
      bucketHelper.cleanup(gcs);
    } catch (IOException e) {
      throw new RuntimeException("Failed to cleanup test buckets", e);
    }
  }

  /**
   * Writes a file with the give text at the given path. Do not allow overwriting existing files.
   *
   * <p>Note: This method takes non-trivial amount of time to complete. If you use it in a test
   * where multiple operations on the same file are performed, then it is better to create the file
   * once and perform multiple operations in the same test rather than have multiple tests each
   * creating its own test file.
   *
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param text file contents
   * @return number of bytes written
   */
  public int writeTextFile(String bucketName, String objectName, String text) throws IOException {
    byte[] textBytes = text.getBytes(UTF_8);
    return writeFile(bucketName, objectName, textBytes, 1);
  }

  /**
   * Writes a file with the give text at the given path. Allow overwriting an existing file.
   *
   * <p>Note: This method takes non-trivial amount of time to complete. If you use it in a test
   * where multiple operations on the same file are performed, then it is better to create the file
   * once and perform multiple operations in the same test rather than have multiple tests each
   * creating its own test file.
   *
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param text file contents
   * @return number of bytes written
   */
  protected int writeTextFileOverwriting(String bucketName, String objectName, String text)
      throws IOException {
    byte[] textBytes = text.getBytes(UTF_8);
    return writeFileOverwriting(bucketName, objectName, textBytes, 1);
  }

  /**
   * Writes a file with the given buffer repeated numWrites times
   *
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param buffer Data to write
   * @param numWrites number of times to repeat the data
   * @param overwriteExisting flag to indicate whether to overwrite if file already exists.
   * @return number of bytes written
   */
  protected int writeFile(
      String bucketName, String objectName, byte[] buffer, int numWrites, boolean overwriteExisting)
      throws IOException {
    int totalBytesWritten = 0;

    try (WritableByteChannel writeChannel =
        create(bucketName, objectName, new CreateFileOptions(overwriteExisting))) {
      for (int i = 0; i < numWrites; i++) {
        int numBytesWritten = writeChannel.write(ByteBuffer.wrap(buffer));
        assertWithMessage("could not write the entire buffer")
            .that(numBytesWritten)
            .isEqualTo(buffer.length);
        totalBytesWritten += numBytesWritten;
      }
    }

    return totalBytesWritten;
  }

  /**
   * Writes a file with the given buffer repeated numWrites times. Do not allow overwriting if file
   * already exists.
   *
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param buffer Data to write
   * @param numWrites number of times to repeat the data
   * @return number of bytes written
   */
  protected int writeFile(String bucketName, String objectName, byte[] buffer, int numWrites)
      throws IOException {
    return writeFile(bucketName, objectName, buffer, numWrites, /* overwriteExisting= */ false);
  }

  /**
   * Writes a file with the given buffer repeated numWrites times. If file already exists, overwrite
   * it.
   *
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param buffer Data to write
   * @param numWrites number of times to repeat the data
   * @return number of bytes written
   */
  protected int writeFileOverwriting(
      String bucketName, String objectName, byte[] buffer, int numWrites) throws IOException {
    return writeFile(bucketName, objectName, buffer, numWrites, /* overwriteExisting= */ true);
  }

  /** Helper which reads the entire file as a String. */
  public String readTextFile(String bucketName, String objectName) throws IOException {
    ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    StringBuilder returnBuffer = new StringBuilder();

    try (SeekableByteChannel readChannel = open(bucketName, objectName)) {
      int numBytesRead = readChannel.read(readBuffer);
      while (numBytesRead > 0) {
        readBuffer.flip();
        returnBuffer.append(StandardCharsets.UTF_8.decode(readBuffer));
        readBuffer.clear();
        numBytesRead = readChannel.read(readBuffer);
      }
    }

    return returnBuffer.toString();
  }

  /**
   * Helper that reads text from the given file at the given offset and returns it. If checkOverflow
   * is true, it will make sure that no more than 'len' bytes were read.
   */
  protected String readTextFile(
      String bucketName, String objectName, int offset, int len, boolean checkOverflow)
      throws IOException {
    int bufferSize = len + (checkOverflow ? 1 : 0);
    ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);

    try (SeekableByteChannel readChannel = open(bucketName, objectName)) {
      if (offset > 0) {
        readChannel.position(offset);
      }
      int numBytesRead = readChannel.read(readBuffer);
      assertWithMessage("readTextFile: read size mismatch").that(numBytesRead).isEqualTo(len);
    }

    readBuffer.flip();
    return StandardCharsets.UTF_8.decode(readBuffer).toString();
  }

  /** Helper that reads text from a given SeekableByteChannel. */
  protected String readText(
      SeekableByteChannel readChannel, int offset, int len, boolean checkOverflow)
      throws IOException {
    int bufferSize = len + (checkOverflow ? 1 : 0);
    ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
    if (offset > 0) {
      readChannel.position(offset);
    }

    int numBytesRead = readChannel.read(readBuffer);
    assertWithMessage("readText: read size mismatch").that(numBytesRead).isEqualTo(len);

    readBuffer.flip();
    return StandardCharsets.UTF_8.decode(readBuffer).toString();
  }

  /** Opens the given object for reading. */
  protected abstract SeekableByteChannel open(String bucketName, String objectName)
      throws IOException;

  /** Opens the given object for reading, with the specified read options. */
  protected abstract SeekableByteChannel open(
      String bucketName, String objectName, GoogleCloudStorageReadOptions readOptions)
      throws IOException;

  /** Opens the given object for writing. */
  protected WritableByteChannel create(String bucketName, String objectName) throws IOException {
    return create(bucketName, objectName, CreateFileOptions.DEFAULT);
  }

  /** Opens the given object for writing. */
  protected abstract WritableByteChannel create(
      String bucketName, String objectName, CreateFileOptions options) throws IOException;

  /** Creates a directory like object. */
  protected abstract void mkdir(String bucketName, String objectName) throws IOException;

  /** Creates the given bucket. */
  protected abstract void mkdir(String bucketName) throws IOException;

  /** Deletes the given bucket. */
  protected abstract void delete(String bucketName) throws IOException;

  /** Deletes the given object. */
  protected abstract void delete(String bucketName, String objectName) throws IOException;

  /** Deletes all objects from the given bucket. */
  protected abstract void clearBucket(String bucketName) throws IOException;

  // -----------------------------------------------------------------
  // Misc helpers
  // -----------------------------------------------------------------

  /**
   * Gets the expected size of a test object. Subclasses are allowed to return Long.MIN_VALUE to
   * denote "don't care" or "don't know".
   *
   * <p>This function assumes a certain behavior when we create test objects. See {@link
   * #createObjects(String, String[])} for details.
   */
  public long getExpectedObjectSize(String objectName, boolean expectedToExist)
      throws UnsupportedEncodingException {
    // Determine the expected size.
    if (expectedToExist) {
      if (Strings.isNullOrEmpty(objectName)
          || objectName.endsWith(GoogleCloudStorage.PATH_DELIMITER)) {
        return 0;
      }
      return objectName.getBytes(UTF_8).length;
    }
    return -1;
  }

  /**
   * Creates objects in the given bucket. For objects whose name looks like a path (foo/bar/zoo),
   * creates objects for intermediate sub-paths.
   *
   * <p>For example, foo/bar/zoo => creates: foo/, foo/bar/, foo/bar/zoo.
   */
  public void createObjectsWithSubdirs(String bucketName, String... objectNames) throws Exception {
    List<String> allNames = new ArrayList<>();
    Set<String> created = new HashSet<>();
    for (String objectName : objectNames) {
      for (String subdir : getSubdirs(objectName)) {
        if (created.add(subdir)) {
          allNames.add(subdir);
        }
      }

      if (!created.contains(objectName)) {
        allNames.add(objectName);
      }
    }

    createObjects(bucketName, allNames.toArray(new String[0]));
  }

  /**
   * For objects whose name looks like a path (foo/bar/zoo), returns intermediate sub-paths.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>foo/bar/zoo => returns: (foo/, foo/bar/)
   *   <li>foo => returns: ()
   * </ul>
   */
  private List<String> getSubdirs(String objectName) {
    checkArgument(
        isNullOrEmpty(objectName) || objectName.charAt(0) != '/',
        "objectName can not start from '/': %s",
        objectName);
    List<String> subdirs = new ArrayList<>();
    // Create a list of all subdirs.
    // for example,
    // foo/bar/zoo => (foo/, foo/bar/)
    int currentIndex = 0;
    while (currentIndex < objectName.length()) {
      int index = objectName.indexOf('/', currentIndex);
      if (index < 0) {
        break;
      }
      subdirs.add(objectName.substring(0, index + 1));
      currentIndex = index + 1;
    }

    return subdirs;
  }

  /** Creates objects with the given names in the given bucket. */
  private void createObjects(final String bucketName, String[] objectNames) throws Exception {
    final ExecutorService threadPool = Executors.newCachedThreadPool();
    final CountDownLatch counter = new CountDownLatch(objectNames.length);
    List<Future<?>> futures = new ArrayList<>();
    // Do each creation asynchronously.
    for (final String objectName : objectNames) {
      Future<?> future =
          threadPool.submit(
              () -> {
                try {
                  if (objectName.endsWith(GoogleCloudStorage.PATH_DELIMITER)) {
                    mkdir(bucketName, objectName);
                  } else {
                    // Just use objectName as file contents.
                    writeTextFile(bucketName, objectName, objectName);
                  }
                } catch (Throwable ioe) {
                  throw new RuntimeException(
                      String.format("Exception creating %s/%s", bucketName, objectName), ioe);
                } finally {
                  counter.countDown();
                }
              });
      futures.add(future);
    }

    try {
      counter.await();
    } finally {
      threadPool.shutdown();
      if (!threadPool.awaitTermination(10L, TimeUnit.SECONDS)) {
        System.err.println("Failed to awaitTermination! Forcing executor shutdown.");
        threadPool.shutdownNow();
      }
    }

    for (Future<?> future : futures) {
      try {
        // We should already be done.
        future.get(10, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        throw new IOException("Creation of file failed with exception", e);
      }
    }
  }

  /**
   * Gets randomly generated name of a bucket.
   *
   * <p>The name is prefixed with an identifiable string. A bucket created by this method can be
   * identified by calling isTestBucketName() for that bucket.
   */
  public String getUniqueBucketName() {
    return getUniqueBucketName("");
  }

  /**
   * Gets randomly generated name of a bucket with the given suffix.
   *
   * <p>The name is prefixed with an identifiable string. A bucket created by this method can be
   * identified by calling isTestBucketName() for that bucket.
   */
  public String getUniqueBucketName(String suffix) {
    return bucketHelper.getUniqueBucketName(suffix);
  }

  /** Convert request to string representation that could be used for assertions in tests */
  public static String requestToString(HttpRequest request) {
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

  /** Creates a bucket and adds it to the list of buckets to delete at the end of tests. */
  public String createUniqueBucket(String suffix) throws IOException {
    String bucketName = getUniqueBucketName(suffix);
    mkdir(bucketName);
    return bucketName;
  }
}
