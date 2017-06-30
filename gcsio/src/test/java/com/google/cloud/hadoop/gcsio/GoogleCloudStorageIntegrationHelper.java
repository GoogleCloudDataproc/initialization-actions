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

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

/**
 * Integration tests for GoogleCloudStorage class.
 */
public abstract class GoogleCloudStorageIntegrationHelper {
  // Logger.
  protected static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(GoogleCloudStorageIntegrationHelper.class);

  // Application name for OAuth.
  static final String APP_NAME = "GCS-test";

  // Name of environment variables from which to get GCS access info.
  public static final String GCS_TEST_PROJECT_ID = "GCS_TEST_PROJECT_ID";
  public static final String GCS_TEST_CLIENT_ID = "GCS_TEST_CLIENT_ID";
  public static final String GCS_TEST_CLIENT_SECRET = "GCS_TEST_CLIENT_SECRET";

  // Prefix used for naming test buckets.
  public static final String TEST_BUCKET_NAME_PREFIX = "gcsio-test-";

  // Regex pattern to match UUID string.
  public static final String UUID_PATTERN =
      "\\p{XDigit}{8}+-\\p{XDigit}{4}+-\\p{XDigit}{4}+-\\p{XDigit}{4}+-\\p{XDigit}{12}+";

  // Regex that matches a test bucket name.
  public static final String TEST_BUCKET_NAME_PATTERN =
      "^" + TEST_BUCKET_NAME_PREFIX + UUID_PATTERN + ".*";

  // Name of the test object.
  //protected static String objectName = "gcsio-test.txt";

  // Name of test buckets.
  public String bucketName;
  public String otherBucketName;

  // List of buckets to delete during post-test cleanup.
  private List<String> bucketsToDelete = new ArrayList<>();

  /**
   * Perform initialization once before tests are run.
   */
  public void beforeAllTests() throws IOException {
    // Un-comment the following to enable logging during test run.
    //enableLogging();

    // Create a couple of buckets. The first one is used by most tests.
    // The second one is used by some tests (eg, copy()).
    // TODO(user): bucketName and otherBucketName are too generic names.
    // They should be renamed to testBucketName and otherTestBucketName.
    bucketName = createTempBucket();
    otherBucketName = createTempBucket();
  }

  /**
   * Perform clean-up once after all tests are turn.
   */
  public void afterAllTests()
      throws IOException {

    // Delete buckets created during test.
    deleteBuckets();
  }

  /**
   * Writes a file with the give text at the given path.
   *
   * Note: This method takes non-trivial amount of time to complete.
   * If you use it in a test where multiple operations on the same file are performed,
   * then it is better to create the file once and perform multiple operations in
   * the same test rather than have multiple tests each creating its own test file.
   *
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param text file contents
   * @return number of bytes written
   */
  protected int writeTextFile(String bucketName, String objectName, String text)
      throws IOException {
    int numBytesWritten = -1;
    byte[] textBytes = text.getBytes("UTF-8");
    ByteBuffer writeBuffer = ByteBuffer.wrap(textBytes);
    numBytesWritten = writeFile(bucketName, objectName, writeBuffer, 1);
    return numBytesWritten;
  }

  /**
   * Writes a file with the given buffer repeated numWrites times.
   *
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param buffer Data to write
   * @param numWrites number of times to repeat the data
   * @return number of bytes written
   */
  protected int writeFile(String bucketName, String objectName, ByteBuffer buffer, int numWrites)
      throws IOException {
    int numBytesWritten = -1;
    int totalBytesWritten = 0;
    WritableByteChannel writeChannel = null;

    try {
      writeChannel = create(
          bucketName, objectName, new CreateFileOptions(false /* overwrite existing */));
      for (int i = 0; i < numWrites; i++) {
        buffer.clear();
        numBytesWritten = writeChannel.write(buffer);
        Assert.assertEquals("could not write the entire buffer",
            buffer.capacity(), numBytesWritten);
        totalBytesWritten += numBytesWritten;
      }

    } finally {
      if (writeChannel != null) {
        writeChannel.close();
      }
    }

    return totalBytesWritten;
  }

  /**
   * Helper which reads the entire file as a String.
   */
  protected String readTextFile(String bucketName, String objectName)
      throws IOException {
    SeekableByteChannel readChannel = null;
    ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    StringBuffer returnBuffer = new StringBuffer();

    try {
      readChannel = open(bucketName, objectName);
      int numBytesRead = readChannel.read(readBuffer);
      while (numBytesRead > 0) {
        readBuffer.flip();
        returnBuffer.append(StandardCharsets.UTF_8.decode(readBuffer));
        readBuffer.clear();
        numBytesRead = readChannel.read(readBuffer);
      }
    } finally {
      if (readChannel != null) {
        readChannel.close();
      }
    }
    return returnBuffer.toString();
  }

  /**
   * Helper that reads text from the given file at the given offset
   * and returns it. If checkOverflow is true, it will make sure that
   * no more than 'len' bytes were read.
   */
  protected String readTextFile(
      String bucketName, String objectName, int offset, int len, boolean checkOverflow)
      throws IOException {
    String text = null;
    SeekableByteChannel readChannel = null;

    try {
      int bufferSize = len;
      bufferSize += checkOverflow ? 1 : 0;
      ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
      readChannel = open(bucketName, objectName);
      if (offset > 0) {
        readChannel.position(offset);
      }
      int numBytesRead = readChannel.read(readBuffer);
      Assert.assertEquals("readTextFile: read size mismatch", len, numBytesRead);
      readBuffer.flip();
      text = StandardCharsets.UTF_8.decode(readBuffer).toString();
    } finally {
      if (readChannel != null) {
        readChannel.close();
      }
    }

    return text;
  }

  /**
   * Opens the given object for reading.
   */
  protected abstract SeekableByteChannel open(String bucketName, String objectName)
      throws IOException;


  /**
   * Opens the given object for writing.
   */
  protected WritableByteChannel create(String bucketName, String objectName) throws IOException {
    return create(bucketName, objectName, CreateFileOptions.DEFAULT);
  }

  /**
   * Opens the given object for writing.
   */
  protected abstract WritableByteChannel create(
      String bucketName, String objectName, CreateFileOptions options) throws IOException;

  /**
   * Creates a directory like object.
   */
  protected abstract void mkdir(String bucketName, String objectName)
      throws IOException;

  /**
   * Creates the given bucket.
   */
  protected abstract void mkdir(String bucketName) throws IOException;

  /**
   * Deletes the given bucket.
   */
  protected abstract void delete(String bucketName) throws IOException;

  /**
   * Deletes the given object.
   */
  protected abstract void delete(String bucketName, String objectName)
      throws IOException;

  /**
   * Deletes all objects from the given bucket.
   */
  protected abstract void clearBucket(String bucketName)
      throws IOException;

  // -----------------------------------------------------------------
  // Misc helpers
  // -----------------------------------------------------------------

  /**
   * Gets the expected size of a test object. Subclasses are allowed to return Long.MIN_VALUE to
   * denote "don't care" or "don't know".
   *
   * This function assumes a certain behavior when we create test objects.
   * See {@link createObjects()} for details.
   */
  public long getExpectedObjectSize(String objectName, boolean expectedToExist)
      throws UnsupportedEncodingException {
    // Determine the expected size.
    long expectedSize;
    if (expectedToExist) {
      if (Strings.isNullOrEmpty(objectName)
          || objectName.endsWith(GoogleCloudStorage.PATH_DELIMITER)) {
        expectedSize = 0;
      } else {
        expectedSize = objectName.getBytes("UTF-8").length;
      }
    } else {
      expectedSize = -1;
    }
    return expectedSize;
  }

  /**
   * Indicates whether the given object name looks like a directory path.
   *
   * @param objectName name of the object to inspect
   * @return whether the given object name looks like a directory path
   */
  public boolean objectHasDirectoryPath(String objectName) {
    return FileInfo.objectHasDirectoryPath(objectName);
  }

  /**
   * Creates objects in the given bucket.
   * For objects whose name looks like a path (foo/bar/zoo), creates
   * objects for intermediate sub-paths.
   * for example,
   * foo/bar/zoo => creates: foo/, foo/bar/, foo/bar/zoo.
   */
  public void createObjectsWithSubdirs(String bucketName, String[] objectNames)
      throws IOException {
    List<String> allNames = new ArrayList<>();
    List<String> subdirs;
    Set<String> created = new HashSet<>();
    for (String objectName : objectNames) {
      subdirs = getSubdirs(objectName);
      for (String subdir : subdirs) {
        if (!created.contains(subdir)) {
          created.add(subdir);
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
   * For objects whose name looks like a path (foo/bar/zoo),
   * returns intermediate sub-paths.
   * for example,
   * foo/bar/zoo => returns: (foo/, foo/bar/)
   * foo => returns: ()
   */
  private List<String> getSubdirs(String objectName) {
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

  /**
   * Creates objects with the given names in the given bucket.
   */
  private void createObjects(final String bucketName, String[] objectNames)
      throws IOException {

    final ExecutorService threadPool = Executors.newCachedThreadPool();
    final CountDownLatch counter = new CountDownLatch(objectNames.length);
    List<Future<?>> futures = new ArrayList<>();
    // Do each creation asynchronously.
    for (final String objectName : objectNames) {
      Future<?> future = threadPool.submit(new Runnable() {
        @Override
        public void run() {
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
        }
      });
      futures.add(future);
    }

    try {
      counter.await();
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted while awaiting object creation!", ie);
    }  finally {
      threadPool.shutdown();
      try {
        if (!threadPool.awaitTermination(10L, TimeUnit.SECONDS)) {
          System.err.println("Failed to awaitTermination! Forcing executor shutdown.");
          threadPool.shutdownNow();
        }
      } catch (InterruptedException ie) {
        throw new IOException("Interrupted while shutting down threadpool!", ie);
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
   * Gets the credential tests should use for accessing GCS.
   */
  @Deprecated
  Credential getCredential()
      throws IOException {
    String clientId = System.getenv(GCS_TEST_CLIENT_ID);
    String clientSecret = System.getenv(GCS_TEST_CLIENT_SECRET);
    Assert.assertNotNull("clientId must not be null", clientId);
    Assert.assertNotNull("clientSecret must not be null", clientSecret);
    Credential credential;
    try {
      CredentialFactory credentialFactory = new CredentialFactory();
      credential =
          credentialFactory.getStorageCredential(clientId, clientSecret);
    } catch (GeneralSecurityException gse) {
      throw new IOException(gse);
    }
    return credential;
  }

  /**
   * Gets randomly generated name of a bucket.
   * The name is prefixed with an identifiable string. A bucket created by this method
   * can be identified by calling isTestBucketName() for that bucket.
   */
  public String getUniqueBucketName() {
    return getUniqueBucketName("");
  }

  /**
   * Gets randomly generated name of a bucket with the given suffix.
   * The name is prefixed with an identifiable string. A bucket created by this method
   * can be identified by calling isTestBucketName() for that bucket.
   */
  public String getUniqueBucketName(String suffix) {
    UUID uuid = UUID.randomUUID();
    String uniqueBucketName = TEST_BUCKET_NAME_PREFIX + uuid.toString() + suffix;
    // In most cases, caller creates a bucket with this name therefore we proactively add the name
    // to the list of buckets to be deleted. It is ok even if a bucket is not created with
    // this name because deleteBuckets() will ignore non-existent buckets.
    addToDeleteBucketList(uniqueBucketName);
    return uniqueBucketName;
  }

  /**
   * Indicates whether the given name matches the pattern used for
   * naming a test bucket.
   */
  boolean isTestBucketName(String bucketName) {
    return bucketName.matches(TEST_BUCKET_NAME_PATTERN);
  }

  /**
   * Gets randomly generated name of a file like object.
   */
  String getUniqueFileObjectName() {
    return "file-" + UUID.randomUUID().toString();
  }

  /**
   * Gets randomly generated name of a directory like object.
   */
  String getUniqueDirectoryObjectName() {
    return "dir-" + UUID.randomUUID().toString();
  }

  /**
   * Creates a bucket and adds it to the list of buckets to delete
   * at the end of tests.
   */
  public void createTempBucket(String tempBucketName)
      throws IOException {
    mkdir(tempBucketName);
    addToDeleteBucketList(tempBucketName);
  }

  /**
   * Creates a bucket and adds it to the list of buckets to delete
   * at the end of tests.
   */
  String createTempBucket()
      throws IOException {
    String tempBucketName = getUniqueBucketName();
    createTempBucket(tempBucketName);
    return tempBucketName;
  }

  /**
   * Add the given bucket name to the list of buckets to delete
   * during post-test cleanup.
   */
  void addToDeleteBucketList(String bucketToDelete) {
    // We do not create more than a few buckets during tests, therefore linear search
    // to find duplicates is ok.
    if (!bucketsToDelete.contains(bucketToDelete)) {
      bucketsToDelete.add(bucketToDelete);
    }
  }

  /**
   * Delete buckets that were temporarily created during tests.
   */
  public void deleteBuckets() {
    for (String bucket : bucketsToDelete) {
      try {
        clearBucket(bucket);
        delete(bucket);
      } catch (IOException e) {
        LOG.warn("deleteBuckets: " + e.getLocalizedMessage());
      }
    }
    bucketsToDelete.clear();
  }

  /**
   * Enable debug logger.
   */
  void enableLogging() {
    System.setProperty(
        "org.apache.commons.logging.Log",
        "org.apache.commons.logging.impl.Log4JLogger");
    ConsoleAppender consoleAppender = new ConsoleAppender();
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.addAppender(consoleAppender);
    rootLogger.setLevel(Level.DEBUG);
  }
}
