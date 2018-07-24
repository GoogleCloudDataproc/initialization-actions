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

package com.google.cloud.hadoop.gcsio.testing;

import com.google.api.client.util.Clock;
import com.google.cloud.hadoop.gcsio.CreateBucketOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageItemInfo;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageStrings;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.cloud.hadoop.gcsio.UpdatableItemInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * InMemoryGoogleCloudStorage overrides the public methods of GoogleCloudStorage by implementing
 * all the equivalent bucket/object semantics with local in-memory storage.
 */
public class InMemoryGoogleCloudStorage
    implements GoogleCloudStorage {

  // Mapping from bucketName to structs representing a bucket.
  private final Map<String, InMemoryBucketEntry> bucketLookup = new HashMap<>();
  private final GoogleCloudStorageOptions storageOptions;
  private final Clock clock;

  public InMemoryGoogleCloudStorage() {
    storageOptions = GoogleCloudStorageOptions.newBuilder().setAppName("in-memory").build();
    clock = Clock.SYSTEM;
  }

  public InMemoryGoogleCloudStorage(GoogleCloudStorageOptions options) {
    storageOptions = options;
    clock = Clock.SYSTEM;
  }

  public InMemoryGoogleCloudStorage(GoogleCloudStorageOptions storageOptions, Clock clock) {
    this.storageOptions = storageOptions;
    this.clock = clock;
  }

  @Override
  public GoogleCloudStorageOptions getOptions() {
    return storageOptions;
  }

  private boolean validateBucketName(String bucketName) {
    // Validation as per https://developers.google.com/storage/docs/bucketnaming
    if (Strings.isNullOrEmpty(bucketName)) {
      return false;
    }

    if (bucketName.length() < 3) {
      return false;
    }

    if (!bucketName.matches("^[a-z0-9][a-z0-9_.-]*[a-z0-9]$")) {
      return false;
    }

    if (bucketName.length() > 63) {
      return false;
    }

    // TODO(user): Handle dots and names longer than 63, but less than 222.
    return true;
  }

  private boolean validateObjectName(String objectName) {
    // Validation as per https://developers.google.com/storage/docs/bucketnaming
    // Object names must be less than 1024 bytes and may not contain
    // CR or LF characters.
    return !(objectName.length() > 1024
        || objectName.indexOf((char) 0x0A) > -1
        || objectName.indexOf((char) 0x0D) > -1);
  }

  @Override
  public synchronized WritableByteChannel create(StorageResourceId resourceId)
      throws IOException {
    return create(resourceId, CreateObjectOptions.DEFAULT);
  }

  @Override
  public synchronized WritableByteChannel create(
      StorageResourceId resourceId, final CreateObjectOptions options) throws IOException {
    if (!bucketLookup.containsKey(resourceId.getBucketName())) {
      throw new IOException(String.format(
          "Tried to insert object '%s' into nonexistent bucket '%s'",
          resourceId.getObjectName(), resourceId.getBucketName()));
    }
    if (!validateObjectName(resourceId.getObjectName())) {
      throw new IOException("Error creating object. Invalid name: " + resourceId.getObjectName());
    }
    if (resourceId.hasGenerationId() && resourceId.getGenerationId() != 0L) {
      GoogleCloudStorageItemInfo itemInfo = getItemInfo(resourceId);
      if (itemInfo.getContentGeneration() != resourceId.getGenerationId()) {
        throw new IOException(
            String.format(
                "Required generationId '%d' doesn't match existing '%d' for '%s'",
                resourceId.getGenerationId(), itemInfo.getContentGeneration(), resourceId));
      }
    }
    if (!options.overwriteExisting() || resourceId.getGenerationId() == 0L) {
      if (getItemInfo(resourceId).exists()) {
        throw new FileAlreadyExistsException(String.format("%s exists.", resourceId));
      }
    }
    InMemoryObjectEntry entry = new InMemoryObjectEntry(
        resourceId.getBucketName(),
        resourceId.getObjectName(),
        clock.currentTimeMillis(),
        options.getContentType(),
        options.getMetadata());
    bucketLookup.get(resourceId.getBucketName()).add(entry);
    return entry.getWriteChannel();
  }

  @Override
  public synchronized void createEmptyObject(StorageResourceId resourceId)
      throws IOException {
    createEmptyObject(resourceId, CreateObjectOptions.DEFAULT);
  }

  @Override
  public synchronized void createEmptyObject(
      StorageResourceId resourceId, CreateObjectOptions options) throws IOException {
    // TODO(user): Since this class is not performance-tuned, we'll just delegate to the
    // write-channel version of the method.
    create(resourceId, options).close();
  }

  @Override
  public synchronized void createEmptyObjects(List<StorageResourceId> resourceIds)
      throws IOException {
    createEmptyObjects(resourceIds, CreateObjectOptions.DEFAULT);
  }

  @Override
  public synchronized void createEmptyObjects(
      List<StorageResourceId> resourceIds,
      CreateObjectOptions options)
      throws IOException {
    for (StorageResourceId resourceId : resourceIds) {
      createEmptyObject(resourceId, options);
    }
  }

  @Override
  public synchronized SeekableByteChannel open(StorageResourceId resourceId)
      throws IOException {
    return open(resourceId, GoogleCloudStorageReadOptions.DEFAULT);
  }

  @Override
  public SeekableByteChannel open(
      StorageResourceId resourceId, GoogleCloudStorageReadOptions readOptions) throws IOException {
    if (!getItemInfo(resourceId).exists()) {
      throw GoogleCloudStorageExceptions.getFileNotFoundException(
          resourceId.getBucketName(), resourceId.getObjectName());
    }
    return bucketLookup
        .get(resourceId.getBucketName())
        .get(resourceId.getObjectName())
        .getReadChannel(readOptions);
  }

  @Override
  public synchronized void create(String bucketName)
      throws IOException {
    create(bucketName, CreateBucketOptions.DEFAULT);
  }

  @Override
  public synchronized void create(String bucketName, CreateBucketOptions options)
      throws IOException {
    if (!validateBucketName(bucketName)) {
      throw new IOException("Error creating bucket. Invalid name: " + bucketName);
    }
    if (!bucketLookup.containsKey(bucketName)) {
      bucketLookup.put(
          bucketName, new InMemoryBucketEntry(bucketName, clock.currentTimeMillis(), options));
    } else {
      throw new IOException("Bucket '" + bucketName + "'already exists");
    }
  }

  @Override
  public synchronized void deleteBuckets(List<String> bucketNames)
      throws IOException {
    boolean hasError = false;
    for (String bucketName : bucketNames) {
      // TODO(user): Enforcement of not being able to delete non-empty buckets should probably also
      // be in here, but gcsfs handles it explicitly when it calls listObjectNames.
      if (bucketLookup.containsKey(bucketName)) {
        bucketLookup.remove(bucketName);
      } else {
        hasError = true;
      }
      hasError = hasError || !validateBucketName(bucketName);
    }

    if (hasError) {
      throw new IOException("Error deleting");
    }
  }

  @Override
  public synchronized void deleteObjects(List<StorageResourceId> fullObjectNames)
      throws IOException {

    for (StorageResourceId resourceId : fullObjectNames) {
      if (!validateObjectName(resourceId.getObjectName())) {
        throw new IOException("Error deleting object. Invalid name: " + resourceId.getObjectName());
      }
    }

    for (StorageResourceId fullObjectName : fullObjectNames) {
      String bucketName = fullObjectName.getBucketName();
      String objectName = fullObjectName.getObjectName();
      if (fullObjectName.hasGenerationId()) {
        GoogleCloudStorageItemInfo existingInfo = getItemInfo(fullObjectName);
        if (existingInfo.getContentGeneration() != fullObjectName.getGenerationId()) {
          throw new IOException(String.format(
            "Required generationId '%d' doesn't match existing '%d' for '%s'",
            fullObjectName.getGenerationId(),
            existingInfo.getContentGeneration(),
            fullObjectName));
        }
      }
      bucketLookup.get(bucketName).remove(objectName);
    }
  }

  @Override
  public synchronized void copy(String srcBucketName, List<String> srcObjectNames,
      String dstBucketName, List<String> dstObjectNames)
      throws IOException {
    GoogleCloudStorageImpl.validateCopyArguments(srcBucketName, srcObjectNames,
        dstBucketName, dstObjectNames, this);

    // Gather FileNotFoundExceptions for individual objects, but only throw a single combined
    // exception at the end.
    // TODO(user): Add a unittest for this entire class to test for the behavior of partial
    // failures; there is no way to do so in GCSFSIT because it only indirectly calls GCS.copy.
    List<IOException> innerExceptions = new ArrayList<>();

    // Perform the copy operations.
    for (int i = 0; i < srcObjectNames.size(); i++) {
      // Due to the metadata-copy semantics of GCS, we copy the object container, but not the byte[]
      // contents; the write-once constraint means this behavior is indistinguishable from a deep
      // copy, but the behavior might have to become complicated if GCS ever supports appends.
      if (!getItemInfo(new StorageResourceId(srcBucketName, srcObjectNames.get(i))).exists()) {
        innerExceptions.add(GoogleCloudStorageExceptions.getFileNotFoundException(
            srcBucketName, srcObjectNames.get(i)));
        continue;
      }

      InMemoryObjectEntry srcObject =
          bucketLookup.get(srcBucketName).get(srcObjectNames.get(i));
      bucketLookup.get(dstBucketName).add(
          srcObject.getShallowCopy(dstBucketName, dstObjectNames.get(i)));
    }

    if (innerExceptions.size() > 0) {
      throw GoogleCloudStorageExceptions.createCompositeException(innerExceptions);
    }
  }

  @Override
  public synchronized List<String> listBucketNames()
      throws IOException {
    return new ArrayList<>(bucketLookup.keySet());
  }

  @Override
  public synchronized List<GoogleCloudStorageItemInfo> listBucketInfo()
      throws IOException {
    List<GoogleCloudStorageItemInfo> bucketInfos = new ArrayList<>();
    for (InMemoryBucketEntry entry : bucketLookup.values()) {
      bucketInfos.add(entry.getInfo());
    }
    return bucketInfos;
  }

  @Override
  public synchronized List<String> listObjectNames(
      String bucketName, String objectNamePrefix, String delimiter)
      throws IOException {
    return listObjectNames(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  @Override
  public synchronized List<String> listObjectNames(
      String bucketName, String objectNamePrefix, String delimiter,
      long maxResults)
      throws IOException {
    // TODO(user): Add tests for behavior when bucket doesn't exist.
    InMemoryBucketEntry bucketEntry = bucketLookup.get(bucketName);
    if (bucketEntry == null) {
      throw new FileNotFoundException("Bucket not found: " + bucketName);
    }
    Set<String> uniqueNames = new HashSet<>();
    for (String objectName : bucketEntry.getObjectNames()) {
      String processedName = GoogleCloudStorageStrings.matchListPrefix(
          objectNamePrefix, delimiter, objectName);
      if (processedName != null) {
        uniqueNames.add(processedName);
      }
      if (maxResults > 0 && uniqueNames.size() >= maxResults) {
        break;
      }
    }
    return new ArrayList<>(uniqueNames);
  }

  @Override
  public synchronized List<GoogleCloudStorageItemInfo> listObjectInfo(
      final String bucketName, String objectNamePrefix, String delimiter)
      throws IOException {
    return listObjectInfo(bucketName, objectNamePrefix, delimiter,
        GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
  }

  @Override
  public synchronized List<GoogleCloudStorageItemInfo> listObjectInfo(
      final String bucketName, String objectNamePrefix, String delimiter,
      long maxResults)
      throws IOException {
    // Since we're just in memory, we can do the naive implementation of just listing names and
    // then calling getItemInfo for each.
    List<String> listedNames = listObjectNames(bucketName, objectNamePrefix,
        delimiter, GoogleCloudStorage.MAX_RESULTS_UNLIMITED);
    List<GoogleCloudStorageItemInfo> listedInfo = new ArrayList<>();
    for (String objectName : listedNames) {
      GoogleCloudStorageItemInfo itemInfo =
          getItemInfo(new StorageResourceId(bucketName, objectName));
      if (itemInfo.exists()) {
        listedInfo.add(itemInfo);
      } else if (itemInfo.getResourceId().isStorageObject()
                 && storageOptions.isAutoRepairImplicitDirectoriesEnabled()) {
        create(itemInfo.getResourceId()).close();
        GoogleCloudStorageItemInfo newInfo = getItemInfo(itemInfo.getResourceId());
        if (newInfo.exists()) {
          listedInfo.add(newInfo);
        } else if (storageOptions.isInferImplicitDirectoriesEnabled()) {
          // If we fail to do the repair, but inferImplicit is enabled,
          // then we silently add the implicit (as opposed to silently
          // ignoring the failure, which is what we used to do).
          listedInfo.add(
              GoogleCloudStorageItemInfo.createInferredDirectory(itemInfo.getResourceId()));
        }
      } else if (itemInfo.getResourceId().isStorageObject()
                 && storageOptions.isInferImplicitDirectoriesEnabled()) {
        listedInfo.add(
            GoogleCloudStorageItemInfo.createInferredDirectory(itemInfo.getResourceId()));
      }
      if (maxResults > 0 && listedInfo.size() >= maxResults) {
        break;
      }
    }
    return listedInfo;
  }

  @Override
  public synchronized GoogleCloudStorageItemInfo getItemInfo(StorageResourceId resourceId)
      throws IOException {
    if (resourceId.isRoot()) {
      return GoogleCloudStorageItemInfo.ROOT_INFO;
    }

    if (resourceId.isBucket()) {
      if (bucketLookup.containsKey(resourceId.getBucketName())) {
        return bucketLookup.get(resourceId.getBucketName()).getInfo();
      }
    } else {
      if (!validateObjectName(resourceId.getObjectName())) {
        throw new IOException("Error accessing");
      }
      if (bucketLookup.containsKey(resourceId.getBucketName())
          && bucketLookup.get(resourceId.getBucketName()).get(resourceId.getObjectName()) != null) {
        return bucketLookup
            .get(resourceId.getBucketName())
            .get(resourceId.getObjectName())
            .getInfo();
      }
    }
    GoogleCloudStorageItemInfo notFoundItemInfo =
        new GoogleCloudStorageItemInfo(resourceId, 0, -1, null, null);
    return notFoundItemInfo;
  }

  @Override
  public synchronized List<GoogleCloudStorageItemInfo> getItemInfos(
      List<StorageResourceId> resourceIds)
      throws IOException {
    List<GoogleCloudStorageItemInfo> itemInfos = new ArrayList<>();
    for (StorageResourceId resourceId : resourceIds) {
      try {
        itemInfos.add(getItemInfo(resourceId));
      } catch (IOException ioe) {
        throw new IOException("Error getting StorageObject", ioe);
      }
    }
    return itemInfos;
  }

  @Override
  public List<GoogleCloudStorageItemInfo> updateItems(List<UpdatableItemInfo> itemInfoList)
      throws IOException {
    List<GoogleCloudStorageItemInfo> itemInfos = new ArrayList<>();
    for (UpdatableItemInfo updatableItemInfo : itemInfoList) {
      StorageResourceId resourceId = updatableItemInfo.getStorageResourceId();
      Preconditions.checkArgument(
          !resourceId.isRoot() && !resourceId.isBucket(),
          "Can't update item on GCS Root or bucket resources");
      if (!validateObjectName(resourceId.getObjectName())) {
        throw new IOException("Error accessing");
      }
      if (bucketLookup.containsKey(resourceId.getBucketName())
          && bucketLookup.get(resourceId.getBucketName()).get(resourceId.getObjectName()) != null) {
        InMemoryObjectEntry objectEntry =
            bucketLookup.get(resourceId.getBucketName()).get(resourceId.getObjectName());
        objectEntry.patchMetadata(updatableItemInfo.getMetadata());
        itemInfos.add(getItemInfo(resourceId));
      } else {
        throw new IOException(
            String.format("Error getting StorageObject %s", resourceId.toString()));
      }
    }
    return itemInfos;
  }

  @Override
  public void close() {
  }

  @Override
  public void waitForBucketEmpty(String bucketName)
      throws IOException {
  }

  @Override
  public void compose(
      final String bucketName, List<String> sources, String destination, String contentType)
      throws IOException {
    List<StorageResourceId> sourceResourcesIds =
        Lists.transform(
            sources,
            new Function<String, StorageResourceId>() {
              @Override
              public StorageResourceId apply(String s) {
                return new StorageResourceId(bucketName, s);
              }
            });
    StorageResourceId destinationId = new StorageResourceId(bucketName, destination);
    CreateObjectOptions options = new CreateObjectOptions(
        true, contentType, CreateObjectOptions.EMPTY_METADATA);
    composeObjects(sourceResourcesIds, destinationId, options);
  }

  @Override
  public GoogleCloudStorageItemInfo composeObjects(
      List<StorageResourceId> sources,
      final StorageResourceId destination,
      CreateObjectOptions options)
      throws IOException {
    ByteArrayOutputStream tempOutput = new ByteArrayOutputStream();
    for (StorageResourceId sourceId : sources) {
      // TODO(user): If we change to also set generationIds for source objects in the base
      // GoogleCloudStorageImpl, make sure to also add a generationId check here.
      try (SeekableByteChannel sourceChannel = open(sourceId)) {
        byte[] buf = new byte[(int) sourceChannel.size()];
        ByteBuffer reader = ByteBuffer.wrap(buf);
        sourceChannel.read(reader);
        tempOutput.write(buf, 0, buf.length);
      }
    }

    // If destination.hasGenerationId(), it'll automatically get enforced here by the create()
    // implementation.
    WritableByteChannel destChannel = create(destination, options);
    destChannel.write(ByteBuffer.wrap(tempOutput.toByteArray()));
    destChannel.close();
    return getItemInfo(destination);
  }
}
