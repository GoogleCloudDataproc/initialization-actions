/*
 * Copyright 2013 Google Inc. All Rights Reserved.
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

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Insert;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.LoggingMediaHttpUploaderProgressListener;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/** Implements WritableByteChannel to provide write access to GCS. */
public class GoogleCloudStorageWriteChannel
    extends AbstractGoogleAsyncWriteChannel<Insert, StorageObject>
    implements GoogleCloudStorageItemInfo.Provider {

  private static final long MIN_LOGGING_INTERVAL_MS = 60000L;

  private final Storage gcs;
  private final String bucketName;
  private final String objectName;
  private final String kmsKeyName;
  private final ObjectWriteConditions writeConditions;
  private final Map<String, String> metadata;

  private GoogleCloudStorageItemInfo completedItemInfo = null;

  /**
   * Constructs an instance of GoogleCloudStorageWriteChannel.
   *
   * @param uploadThreadPool thread pool to use for running the upload operation
   * @param gcs storage object instance
   * @param requestHelper a ClientRequestHelper to set extra headers
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param writeConditions conditions on which write should be allowed to continue
   * @param objectMetadata metadata to apply to the newly created object
   */
  @Deprecated
  public GoogleCloudStorageWriteChannel(
      ExecutorService uploadThreadPool,
      Storage gcs,
      ClientRequestHelper<StorageObject> requestHelper,
      String bucketName,
      String objectName,
      AsyncWriteChannelOptions options,
      ObjectWriteConditions writeConditions,
      Map<String, String> objectMetadata) {
    this(
        uploadThreadPool,
        gcs,
        requestHelper,
        bucketName,
        objectName,
        /* contentType= */ null,
        /* kmsKeyName= */ null,
        options,
        writeConditions,
        objectMetadata);
  }

  /**
   * Constructs an instance of GoogleCloudStorageWriteChannel.
   *
   * @param uploadThreadPool thread pool to use for running the upload operation
   * @param gcs storage object instance
   * @param requestHelper a ClientRequestHelper to set extra headers
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param writeConditions conditions on which write should be allowed to continue
   * @param objectMetadata metadata to apply to the newly created object
   * @param contentType content type
   */
  @Deprecated
  public GoogleCloudStorageWriteChannel(
      ExecutorService uploadThreadPool,
      Storage gcs,
      ClientRequestHelper<StorageObject> requestHelper,
      String bucketName,
      String objectName,
      AsyncWriteChannelOptions options,
      ObjectWriteConditions writeConditions,
      Map<String, String> objectMetadata,
      String contentType) {
    this(
        uploadThreadPool,
        gcs,
        requestHelper,
        bucketName,
        objectName,
        contentType,
        /* kmsKeyName= */ null,
        options,
        writeConditions,
        objectMetadata);
  }

  /**
   * Constructs an instance of GoogleCloudStorageWriteChannel.
   *
   * @param uploadThreadPool thread pool to use for running the upload operation
   * @param gcs storage object instance
   * @param requestHelper a ClientRequestHelper to set extra headers
   * @param bucketName name of the bucket to create object in
   * @param objectName name of the object to create
   * @param contentType content type
   * @param kmsKeyName Name of Cloud KMS key to use to encrypt the newly created object
   * @param writeConditions conditions on which write should be allowed to continue
   * @param objectMetadata metadata to apply to the newly created object
   */
  public GoogleCloudStorageWriteChannel(
      ExecutorService uploadThreadPool,
      Storage gcs,
      ClientRequestHelper<StorageObject> requestHelper,
      String bucketName,
      String objectName,
      String contentType,
      String kmsKeyName,
      AsyncWriteChannelOptions options,
      ObjectWriteConditions writeConditions,
      Map<String, String> objectMetadata) {
    super(uploadThreadPool, options);
    this.gcs = gcs;
    this.setClientRequestHelper(requestHelper);
    this.bucketName = bucketName;
    this.objectName = objectName;
    if (contentType != null) {
      setContentType(contentType);
    }
    this.kmsKeyName = kmsKeyName;
    this.writeConditions = writeConditions;
    this.metadata = objectMetadata;
  }

  @Override
  public Insert createRequest(InputStreamContent inputStream) throws IOException {
    // Create object with the given name and metadata.
    StorageObject object =
        new StorageObject()
            .setMetadata(metadata)
            .setName(objectName);

    Insert insert = gcs.objects().insert(bucketName, object, inputStream);
    writeConditions.apply(insert);
    if (insert.getMediaHttpUploader() != null) {
      insert.getMediaHttpUploader().setDirectUploadEnabled(isDirectUploadEnabled());
      insert.getMediaHttpUploader().setProgressListener(
        new LoggingMediaHttpUploaderProgressListener(this.objectName, MIN_LOGGING_INTERVAL_MS));
    }
    insert.setName(objectName);
    if (kmsKeyName != null) {
      insert.setKmsKeyName(kmsKeyName);
    }
    return insert;
  }

  @Override
  public void handleResponse(StorageObject response) {
    this.completedItemInfo = GoogleCloudStorageImpl.createItemInfoForStorageObject(
        new StorageResourceId(bucketName, objectName), response);
  }

  /**
   * Returns non-null only if close() has been called and the underlying object has been
   * successfully committed.
   */
  @Override
  public GoogleCloudStorageItemInfo getItemInfo() {
    return this.completedItemInfo;
  }
}
