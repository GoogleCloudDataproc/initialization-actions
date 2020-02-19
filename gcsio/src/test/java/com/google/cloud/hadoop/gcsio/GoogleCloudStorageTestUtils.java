/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import java.io.IOException;
import java.util.UUID;

/** Utility class with helper methods for GCS IO tests. */
public final class GoogleCloudStorageTestUtils {

  public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  public static final JacksonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  static final String GOOGLEAPIS_ENDPOINT = "https://storage.googleapis.com";

  private static final String RESUMABLE_UPLOAD_LOCATION_FORMAT =
      GOOGLEAPIS_ENDPOINT + "/upload/storage/v1/b/%s/o?name=%s&uploadType=resumable&upload_id=%s";

  static final String BUCKET_NAME = "foo-bucket";
  static final String OBJECT_NAME = "bar-object";

  private static final ApiErrorExtractor ERROR_EXTRACTOR = ApiErrorExtractor.INSTANCE;
  private static final ClientRequestHelper<StorageObject> REQUEST_HELPER =
      new ClientRequestHelper<>();

  private GoogleCloudStorageTestUtils() {}

  public static GoogleCloudStorageReadChannel createReadChannel(
      Storage storage, GoogleCloudStorageReadOptions options) throws IOException {
    return new GoogleCloudStorageReadChannel(
        storage, BUCKET_NAME, OBJECT_NAME, ERROR_EXTRACTOR, REQUEST_HELPER, options);
  }

  public static GoogleCloudStorageReadChannel createReadChannel(
      Storage storage, GoogleCloudStorageReadOptions options, long generation) throws IOException {
    return new GoogleCloudStorageReadChannel(
        storage,
        new StorageResourceId(BUCKET_NAME, OBJECT_NAME, generation),
        ERROR_EXTRACTOR,
        REQUEST_HELPER,
        options);
  }


  public static MockLowLevelHttpResponse resumableUploadResponse(String bucket, String object) {
    String uploadId = UUID.randomUUID().toString();
    return new MockLowLevelHttpResponse()
        .addHeader(
            "location", String.format(RESUMABLE_UPLOAD_LOCATION_FORMAT, bucket, object, uploadId));
  }
}
