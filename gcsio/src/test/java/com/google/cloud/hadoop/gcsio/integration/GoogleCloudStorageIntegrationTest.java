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

import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.ThrottledGoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.ThrottledGoogleCloudStorage.StorageOperation;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class GoogleCloudStorageIntegrationTest extends GoogleCloudStorageTest {

  @Parameters
  public static Collection<Object[]> getConstructorArguments() throws IOException {
    GoogleCloudStorage gcs = getGoogleCloudStorage();
    GoogleCloudStorage apacheTransportGcs = getApacheGoogleCloudStorage();

    return Arrays.asList(new Object[][]{
        {gcs},
        {apacheTransportGcs},
    });
  }

  private static GoogleCloudStorage getApacheGoogleCloudStorage() throws IOException {
    GoogleCloudStorageOptions.Builder builder = GoogleCloudStorageTestHelper
        .getStandardOptionBuilder();
    builder.setTransportType(HttpTransportFactory.HttpTransportType.APACHE);
    return getGoogleCloudStorage(Optional.of(builder));
  }

  public GoogleCloudStorageIntegrationTest(GoogleCloudStorage gcs) {
    super(gcs);
  }

  private static GoogleCloudStorage getGoogleCloudStorage() throws IOException {
    return getGoogleCloudStorage(Optional.<GoogleCloudStorageOptions.Builder>absent());
  }
  private static GoogleCloudStorage getGoogleCloudStorage(
      Optional<GoogleCloudStorageOptions.Builder> optionalBuilder) throws IOException {
    Credential credential = GoogleCloudStorageTestHelper.getCredential();

    GoogleCloudStorageOptions.Builder builder =
        optionalBuilder.or(GoogleCloudStorageTestHelper.getStandardOptionBuilder());

    return new ThrottledGoogleCloudStorage(
        // Allow 2 create or delete bucket operation every second. This will hit rate limits,
        // but GCS now has back-offs implemented for bucket operations.
        RateLimiter.create(2),
        new GoogleCloudStorageImpl(builder.build(), credential),
        EnumSet.of(StorageOperation.DELETE_BUCKETS, StorageOperation.CREATE_BUCKET));
  }
}
