/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.gcsio.testing.TestConfiguration.GCS_TEST_PRIVATE_KEYFILE;
import static com.google.cloud.hadoop.gcsio.testing.TestConfiguration.GCS_TEST_PROJECT_ID;
import static com.google.cloud.hadoop.gcsio.testing.TestConfiguration.GCS_TEST_SERVICE_ACCOUNT;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

public final class GoogleHadoopFileSystemIntegrationHelper {

  private static final String ENV_VAR_MSG_FMT = "Environment variable %s should be set";

  public static final String APP_NAME = "GHFS-test";

  public static GoogleHadoopFileSystem createGhfs(String path, Configuration config)
      throws Exception {
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(new URI(path), config);
    return ghfs;
  }

  /**
   * Helper to load all the GHFS-specific config values from environment variables, such as those
   * needed for setting up the credentials of a real GoogleCloudStorage.
   */
  public static Configuration getTestConfig() {
    TestConfiguration testConfiguration = TestConfiguration.getInstance();
    String projectId =
        checkNotNull(testConfiguration.getProjectId(), ENV_VAR_MSG_FMT, GCS_TEST_PROJECT_ID);
    String privateKeyFile =
        checkNotNull(
            testConfiguration.getPrivateKeyFile(), ENV_VAR_MSG_FMT, GCS_TEST_PRIVATE_KEYFILE);
    String serviceAccount =
        checkNotNull(
            testConfiguration.getServiceAccount(), ENV_VAR_MSG_FMT, GCS_TEST_SERVICE_ACCOUNT);

    Configuration config = new Configuration();
    config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), projectId);
    config.set(
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_EMAIL.getKey(), serviceAccount);
    config.set(
        GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_KEY_FILE.getKey(), privateKeyFile);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_REPAIR_IMPLICIT_DIRECTORIES_ENABLE.getKey(), true);
    config.setBoolean(
        GoogleHadoopFileSystemConfiguration.GCS_INFER_IMPLICIT_DIRECTORIES_ENABLE.getKey(), false);
    // Allow buckets to be deleted in test cleanup:
    config.setBoolean(GoogleHadoopFileSystemConfiguration.GCE_BUCKET_DELETE_ENABLE.getKey(), true);
    return config;
  }

  private GoogleHadoopFileSystemIntegrationHelper() {}
}
