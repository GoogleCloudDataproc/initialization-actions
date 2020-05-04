/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs.contract;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX;
import static com.google.cloud.hadoop.util.HadoopCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX;

import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

/** Contract of GoogleHadoopFileSystem via scheme "gs". */
public class GoogleContract extends AbstractBondedFSContract {

  public static final String TEST_BUCKET_NAME_PREFIX = "ghfs-contract-test";

  private static final String CONTRACT_XML = "contract/gs.xml";

  public GoogleContract(Configuration conf, TestBucketHelper bucketHelper) {
    super(conf);
    addConfResource(CONTRACT_XML);
    conf.set("fs.contract.test.fs.gs", "gs://" + bucketHelper.getUniqueBucketPrefix());
    // TODO: remove when GCS fixes non-retriable "410 Gone" responses
    conf.setInt("fs.gs.outputstream.upload.cache.size", 1024 * 1024);

    TestConfiguration testConf = TestConfiguration.getInstance();
    if (testConf.getProjectId() != null) {
      conf.set(GCS_PROJECT_ID.getKey(), testConf.getProjectId());
    }
    if (testConf.getServiceAccount() != null && testConf.getPrivateKeyFile() != null) {
      conf.setBoolean(GCS_CONFIG_PREFIX + ENABLE_SERVICE_ACCOUNTS_SUFFIX.getKey(), true);
      conf.set(
          GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_EMAIL_SUFFIX.getKey(), testConf.getServiceAccount());
      conf.set(
          GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_KEYFILE_SUFFIX.getKey(),
          testConf.getPrivateKeyFile());
    }
  }

  @Override
  public String getScheme() {
    return "gs";
  }
}
