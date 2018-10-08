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

import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
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
  }

  @Override
  public String getScheme() {
    return "gs";
  }
}
