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

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.AfterClass;
import org.junit.Before;

/** GCS contract tests covering file rename. */
public class TestGoogleContractRename extends AbstractContractRenameTest {

  private static final TestBucketHelper TEST_BUCKET_HELPER =
      new TestBucketHelper(GoogleContract.TEST_BUCKET_NAME_PREFIX);

  private static final AtomicReference<GoogleHadoopFileSystem> fs = new AtomicReference<>();

  @Before
  public void before() {
    fs.compareAndSet(null, (GoogleHadoopFileSystem) getFileSystem());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    TEST_BUCKET_HELPER.cleanup(fs.get().getGcsFs().getGcs());
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new GoogleContract(conf, TEST_BUCKET_HELPER);
  }
}
