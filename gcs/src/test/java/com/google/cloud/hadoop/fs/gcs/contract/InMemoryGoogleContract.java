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

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemTestHelper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * Contract of GoogleHadoopFileSystem via scheme "gs" based on an in-memory fake of the GCS
 * service.
 */
public class InMemoryGoogleContract extends AbstractFSContract {
  public static final String CONTRACT_XML = "contract/gs.xml";

  // This bucket comes from our helper method which produces an in-memory fake for the test to use.
  public static final String FAKE_TEST_PATH = "gs://fake-test-system-bucket/test/";

  private FileSystem fs;

  public InMemoryGoogleContract(Configuration conf) {
    super(conf);
    // Always enable these in-memory unittest versions of the contract tests.
    conf.set("fs.contract.test.fs.gs", FAKE_TEST_PATH);
    addConfResource(CONTRACT_XML);
  }

  @Override
  public void init() throws IOException {
    super.init();
    fs = GoogleHadoopFileSystemTestHelper.createInMemoryGoogleHadoopFileSystem();
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return fs;
  }

  @Override
  public String getScheme() {
    return "gs";
  }

  @Override
  public Path getTestPath() {
    return new Path(FAKE_TEST_PATH);
  }
}
