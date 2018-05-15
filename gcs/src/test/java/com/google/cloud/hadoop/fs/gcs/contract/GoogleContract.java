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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

/**
 * Contract of GoogleHadoopFileSystem via scheme "gs".
 */
public class GoogleContract extends AbstractBondedFSContract {
  public static final String CONTRACT_XML = "contract/gs.xml";

  public GoogleContract(Configuration conf) {
    super(conf);
    addConfResource(CONTRACT_XML);
  }

  @Override
  public String getScheme() {
    return "gs";
  }

  @Override
  public Path getTestPath() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    return testUniqueForkId == null
        ? super.getTestPath()
        : new Path("/" + testUniqueForkId, "test");
  }
}
