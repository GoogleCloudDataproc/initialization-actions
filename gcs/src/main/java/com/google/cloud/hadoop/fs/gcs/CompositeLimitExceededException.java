/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs;

import java.io.IOException;

/**
 * Throw when an attempted operation is prohibited because it would require exceeding the GCS
 * component limit. Generally, methods which throw this exception should fail-fast and preserve
 * correctness of internal state so that other methods which don't entail exceeding the
 * limit for composite component count can still be called safely.
 */
public class CompositeLimitExceededException extends IOException {
  public CompositeLimitExceededException() {
    super();
  }

  public CompositeLimitExceededException(String message) {
    super(message);
  }
}
