/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CreateFileOptionsTest {
  @Test
  public void testConstructorChecksForContentTypeAttributes() throws Exception {
    new CreateFileOptions(true, "", ImmutableMap.<String, byte[]>of("Innocuous", "".getBytes()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new CreateFileOptions(
                true, "", ImmutableMap.<String, byte[]>of("Content-Type", "".getBytes(UTF_8))));
  }
}
