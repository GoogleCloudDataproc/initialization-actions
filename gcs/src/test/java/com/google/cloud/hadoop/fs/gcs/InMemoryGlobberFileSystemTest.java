/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import java.io.FileNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link InMemoryGlobberFileSystem} class. */
@RunWith(JUnit4.class)
public class InMemoryGlobberFileSystemTest {

  @Test
  public void testGetFileStatus_shouldReturnExceptionForNotExistingFile() throws Exception {
    GoogleHadoopFileSystem ghfs = new InMemoryGoogleHadoopFileSystem();

    FileSystem helperFileSystem =
        InMemoryGlobberFileSystem.createInstance(
            new Configuration(), ghfs.getWorkingDirectory(), ImmutableSet.of());

    FileNotFoundException e =
        assertThrows(
            FileNotFoundException.class,
            () -> helperFileSystem.getFileStatus(ghfs.getWorkingDirectory()));

    assertThat(e)
        .hasMessageThat()
        .startsWith(
            String.format(
                "Path '%s' (qualified: '%s') does not exist.",
                ghfs.getWorkingDirectory(), ghfs.getWorkingDirectory()));
  }
}
