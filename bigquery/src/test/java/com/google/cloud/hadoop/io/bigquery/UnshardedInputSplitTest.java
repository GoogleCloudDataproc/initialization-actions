/*
 * Copyright 2017 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.io.bigquery;

import static com.google.common.truth.Truth.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit Tests for UnshardedInputSplit.
 */
@RunWith(JUnit4.class)
public class UnshardedInputSplitTest {
  // Sample start value for testing.
  private static final int START = 0;

  // Sample length value for testing.
  private static final int LENGTH = 60;

  // Sample path value for testing.
  private static final Path PATH = new Path("test/path");

  /**
   * Tests the getLength method.
   */
  @Test
  public void testGetLength() throws IOException, InterruptedException {
    // Create a new InputSplit containing the values.
    UnshardedInputSplit bqInputSplit =
        new UnshardedInputSplit(PATH, START, START + LENGTH, new String[0]);

    // Test for correct construction
    assertThat(bqInputSplit.getLength()).isEqualTo(LENGTH);
  }

  /**
   * Tests the getLocations method.
   */
  @Test
  public void testGetLocations() 
      throws IOException, InterruptedException {
    // Create a new InputSplit containing the values.
    UnshardedInputSplit bqInputSplit =
        new UnshardedInputSplit(PATH, START, START + LENGTH, new String[0]);

    // Test for correct construction
    assertThat(bqInputSplit.getLocations()).hasLength(START);
  }

  /**
   * Tests the getLocations method.
   */
  @Test
  public void testGetPath() {
    // Create a new InputSplit containing the values.
    UnshardedInputSplit bqInputSplit =
        new UnshardedInputSplit(PATH, START, START + LENGTH, new String[0]);

    // Test for correct construction
    assertThat(bqInputSplit.getPath()).isEqualTo(PATH);
  }

  /**
   * Tests the toString method.
   */
  @Test
  public void testToString() {
    // Create a new InputSplit containing the values.
    FileSplit inputSplit = new FileSplit(PATH, START, START + LENGTH, new String[0]);
    UnshardedInputSplit bqInputSplit = new UnshardedInputSplit(
        PATH, START, START + LENGTH, new String[0]);

    // Test for correct construction
    assertThat(bqInputSplit.toString()).isEqualTo(inputSplit.toString());
  }

  /**
   * Tests the serialization methods.
   */
  @Test
  public void testReadWriteFields() 
      throws IOException, InterruptedException {
    // Create a new InputSplit containing the values.
    UnshardedInputSplit bqInputSplit =
        new UnshardedInputSplit(PATH, START, START + LENGTH, new String[0]);

    // Sample UnshardedInputSplit to read into for testing.
    UnshardedInputSplit bqResultSplit = new UnshardedInputSplit();

    // Construct a DataOutputStream
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    // Write to the DataOutputStream
    bqInputSplit.write(out);
    out.flush();
    byte[] data = baos.toByteArray();

    // Construct a DataInputStream
    DataInput in = new DataInputStream(new ByteArrayInputStream(data));

    // Read from DataInoutStream
    bqResultSplit.readFields(in);

    // Test for correct serialization
    assertThat(bqInputSplit.getLength()).isEqualTo(LENGTH);
    assertThat(bqInputSplit.getStart()).isEqualTo(START);
    assertThat(bqInputSplit.getPath()).isEqualTo(PATH);
  }
}
