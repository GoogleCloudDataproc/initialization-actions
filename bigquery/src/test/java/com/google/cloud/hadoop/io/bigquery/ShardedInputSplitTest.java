/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for ShardedInputSplit.
 */
@RunWith(JUnit4.class)
public class ShardedInputSplitTest {
  private Path shardPath1;
  private Path shardPath2;

  private long numRecords1;
  private long numRecords2;
  
  private ShardedInputSplit split1;
  private ShardedInputSplit split2;

  @Before
  public void setUp() {
    shardPath1 = new Path("gs://foo-bucket/shard0/data-*.json");
    shardPath2 = new Path("gs://bar-bucket/shard1/part-*.csv");
    numRecords1 = 123;
    numRecords2 = 456;
    split1 = new ShardedInputSplit(shardPath1, numRecords1);
    split2 = new ShardedInputSplit(shardPath2, numRecords2);
  }

  @Test
  public void testGetLocations()
      throws IOException {
    // No notion of locations for now; return empty but non-null array.
    assertNotNull(split1.getLocations());
    assertEquals(0, split1.getLocations().length);
  }

  @Test
  public void testBasicFields()
      throws IOException {
    assertEquals(shardPath1, split1.getShardDirectoryAndPattern());
    assertEquals(numRecords1, split1.getLength());
  }

  @Test
  public void testToString()
      throws IOException {
    assertTrue(split1.toString().contains(shardPath1.toString()));
    assertTrue(split1.toString().contains(Long.toString(numRecords1)));
  }

  @Test
  public void testSerializeAndDeserializeSingle()
      throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);

    split1.write(dataOut);

    dataOut.close();

    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
    DataInputStream dataIn = new DataInputStream(byteIn);

    ShardedInputSplit recoveredSplit = new ShardedInputSplit();
    recoveredSplit.readFields(dataIn);
    assertEquals(shardPath1, recoveredSplit.getShardDirectoryAndPattern());
    assertEquals(numRecords1, recoveredSplit.getLength());
  }

  @Test
  public void testSerializeAndDeserializeMultiple()
      throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(byteOut);

    split1.write(dataOut);
    split2.write(dataOut);

    dataOut.close();

    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
    DataInputStream dataIn = new DataInputStream(byteIn);

    ShardedInputSplit recoveredSplit = new ShardedInputSplit();
    recoveredSplit.readFields(dataIn);
    assertEquals(shardPath1, recoveredSplit.getShardDirectoryAndPattern());
    assertEquals(numRecords1, recoveredSplit.getLength());

    // Same InputSplit can be reused to read a second deserialization.
    recoveredSplit.readFields(dataIn);
    assertEquals(shardPath2, recoveredSplit.getShardDirectoryAndPattern());
    assertEquals(numRecords2, recoveredSplit.getLength());
  }
}
