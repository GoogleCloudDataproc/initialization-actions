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

import static com.google.common.truth.Truth.assertThat;

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
    assertThat(split1.getLocations()).isNotNull();
    assertThat(split1.getLocations()).isEmpty();
  }

  @Test
  public void testBasicFields()
      throws IOException {
    assertThat(split1.getShardDirectoryAndPattern()).isEqualTo(shardPath1);
    assertThat(split1.getLength()).isEqualTo(numRecords1);
  }

  @Test
  public void testToString()
      throws IOException {
    assertThat(split1.toString()).contains(shardPath1.toString());
    assertThat(split1.toString()).contains(Long.toString(numRecords1));
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
    assertThat(recoveredSplit.getShardDirectoryAndPattern()).isEqualTo(shardPath1);
    assertThat(recoveredSplit.getLength()).isEqualTo(numRecords1);
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
    assertThat(recoveredSplit.getShardDirectoryAndPattern()).isEqualTo(shardPath1);
    assertThat(recoveredSplit.getLength()).isEqualTo(numRecords1);

    // Same InputSplit can be reused to read a second deserialization.
    recoveredSplit.readFields(dataIn);
    assertThat(recoveredSplit.getShardDirectoryAndPattern()).isEqualTo(shardPath2);
    assertThat(recoveredSplit.getLength()).isEqualTo(numRecords2);
  }
}
