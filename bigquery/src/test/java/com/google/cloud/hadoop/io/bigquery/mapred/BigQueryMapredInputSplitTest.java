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
package com.google.cloud.hadoop.io.bigquery.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.io.bigquery.UnshardedInputSplit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link BigQueryMapredInputSplit}.
 */
@RunWith(JUnit4.class)
public class BigQueryMapredInputSplitTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Mock private UnshardedInputSplit mockInputSplit;

  @Before public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test public void testWriteAndReadFields()
      throws IOException, InterruptedException {
    Path path = new Path("testing123");
    UnshardedInputSplit mapreduceInputSplit =
        new UnshardedInputSplit(path, 2000, 1000, new String[0]);
    BigQueryMapredInputSplit inputSplit =
        new BigQueryMapredInputSplit(mapreduceInputSplit);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    inputSplit.write(out);
    out.close();
    byte[] serializedData = baos.toByteArray();

    ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
    DataInputStream in = new DataInputStream(bais);
    BigQueryMapredInputSplit newInputSplit =
        new BigQueryMapredInputSplit();
    newInputSplit.readFields(in);
    in.close();

    org.apache.hadoop.mapreduce.InputSplit newMapreduceInputSplit =
        newInputSplit.getMapreduceInputSplit();
    assertEquals(mapreduceInputSplit.getClass().getName(),
        newMapreduceInputSplit.getClass().getName());
    assertTrue(newMapreduceInputSplit instanceof UnshardedInputSplit);
    UnshardedInputSplit bqInputSplit =
        (UnshardedInputSplit) newMapreduceInputSplit;
    assertEquals(path, bqInputSplit.getPath());
  }

  @Test public void testGetLength()
      throws IOException, InterruptedException {
    BigQueryMapredInputSplit inputSplit =
        new BigQueryMapredInputSplit(mockInputSplit);

    when(mockInputSplit.getLength()).thenReturn(135L);
    long length = inputSplit.getLength();
    assertEquals(135L, length);
  }

  @Test public void testGetLocations()
      throws IOException, InterruptedException {
    BigQueryMapredInputSplit inputSplit =
        new BigQueryMapredInputSplit(mockInputSplit);

    when(mockInputSplit.getLocations()).
        thenReturn(new String[] { "g", "o", "o", "g" });
    String[] locations = inputSplit.getLocations();
    assertEquals(new String[] {"g", "o", "o", "g"}, locations);
  }
}
