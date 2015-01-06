package com.google.cloud.hadoop.io.bigquery;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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
    assertEquals(LENGTH, bqInputSplit.getLength());
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
    assertEquals(START, bqInputSplit.getLocations().length);
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
    assertEquals(PATH, bqInputSplit.getPath());
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
    assertEquals(inputSplit.toString(), bqInputSplit.toString());
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
    assertEquals(LENGTH, bqInputSplit.getLength());
    assertEquals(START, bqInputSplit.getStart());
    assertEquals(PATH, bqInputSplit.getPath());
  }
}
