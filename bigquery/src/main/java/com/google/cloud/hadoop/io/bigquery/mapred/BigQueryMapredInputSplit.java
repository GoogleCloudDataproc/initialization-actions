package com.google.cloud.hadoop.io.bigquery.mapred;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wrap our {@link com.google.cloud.hadoop.io.bigquery.UnshardedInputSplit} class.
 */
public class BigQueryMapredInputSplit implements InputSplit {

  private org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit;
  private Writable writableInputSplit;

  @VisibleForTesting
  BigQueryMapredInputSplit() {
    // Used by Hadoop serialization via reflection.
  }

  /**
   * @param mapreduceInputSplit An InputSplit that also
   *        implements Writable.
   */
  public BigQueryMapredInputSplit(
      org.apache.hadoop.mapreduce.InputSplit mapreduceInputSplit) {
    Preconditions.checkArgument(
        mapreduceInputSplit instanceof Writable,
        "inputSplit must also be Writable");
    this.mapreduceInputSplit = mapreduceInputSplit;
    writableInputSplit = (Writable) mapreduceInputSplit;
  }

  public long getLength() throws IOException {
    try {
      return mapreduceInputSplit.getLength();
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
  }

  public String[] getLocations() throws IOException {
    try {
      return mapreduceInputSplit.getLocations();
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
  }

  public void readFields(DataInput in) throws IOException {
    String splitClassName = in.readUTF();
    Class<? extends org.apache.hadoop.mapreduce.InputSplit> splitClass = null;
    try {
      splitClass = Class.forName(splitClassName).asSubclass(
              org.apache.hadoop.mapreduce.InputSplit.class);
    } catch (ClassNotFoundException ex) {
      throw new IOException("No such InputSplit class " + splitClassName, ex);
    } catch (ClassCastException ex) {
      throw new IOException("Expected subclass of InputSplit but got "
          + splitClassName, ex);
    }
    try {
      mapreduceInputSplit = splitClass.newInstance();
      writableInputSplit = (Writable) mapreduceInputSplit;
      writableInputSplit.readFields(in);
    } catch (InstantiationException | IllegalAccessException ex) {
      throw new IOException("Can't instantiate InputSplit", ex);
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(mapreduceInputSplit.getClass().getName());
    writableInputSplit.write(out);
  }

  public org.apache.hadoop.mapreduce.InputSplit getMapreduceInputSplit() {
    return mapreduceInputSplit;
  }
}
