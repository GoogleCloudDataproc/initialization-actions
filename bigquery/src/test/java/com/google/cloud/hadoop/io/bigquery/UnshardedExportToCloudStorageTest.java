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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UnshardedExportToCloudStorageTest {

  @Test
  public void testGetSplits() throws IOException, InterruptedException {
    Configuration conf = new Configuration();

    UnshardedExportToCloudStorage export =
        new UnshardedExportToCloudStorage(
            conf,
            "path",
            ExportFileFormat.AVRO,
            new BigQueryHelper(null),
            "project-id",
            null, /* table */
            new InputFormat<LongWritable, Text>() {
              @Override
              public List<InputSplit> getSplits(JobContext jobContext)
                  throws IOException, InterruptedException {
                return ImmutableList.<InputSplit>builder()
                    .add(new FileSplit(new Path("Foo"), 0L, 1L, new String[0]))
                    .add(new FileSplit(new Path("Bar"), 0L, 1L, new String[0]))
                    .build();
              }

              @Override
              public RecordReader<LongWritable, Text> createRecordReader(
                  InputSplit inputSplit,
                  TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                throw new UnsupportedOperationException("Not implemented.");
              }
            });

    List<InputSplit> splits = export.getSplits(null);
    UnshardedInputSplit fooSplit = (UnshardedInputSplit) splits.get(0);
    Assert.assertEquals("Foo", fooSplit.getPath().getName());

    UnshardedInputSplit barSplit = (UnshardedInputSplit) splits.get(1);
    Assert.assertEquals("Bar", barSplit.getPath().getName());
  }
}
