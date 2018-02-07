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

import static com.google.cloud.hadoop.io.bigquery.ExportFileFormat.AVRO;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.expectThrows;

import com.google.api.services.bigquery.model.ExternalDataConfiguration;
import com.google.api.services.bigquery.model.Table;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NoopFederatedExportToCloudStorageTest {
  private Table table;
  private Configuration conf;
  private final BigQueryHelper helper = new BigQueryHelper(null);
  private final String projectId = "project-id";

  @Before
  public void setup() {
    conf = new Configuration();
    table = new Table().setExternalDataConfiguration(
        new ExternalDataConfiguration()
            .setSourceFormat("AVRO")
            .setSourceUris(
                ImmutableList.of(
                    "gs://foo-bucket/bar-dir/glob-*.avro",
                    "gs://foo-bucket/bar-dir/file.avro")));
  }

  @Test
  public void testGetCommaSeparatedGcsPathList() throws Exception {
    String result = NoopFederatedExportToCloudStorage.getCommaSeparatedGcsPathList(table);
    assertThat(result)
        .isEqualTo("gs://foo-bucket/bar-dir/glob-*.avro,gs://foo-bucket/bar-dir/file.avro");
  }

  @Test
  public void testValidateGcsPaths() throws Exception {
    table.getExternalDataConfiguration().setSourceUris(ImmutableList.of(
        "https://drive.google.com/open?id=1234"));
    IllegalArgumentException thrown =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                new NoopFederatedExportToCloudStorage(conf, AVRO, helper, projectId, table, null));
    assertThat(thrown)
        .hasMessageThat()
        .contains("Invalid GCS resource: 'https://drive.google.com/open?id=1234'");
  }

  @Test
  public void testFormatMismatch() throws Exception {
    table.getExternalDataConfiguration().setSourceFormat("CSV");
    IllegalArgumentException thrown =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                new NoopFederatedExportToCloudStorage(conf, AVRO, helper, projectId, table, null));
    assertThat(thrown)
        .hasMessageThat()
        .contains(
            "MapReduce fileFormat 'AVRO' does not match BigQuery sourceFormat 'CSV'. "
                + "Use the appropriate InputFormat");
  }

  @Test
  public void testGetSplits() throws IOException, InterruptedException {
    Configuration conf = new Configuration();

    NoopFederatedExportToCloudStorage export =
        new NoopFederatedExportToCloudStorage(
            conf, AVRO, helper, projectId, table,
            new InputFormat<LongWritable, Text>() {
              @Override
              public List<InputSplit> getSplits(JobContext jobContext)
                  throws IOException, InterruptedException {
                return ImmutableList.<InputSplit>builder()
                    .add(new FileSplit(
                        new Path("gs://foo-bucket/bar-dir/glob-1.avro"), 0L, 1L, new String[0]))
                    .add(new FileSplit(
                        new Path("gs://foo-bucket/bar-dir/glob-2.avro"), 0L, 1L, new String[0]))
                    .add(new FileSplit(
                        new Path("gs://foo-bucket/bar-dir/file.avro"), 0L, 1L, new String[0]))
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
    // Verify configuration
    assertThat(conf.get("mapred.input.dir"))
        .isEqualTo("gs://foo-bucket/bar-dir/glob-*.avro,gs://foo-bucket/bar-dir/file.avro");

    UnshardedInputSplit split1 = (UnshardedInputSplit) splits.get(0);
    assertThat(split1.getPath().toString()).isEqualTo("gs://foo-bucket/bar-dir/glob-1.avro");
    UnshardedInputSplit split2 = (UnshardedInputSplit) splits.get(1);
    assertThat(split2.getPath().toString()).isEqualTo("gs://foo-bucket/bar-dir/glob-2.avro");
    UnshardedInputSplit split3 = (UnshardedInputSplit) splits.get(2);
    assertThat(split3.getPath().toString()).isEqualTo("gs://foo-bucket/bar-dir/file.avro");
  }
}
