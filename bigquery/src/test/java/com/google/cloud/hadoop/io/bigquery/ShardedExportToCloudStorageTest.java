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

import static com.google.cloud.hadoop.io.bigquery.ShardedExportToCloudStorage.NUM_MAP_TASKS_HINT_KEY;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ShardedExportToCloudStorageTest {

  @Test
  public void testGetSplits() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set(NUM_MAP_TASKS_HINT_KEY, "100");

    ShardedExportToCloudStorage export =
        new ShardedExportToCloudStorage(
            conf,
            "gs://bucket/path",
            ExportFileFormat.AVRO,
            new BigQueryHelper(null),
            "project-id",
            new Table()
                .setTableReference(
                    new TableReference().setProjectId("p").setDatasetId("d").setTableId("t"))
                .setNumBytes(200 * 1000 * 1000 * 1000L)
                .setNumRows(BigInteger.valueOf(100 * 1000)));

    List<InputSplit> splits = export.getSplits(null);
    assertThat(splits).hasSize(100);
    assertThat(splits.get(0).getLength()).isEqualTo(1000);
    assertThat(((ShardedInputSplit) splits.get(0)).getShardDirectoryAndPattern())
        .isEqualTo(new Path("gs://bucket/path/shard-0/data-*.avro"));
  }

  /** Test that there are always at least 2 maps. */
  @Test
  public void testGetSplitsTiny() throws IOException {
    Configuration conf = new Configuration();
    conf.set(NUM_MAP_TASKS_HINT_KEY, "1");

    ShardedExportToCloudStorage export =
        new ShardedExportToCloudStorage(
            conf,
            "gs://bucket/path",
            ExportFileFormat.AVRO,
            new BigQueryHelper(null),
            "project-id",
            new Table()
                .setTableReference(
                    new TableReference().setProjectId("p").setDatasetId("d").setTableId("t"))
                .setNumBytes(1L)
                .setNumRows(BigInteger.ONE));

    List<InputSplit> splits = export.getSplits(null);
    assertThat(splits).hasSize(2);
  }
}
