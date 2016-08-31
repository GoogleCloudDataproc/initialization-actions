package com.google.cloud.hadoop.io.bigquery;

import static com.google.cloud.hadoop.io.bigquery.ExportFileFormat.AVRO;
import static org.junit.Assert.assertEquals;

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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NoopFederatedExportToCloudStorageTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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
    assertEquals("gs://foo-bucket/bar-dir/glob-*.avro,gs://foo-bucket/bar-dir/file.avro", result);
  }

  @Test
  public void testValidateGcsPaths() throws Exception {
    table.getExternalDataConfiguration().setSourceUris(ImmutableList.of(
        "https://drive.google.com/open?id=1234"));
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Invalid GCS resource: 'https://drive.google.com/open?id=1234'");
    new NoopFederatedExportToCloudStorage(conf, AVRO, helper, projectId, table, null);
  }

  @Test
  public void testFormatMismatch() throws Exception {
    table.getExternalDataConfiguration().setSourceFormat("CSV");
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "MapReduce fileFormat 'AVRO' does not match BigQuery sourceFormat 'CSV'. "
            + "Use the appropriate InputFormat");
    new NoopFederatedExportToCloudStorage(conf, AVRO, helper, projectId, table, null);
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
    assertEquals(
        "gs://foo-bucket/bar-dir/glob-*.avro,gs://foo-bucket/bar-dir/file.avro",
        conf.get("mapred.input.dir"));

    UnshardedInputSplit split1 = (UnshardedInputSplit) splits.get(0);
    assertEquals("gs://foo-bucket/bar-dir/glob-1.avro", split1.getPath().toString());
    UnshardedInputSplit split2 = (UnshardedInputSplit) splits.get(1);
    assertEquals("gs://foo-bucket/bar-dir/glob-2.avro", split2.getPath().toString());
    UnshardedInputSplit split3 = (UnshardedInputSplit) splits.get(2);
    assertEquals("gs://foo-bucket/bar-dir/file.avro", split3.getPath().toString());
  }
}
