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
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ExternalDataConfiguration;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.fs.gcs.InMemoryGoogleHadoopFileSystem;
import com.google.cloud.hadoop.testing.CredentialConfigurationUtil;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for GsonBigQueryInputFormat.
 */
@RunWith(JUnit4.class)
public class GsonBigQueryInputFormatTest {

  // Sample text values for tests.
  private Text value1 = new Text("{'title':'Test1','value':'test_1'}");
  private Text value2 = new Text("{'title':'Test2','value':'test_2'}");

  // GoogleHadoopFileSystem to use.
  private InMemoryGoogleHadoopFileSystem ghfs;

  // Hadoop job configuration.
  private Configuration config;

  // Sample projectIds for testing; one for owning the BigQuery jobs, another for the
  // TableReference.
  private String jobProjectId = "google.com:foo-project";
  private String dataProjectId = "publicdata";
  private String intermediateDataset = "test_dataset";
  private String intermediateTable = "test_table";

  // Misc mocks for Bigquery auto-generated API objects.
  @Mock private Bigquery mockBigquery;
  @Mock private Bigquery.Jobs mockBigqueryJobs;
  @Mock private Bigquery.Jobs.Get mockBigqueryJobsGet;
  @Mock private Bigquery.Jobs.Insert mockBigqueryJobsInsert;
  @Mock private Bigquery.Tables mockBigqueryTables;
  @Mock private Bigquery.Tables.Get mockBigqueryTablesGet;
  @Mock private Bigquery.Tables.Delete mockBigqueryTablesDelete;
  @Mock private InputFormat<LongWritable, Text> mockInputFormat;
  @Mock private TaskAttemptContext mockTaskAttemptContext;
  @Mock private BigQueryHelper mockBigQueryHelper;

  // JobStatus to return for testing.
  private JobStatus jobStatus;

  // Bigquery Job result to return for testing.
  private Job jobHandle;

  // Sample TableReference for BigQuery.
  private TableReference tableRef;

  private Table table;

  /**
   * Creates an in-memory GHFS.
   *
   * @throws IOException on IOError.
   */
  @Before
  public void setUp()
      throws IOException {
    MockitoAnnotations.initMocks(this);
    Logger.getLogger(GsonBigQueryInputFormat.class).setLevel(Level.DEBUG);

    // Set the Hadoop job configuration.
    config = InMemoryGoogleHadoopFileSystem.getSampleConfiguration();
    config.set(BigQueryConfiguration.PROJECT_ID_KEY, jobProjectId);
    config.set(BigQueryConfiguration.INPUT_PROJECT_ID_KEY, dataProjectId);
    config.set(BigQueryConfiguration.INPUT_DATASET_ID_KEY, intermediateDataset);
    config.set(BigQueryConfiguration.INPUT_TABLE_ID_KEY, intermediateTable);
    config.set(BigQueryConfiguration.INPUT_QUERY_KEY, "test_query");
    config.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, "gs://test_bucket/other_path");
    config.set(
        AbstractBigQueryInputFormat.INPUT_FORMAT_CLASS_KEY,
        GsonBigQueryInputFormat.class.getCanonicalName());
    config.setBoolean(BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, true);
    config.setBoolean(BigQueryConfiguration.DELETE_EXPORT_FILES_FROM_GCS_KEY, true);
    config.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, false);

    CredentialConfigurationUtil.addTestConfigurationSettings(config);

    // Create a GoogleHadoopFileSystem to use to initialize and write to
    // the in-memory GcsFs.
    ghfs = new InMemoryGoogleHadoopFileSystem();

    JobReference fakeJobReference = new JobReference();
    fakeJobReference.setProjectId(jobProjectId);
    fakeJobReference.setJobId("bigquery-job-1234");

    // Create the job result.
    jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);

    jobHandle = new Job();
    jobHandle.setStatus(jobStatus);
    jobHandle.setJobReference(fakeJobReference);

    // Create table reference.
    tableRef = new TableReference();
    tableRef.setProjectId(dataProjectId);
    tableRef.setDatasetId("test_dataset");
    tableRef.setTableId("test_table");

    table = new Table().setTableReference(tableRef).setLocation("test_location");

    when(mockBigQueryHelper.getRawBigquery())
        .thenReturn(mockBigquery);

    // Mocks for Bigquery jobs.
    when(mockBigquery.jobs())
        .thenReturn(mockBigqueryJobs);

    // Mock getting Bigquery job.
    when(mockBigqueryJobs.get(any(String.class), any(String.class)))
        .thenReturn(mockBigqueryJobsGet);
    when(mockBigqueryJobsGet.execute())
        .thenReturn(jobHandle);

    // Mock inserting Bigquery job.
    when(mockBigqueryJobs.insert(any(String.class), any(Job.class)))
        .thenReturn(mockBigqueryJobsInsert);
    when(mockBigqueryJobsInsert.execute())
        .thenReturn(jobHandle);

    // Mocks for Bigquery tables.
    when(mockBigquery.tables())
        .thenReturn(mockBigqueryTables);

    // Mocks for getting Bigquery table.
    when(mockBigqueryTables.get(any(String.class), any(String.class), any(String.class)))
        .thenReturn(mockBigqueryTablesGet);
    when(mockBigqueryTablesGet.execute())
        .thenReturn(table);

    when(mockBigQueryHelper.getTable(any(TableReference.class)))
        .thenReturn(table);

    when(mockBigQueryHelper.createJobReference(
            any(String.class), any(String.class), any(String.class)))
        .thenReturn(fakeJobReference);
    when(mockBigQueryHelper.insertJobOrFetchDuplicate(any(String.class), any(Job.class)))
        .thenReturn(jobHandle);
  }

  @After
  public void tearDown()
      throws IOException {
    Path tmpPath = new Path(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
    tmpPath.getFileSystem(config).delete(tmpPath, true);
    verifyNoMoreInteractions(mockBigQueryHelper);
  }

  /**
   * Tests createRecordReader method of GsonBigQueryInputFormat.
   */
  @Test
  public void testCreateRecordReader()
      throws IOException, InterruptedException {

    when(mockTaskAttemptContext.getConfiguration()).thenReturn(config);
    when(mockTaskAttemptContext.getJobID()).thenReturn(new JobID());

    // Write values to file.
    ByteBuffer buffer = GsonRecordReaderTest.stringToBytebuffer(value1 + "\n" + value2 + "\n");
    Path mockPath = new Path("gs://test_bucket/path/test");
    GsonRecordReaderTest.writeFile(ghfs, mockPath, buffer);

    // Create a new InputSplit containing the values.
    UnshardedInputSplit bqInputSplit = new UnshardedInputSplit(mockPath, 0, 60, new String[0]);

    // Construct GsonBigQueryInputFormat and call createBigQueryRecordReader.
    GsonBigQueryInputFormat gsonBigQueryInputFormat = new GsonBigQueryInputFormat();
    GsonRecordReader recordReader =
        (GsonRecordReader) gsonBigQueryInputFormat.createRecordReader(bqInputSplit, config);
    recordReader.initialize(bqInputSplit, mockTaskAttemptContext);

    // Verify BigQueryRecordReader set as expected.
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.nextKeyValue()).isTrue();
    assertThat(recordReader.nextKeyValue()).isFalse();
  }

  /**
   * Tests runQuery method of GsonBigQueryInputFormat.
   */
  @Test
  public void testRunQuery()
      throws IOException, InterruptedException {
    // Run runQuery method.
    QueryBasedExport.runQuery(mockBigQueryHelper, jobProjectId, tableRef, "test");

    // Verify correct calls to BigQuery are made.
    verify(mockBigQueryHelper).getTable(any(TableReference.class));
    verify(mockBigQueryHelper, times(1))
        .createJobReference(eq(jobProjectId), any(String.class), eq("test_location"));
    verify(mockBigQueryHelper, times(1))
        .createJobReference(eq(jobProjectId), any(String.class), eq("test_location"));
    verify(mockBigQueryHelper, times(1))
        .insertJobOrFetchDuplicate(eq(jobProjectId), any(Job.class));
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
  }

  /**
   * Tests getSplits method of GsonBigQueryInputFormat.
   */
  @Test
  public void testGetSplitsSharded()
      throws IOException, InterruptedException {
    config.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, true);

    // Make the bytes large enough that we will estimate a large number of shards.
    table.setNumRows(BigInteger.valueOf(99999L))
        .setNumBytes(1024L * 1024 * 1024 * 8);

    // If the hinted map.tasks is smaller than the estimated number of files, then we defer
    // to the hint.
    config.setInt(ShardedExportToCloudStorage.NUM_MAP_TASKS_HINT_KEY, 3);

    // Run getSplits method.
    GsonBigQueryInputFormat gsonBigQueryInputFormat = new GsonBigQueryInputFormatForTest();
    BigQueryJobWrapper wrapper = new BigQueryJobWrapper(config);
    wrapper.setJobID(new JobID());
    List<InputSplit> splits = gsonBigQueryInputFormat.getSplits(wrapper);

    // The base export path should've gotten created.
    Path baseExportPath = new Path(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
    FileStatus baseStatus = baseExportPath.getFileSystem(config).getFileStatus(baseExportPath);
    assertThat(baseStatus.isDir()).isTrue();

    assertThat(splits).hasSize(3);
    for (int i = 0; i < 3; ++i) {
      assertThat(splits.get(i)).isInstanceOf(ShardedInputSplit.class);
      DynamicFileListRecordReader<LongWritable, Text> reader =
          new DynamicFileListRecordReader<>(new DelegateRecordReaderFactory<LongWritable, Text>() {
            @Override
            public RecordReader<LongWritable, Text> createDelegateRecordReader(
                InputSplit split, Configuration configuration)
                throws IOException, InterruptedException {
              return new LineRecordReader();
            }
          });
      when(mockTaskAttemptContext.getConfiguration()).thenReturn(config);
      reader.initialize(splits.get(i), mockTaskAttemptContext);
      Path shardDir = ((ShardedInputSplit) splits.get(i))
          .getShardDirectoryAndPattern()
          .getParent();
      FileStatus shardDirStatus = shardDir.getFileSystem(config).getFileStatus(shardDir);
      assertThat(shardDirStatus.isDir()).isTrue();
    }

    // Verify correct calls to BigQuery are made.
    verify(mockBigQueryHelper, times(2))
        .createJobReference(eq(jobProjectId), any(String.class), eq("test_location"));
    verify(mockBigQueryHelper, times(2))
        .insertJobOrFetchDuplicate(eq(jobProjectId), any(Job.class));

    // Make sure we didn't try to delete the table in sharded mode even though
    // DELETE_INTERMEDIATE_TABLE_KEY is true and we had a query.
    verify(mockBigQueryHelper, times(2)).getTable(eq(tableRef));
    verifyNoMoreInteractions(mockBigqueryTables);
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
  }

  @Test
  public void testGetSplitsShardedSmall()
      throws IOException, InterruptedException {
    config.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, true);

    // Make the bytes as small as possible.
    table.setNumRows(BigInteger.valueOf(2L))
        .setNumBytes(1L);

    // If the hinted map.tasks is smaller than the estimated number of files, then we defer
    // to the hint.
    config.setInt(ShardedExportToCloudStorage.NUM_MAP_TASKS_HINT_KEY, 3);

    // Run getSplits method.
    GsonBigQueryInputFormat gsonBigQueryInputFormat = new GsonBigQueryInputFormatForTest();
    BigQueryJobWrapper wrapper = new BigQueryJobWrapper(config);
    wrapper.setJobID(new JobID());
    List<InputSplit> splits = gsonBigQueryInputFormat.getSplits(wrapper);

    // The base export path should've gotten created.
    Path baseExportPath = new Path(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
    FileStatus baseStatus = baseExportPath.getFileSystem(config).getFileStatus(baseExportPath);
    assertThat(baseStatus.isDir()).isTrue();

    assertThat(splits).hasSize(2);
    for (int i = 0; i < 2; ++i) {
      assertThat(splits.get(i)).isInstanceOf(ShardedInputSplit.class);
    }

    // Verify correct calls to BigQuery are made.
    verify(mockBigQueryHelper, times(2))
        .createJobReference(eq(jobProjectId), any(String.class), eq("test_location"));
    verify(mockBigQueryHelper, times(2))
        .insertJobOrFetchDuplicate(eq(jobProjectId), any(Job.class));

    // Make sure we didn't try to delete the table in sharded mode even though
    // DELETE_INTERMEDIATE_TABLE_KEY is true and we had a query.
    verify(mockBigQueryHelper, times(2)).getTable(eq(tableRef));
    verifyNoMoreInteractions(mockBigqueryTables);
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
  }

  @Test
  public void testGetSplitsShardedBig()
      throws IOException, InterruptedException {
    config.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, true);

    // Make the bytes large enough that we will estimate a large number of shards
    table.setNumRows(BigInteger.valueOf(9999999L))
        .setNumBytes(1024L * 1024 * 1024 * 1024 * 8);

    // Set the hint to a value larger than the maximum number of shards allowed by the service.
    config.setInt(ShardedExportToCloudStorage.NUM_MAP_TASKS_HINT_KEY, 2400);
    config.setInt(ShardedExportToCloudStorage.MAX_EXPORT_SHARDS_KEY, 250);

    // Run getSplits method.
    GsonBigQueryInputFormat gsonBigQueryInputFormat = new GsonBigQueryInputFormatForTest();
    BigQueryJobWrapper wrapper = new BigQueryJobWrapper(config);
    wrapper.setJobID(new JobID());
    List<InputSplit> splits = gsonBigQueryInputFormat.getSplits(wrapper);

    // The base export path should've gotten created.
    Path baseExportPath = new Path(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
    FileStatus baseStatus = baseExportPath.getFileSystem(config).getFileStatus(baseExportPath);
    assertThat(baseStatus.isDir()).isTrue();

    assertThat(splits).hasSize(250);
    for (int i = 0; i < 2; ++i) {
      assertThat(splits.get(i)).isInstanceOf(ShardedInputSplit.class);
    }

    // Verify correct calls to BigQuery are made.
    verify(mockBigQueryHelper, times(2))
        .createJobReference(eq(jobProjectId), any(String.class), eq("test_location"));
    verify(mockBigQueryHelper, times(2))
        .insertJobOrFetchDuplicate(eq(jobProjectId), any(Job.class));
    verify(mockBigQueryHelper, times(2)).getTable(eq(tableRef));
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
  }

  /**
   * Tests getSplits method of GsonBigQueryInputFormat in unsharded-export mode.
   */
  @Test
  public void testGetSplitsUnshardedBlocking()
      throws IOException, InterruptedException {
    config.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, false);
    // Why are we still setting this??
    //config.unset(BigQueryConfiguration.INPUT_QUERY_KEY);

    BigQueryJobWrapper wrapper = new BigQueryJobWrapper(config);
    wrapper.setJobID(new JobID());

    when(mockInputFormat.getSplits(eq(wrapper)))
        .thenReturn(ImmutableList.<InputSplit>of(
            new FileSplit(new Path("file1"), 0, 100, new String[0])));
    GsonBigQueryInputFormat gsonBigQueryInputFormat = new GsonBigQueryInputFormatForTest();
    gsonBigQueryInputFormat.setDelegateInputFormat(mockInputFormat);

    // Run getSplits method.
    List<InputSplit> splits = gsonBigQueryInputFormat.getSplits(wrapper);

    // The base export path should've gotten created.
    Path baseExportPath = new Path(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
    FileStatus baseStatus = baseExportPath.getFileSystem(config).getFileStatus(baseExportPath);
    assertThat(baseStatus.isDir()).isTrue();

    assertThat(((FileSplit) splits.get(0)).getPath().getName()).isEqualTo("file1");
    assertThat(config.get("mapred.input.dir"))
        .isEqualTo(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));

    // Verify correct calls to BigQuery are made.
    verify(mockBigQueryHelper, times(2))
        .createJobReference(eq(jobProjectId), any(String.class), eq("test_location"));
    verify(mockBigQueryHelper, times(2))
        .insertJobOrFetchDuplicate(eq(jobProjectId), any(Job.class));
    verifyNoMoreInteractions(mockBigqueryTables);
    verify(mockBigQueryHelper, times(2)).getTable(eq(tableRef));
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
  }

  /**
   * Tests getSplits method of GsonBigQueryInputFormat with federated data.
   */
  @Test
  public void testGetSplitsFederated()
      throws IOException, InterruptedException {
    BigQueryJobWrapper wrapper = new BigQueryJobWrapper(config);
    wrapper.setJobID(new JobID());
    config.unset(BigQueryConfiguration.INPUT_QUERY_KEY);

    table.setType("EXTERNAL")
        .setExternalDataConfiguration(
            new ExternalDataConfiguration()
                .setSourceFormat("NEWLINE_DELIMITED_JSON")
                .setSourceUris(ImmutableList.of("gs://foo-bucket/bar.json")));

    FileSplit split = new FileSplit(new Path("gs://foo-bucket/bar.json"), 0, 100, new String[0]);
    when(mockInputFormat.getSplits(eq(wrapper))).thenReturn(ImmutableList.<InputSplit>of(split));

    GsonBigQueryInputFormat gsonBigQueryInputFormat = new GsonBigQueryInputFormatForTest();
    gsonBigQueryInputFormat.setDelegateInputFormat(mockInputFormat);

    // Run getSplits method.
    List<InputSplit> splits = gsonBigQueryInputFormat.getSplits(wrapper);

    assertThat(splits).hasSize(1);
    assertThat(((FileSplit) splits.get(0)).getPath()).isEqualTo(split.getPath());
    assertThat(config.get("mapred.input.dir")).isEqualTo("gs://foo-bucket/bar.json");
    verify(mockBigQueryHelper, times(1)).getTable(eq(tableRef));
    verifyNoMoreInteractions(mockBigquery);
  }

  /**
   * Tests getSplits method of GsonBigQueryInputFormat when Bigquery connection error is thrown.
   */
  @Test
  public void testGetSplitsSecurityException()
      throws IOException, InterruptedException {
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);

    // Write values to file.
    ByteBuffer buffer = GsonRecordReaderTest.stringToBytebuffer(value1 + "\n" + value2 + "\n");
    Path mockPath = new Path("gs://test_bucket/path/test");
    GsonRecordReaderTest.writeFile(ghfs, mockPath, buffer);

    // Run getSplits method.
    GsonBigQueryInputFormat gsonBigQueryInputFormat =
        new GsonBigQueryInputFormatForTestGeneralSecurityException();
    config.set("mapred.input.dir", "gs://test_bucket/path/test");

    BigQueryJobWrapper wrapper = new BigQueryJobWrapper(config);
    wrapper.setJobID(new JobID());

    assertThrows(IOException.class, () -> gsonBigQueryInputFormat.getSplits(wrapper));
  }

  /**
   * Tests the cleanupJob method of GsonBigQueryInputFormat with intermediate delete.
   */
  @Test
  public void testCleanupJobWithIntermediateDeleteAndGcsDelete()
      throws IOException {
    // Set intermediate table for deletion.
    config.setBoolean(BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, true);
    config.setBoolean(BigQueryConfiguration.DELETE_EXPORT_FILES_FROM_GCS_KEY, true);
    config.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, true);

    // Mock method calls to delete temporary table.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);
    when(mockBigqueryTables.delete(
        eq(dataProjectId), eq(intermediateDataset), eq(intermediateTable)))
        .thenReturn(mockBigqueryTablesDelete);

    Path tempPath = new Path(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
    FileSystem fs = tempPath.getFileSystem(config);
    fs.mkdirs(tempPath);
    Path dataFile = new Path(tempPath.toString() + "/data-00000.json");
    fs.createNewFile(dataFile);

    // Check file and directory exist.
    assertThat(fs.exists(tempPath)).isTrue();
    assertThat(fs.exists(dataFile)).isTrue();

    // Run method and verify calls.
    GsonBigQueryInputFormat.cleanupJob(mockBigQueryHelper, config);
    assertThat(!fs.exists(tempPath)).isTrue();
    assertThat(!fs.exists(dataFile)).isTrue();

    // Verify calls to delete temporary table.
    verify(mockBigquery, atLeastOnce()).tables();
    verify(mockBigqueryTables)
        .delete(eq(dataProjectId), eq(intermediateDataset), eq(intermediateTable));
    verify(mockBigqueryTablesDelete).execute();

    // The getTable in constructor of ShardedExportToCloudStorage.
    verify(mockBigQueryHelper, times(1)).getTable(eq(tableRef));

    // To make the low-level "delete" call.
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
  }

  /**
   * Tests the cleanupJob method of GsonBigQueryInputFormat with intermediate delete.
   */
  @Test
  public void testCleanupJobWithIntermediateDeleteNoGcsDelete()
      throws IOException {
    // Set intermediate table for deletion.
    config.setBoolean(BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, true);
    config.setBoolean(BigQueryConfiguration.DELETE_EXPORT_FILES_FROM_GCS_KEY, false);
    config.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, true);

    // Mock method calls to delete temporary table.
    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);
    when(mockBigqueryTables.delete(
        eq(dataProjectId), eq(intermediateDataset), eq(intermediateTable)))
        .thenReturn(mockBigqueryTablesDelete);

    Path tempPath = new Path(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
    FileSystem fs = tempPath.getFileSystem(config);
    fs.mkdirs(tempPath);
    Path dataFile = new Path(tempPath.toString() + "/data-00000.json");
    fs.createNewFile(dataFile);

    // Check file and directory exist.
    assertThat(fs.exists(tempPath)).isTrue();
    assertThat(fs.exists(dataFile)).isTrue();

    // Run method and verify calls.
    GsonBigQueryInputFormat.cleanupJob(mockBigQueryHelper, config);
    assertThat(fs.exists(tempPath)).isTrue();
    assertThat(fs.exists(dataFile)).isTrue();

    // Verify calls to delete temporary table.
    verify(mockBigquery, times(1)).tables();
    verify(mockBigqueryTables)
        .delete(eq(dataProjectId), eq(intermediateDataset), eq(intermediateTable));
    verify(mockBigqueryTablesDelete).execute();

    // The getTable in constructor of ShardedExportToCloudStorage.
    verify(mockBigQueryHelper, times(1)).getTable(eq(tableRef));

    // To make the low-level "delete" call.
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();
  }

  /**
   * Tests the cleanupJob method of GsonBigQueryInputFormat with no intermediate delete.
   */
  @Test
  public void testCleanupJobWithNoIntermediateDelete()
      throws IOException {
    // Set intermediate table for deletion.
    config.setBoolean(BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, false);
    config.setBoolean(BigQueryConfiguration.DELETE_EXPORT_FILES_FROM_GCS_KEY, true);
    config.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, true);

    when(mockBigQueryHelper.getTable(any(TableReference.class)))
        .thenReturn(new Table());

    Path tempPath = new Path(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
    FileSystem fs = tempPath.getFileSystem(config);
    fs.mkdirs(tempPath);
    Path dataFile = new Path(tempPath.toString() + "/data-00000.json");
    fs.createNewFile(dataFile);
    assertThat(fs.exists(tempPath)).isTrue();
    assertThat(fs.exists(dataFile)).isTrue();

    // Run method and verify calls.
    GsonBigQueryInputFormat.cleanupJob(mockBigQueryHelper, config);

    assertThat(!fs.exists(tempPath)).isTrue();
    assertThat(!fs.exists(dataFile)).isTrue();

    verify(mockBigQueryHelper, times(1)).getTable(eq(tableRef));

    verifyNoMoreInteractions(mockBigquery, mockBigqueryTables);
  }

  /**
   * Tests the cleanupJob method of GsonBigQueryInputFormat with intermediate delete but no sharded
   * export.
   */
  @Test
  public void testCleanupJobWithIntermediateDeleteNoShardedExport()
      throws IOException {
    // Set intermediate table for deletion.
    config.setBoolean(BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, true);
    config.setBoolean(BigQueryConfiguration.DELETE_EXPORT_FILES_FROM_GCS_KEY, true);
    config.setBoolean(BigQueryConfiguration.ENABLE_SHARDED_EXPORT_KEY, false);

    // GCS cleanup should still happen.
    Path tempPath = new Path(config.get(BigQueryConfiguration.TEMP_GCS_PATH_KEY));
    FileSystem fs = tempPath.getFileSystem(config);
    fs.mkdirs(tempPath);
    Path dataFile = new Path(tempPath.toString() + "/data-00000.json");
    fs.createNewFile(dataFile);
    assertThat(fs.exists(tempPath)).isTrue();
    assertThat(fs.exists(dataFile)).isTrue();

    when(mockBigquery.tables()).thenReturn(mockBigqueryTables);
    when(mockBigqueryTables
        .delete(
            eq(dataProjectId), eq(intermediateDataset), eq(intermediateTable)))
        .thenReturn(mockBigqueryTablesDelete);

    // Run method and verify calls.
    GsonBigQueryInputFormat.cleanupJob(mockBigQueryHelper, config);

    assertThat(!fs.exists(tempPath)).isTrue();
    assertThat(!fs.exists(dataFile)).isTrue();

    verify(mockBigquery).tables();
    verify(mockBigqueryTables).delete(
        eq(dataProjectId), eq(intermediateDataset), eq(intermediateTable));
    verify(mockBigqueryTablesDelete).execute();

    // To make the low-level "delete" call.
    verify(mockBigQueryHelper, atLeastOnce()).getRawBigquery();

    verify(mockBigQueryHelper, times(1)).getTable(eq(tableRef));
    verifyNoMoreInteractions(mockBigquery);
  }

  /**
   * Helper class to provide a mock Bigquery for testing.
   */
  class GsonBigQueryInputFormatForTest
    extends GsonBigQueryInputFormat {

    @Override
    public Bigquery getBigQuery(Configuration config)
        throws GeneralSecurityException, IOException {
      return mockBigquery;
    }

    @Override
    public BigQueryHelper getBigQueryHelper(Configuration config)
        throws GeneralSecurityException, IOException {
      return mockBigQueryHelper;
    }
  }

  /**
   * Helper class to test behavior when an error is thrown while getting the Bigquery connection.
   */
  class GsonBigQueryInputFormatForTestGeneralSecurityException
    extends GsonBigQueryInputFormat {
    @Override
    public Bigquery getBigQuery(Configuration config)
        throws GeneralSecurityException, IOException {
      throw new GeneralSecurityException();
    }

    @Override
    public BigQueryHelper getBigQueryHelper(Configuration config)
        throws GeneralSecurityException, IOException {
      throw new GeneralSecurityException();
    }
  }
}
