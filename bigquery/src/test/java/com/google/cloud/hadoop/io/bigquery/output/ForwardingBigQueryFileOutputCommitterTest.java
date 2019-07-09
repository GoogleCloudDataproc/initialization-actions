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
package com.google.cloud.hadoop.io.bigquery.output;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.fs.gcs.InMemoryGoogleHadoopFileSystem;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.util.testing.CredentialConfigurationUtil;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class ForwardingBigQueryFileOutputCommitterTest {

  /** Sample projectId for output. */
  private static final String TEST_PROJECT_ID = "domain:project";

  /** Sample datasetId for output. */
  private static final String TEST_DATASET_ID = "dataset";

  /** Sample tableId for output. */
  private static final String TEST_TABLE_ID = "table";

  /** Sample qualified tableId for output. */
  private static final String QUALIFIED_TEST_TABLE_ID =
      String.format("%s:%s.%s", TEST_PROJECT_ID, TEST_DATASET_ID, TEST_TABLE_ID);

  /** Sample output file format for the committer. */
  private static final BigQueryFileFormat TEST_FILE_FORMAT =
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON;

  /** Sample output format class for the configuration. */
  @SuppressWarnings("rawtypes")
  private static final Class<? extends FileOutputFormat> TEST_OUTPUT_CLASS = TextOutputFormat.class;

  /** Sample table schema used for output. */
  private static final BigQueryTableSchema TEST_TABLE_SCHEMA =
      new BigQueryTableSchema()
          .setFields(
              ImmutableList.of(
                  new BigQueryTableFieldSchema().setName("Word").setType("STRING"),
                  new BigQueryTableFieldSchema().setName("Count").setType("INTEGER"),
                  new BigQueryTableFieldSchema()
                      .setName("MetaInfo")
                      .setType("RECORD")
                      .setFields(
                          ImmutableList.of(
                              new BigQueryTableFieldSchema()
                                  .setName("NestedField1")
                                  .setType("STRING"),
                              new BigQueryTableFieldSchema()
                                  .setName("NestedField2")
                                  .setType("INTEGER")))));

  /** Sample task ID for the mock TaskAttemptContext. */
  private static final TaskAttemptID TEST_TASK_ATTEMPT_ID =
      new TaskAttemptID(new TaskID("sample_task", 100, false, 200), 1);

  /** Sample raw output path for data. */
  private static final String TEST_OUTPUT_PATH_STRING = "gs://test_bucket/test_directory/";

  /** Sample output file. */
  private static final String TEST_OUTPUT_FILE_STRING = TEST_OUTPUT_PATH_STRING + "test_file";

  /** GoogleHadoopFileSystem to use. */
  private InMemoryGoogleHadoopFileSystem ghfs;

  /** In memory file system for testing. */
  private Configuration conf;

  /** Path to use for sample data. */
  private Path outputPath;

  /** Sample file in the output path. */
  private Path outputSampleFilePath;

  /** Sample Job context for testing. */
  private Job job;

  /** Instance of the output committer being tested. */
  private ForwardingBigQueryFileOutputCommitter committer;

  @Mock private TaskAttemptContext mockTaskAttemptContext;
  @Mock private OutputCommitter mockCommitter;

  /** Sets up common objects for testing before each test. */
  @Before
  public void setUp() throws IOException {
    // Generate Mocks.
    MockitoAnnotations.initMocks(this);

    // Create the file system.
    ghfs = new InMemoryGoogleHadoopFileSystem();

    // Setup the configuration.
    job = Job.getInstance(InMemoryGoogleHadoopFileSystem.getSampleConfiguration());
    conf = job.getConfiguration();
    CredentialConfigurationUtil.addTestConfigurationSettings(conf);
    BigQueryOutputConfiguration.configure(
        conf,
        QUALIFIED_TEST_TABLE_ID,
        TEST_TABLE_SCHEMA,
        TEST_OUTPUT_PATH_STRING,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS);

    // Setup sample data.
    outputPath = BigQueryOutputConfiguration.getGcsOutputPath(conf);
    outputSampleFilePath = new Path(TEST_OUTPUT_FILE_STRING);

    // Configure mocks.
    when(mockTaskAttemptContext.getConfiguration()).thenReturn(conf);
    when(mockTaskAttemptContext.getTaskAttemptID()).thenReturn(TEST_TASK_ATTEMPT_ID);

    // Setup committer.
    committer = new ForwardingBigQueryFileOutputCommitter(mockTaskAttemptContext, mockCommitter);
  }

  @After
  public void tearDown() throws IOException {
    verifyNoMoreInteractions(mockCommitter);

    // Delete files after use as they're not cleaned up automatically.
    ghfs.delete(outputPath, true);
  }

  /** Helper method to create basic valid output based. */
  private void generateSampleFiles() throws IOException {
    ghfs.createNewFile(outputSampleFilePath);

    // Verify the files were created.
    assertThat(ghfs.exists(outputPath)).isTrue();
    assertThat(ghfs.exists(outputSampleFilePath)).isTrue();
  }

  /** Test to ensure the underlying delegate is being passed the commitJob call. */
  @Test
  public void testCommitJob() throws IOException {
    committer.commitJob(job);

    // Verify the delegate is being called.
    verify(mockCommitter).commitJob(eq(job));
  }

  /** Test to ensure the underlying delegate is being passed the abortJob call. */
  @Test
  public void testAbortJob() throws IOException {
    committer.abortJob(mockTaskAttemptContext, State.KILLED);

    // Verify the delegate is being called.
    verify(mockCommitter).abortJob(eq(mockTaskAttemptContext), eq(State.KILLED));
  }

  /** Test to ensure the underlying delegate is being passed the abortTask call. */
  @Test
  public void testAbortTask() throws IOException {
    committer.abortTask(mockTaskAttemptContext);

    // Verify the delegate is being called.
    verify(mockCommitter).abortTask(eq(mockTaskAttemptContext));
  }

  /** Test to ensure the underlying delegate is being passed the commitTask call. */
  @Test
  public void testCommitTask() throws IOException {
    committer.commitTask(mockTaskAttemptContext);

    // Verify the delegate is being called.
    verify(mockCommitter).commitTask(eq(mockTaskAttemptContext));
  }

  /** Test to ensure the underlying delegate is being passed the needsTaskCommit call. */
  @Test
  public void testNeedsTaskCommit() throws IOException {
    // Mock sample return.
    when(mockCommitter.needsTaskCommit(mockTaskAttemptContext)).thenReturn(false);

    boolean result = committer.needsTaskCommit(mockTaskAttemptContext);

    // Verify the delegate is being called and returns the correct data.
    verify(mockCommitter).needsTaskCommit(eq(mockTaskAttemptContext));
    assertThat(result).isFalse();
  }

  /** Test to ensure the underlying delegate is being passed the setupJob call. */
  @Test
  public void testSetupJob() throws IOException {
    committer.setupJob(mockTaskAttemptContext);

    // Verify the delegate is being called.
    verify(mockCommitter).setupJob(eq(mockTaskAttemptContext));
  }

  /** Test to ensure the underlying delegate is being passed the setupTask call. */
  @Test
  public void testSetupTask() throws IOException {
    committer.setupTask(mockTaskAttemptContext);

    // Verify the delegate is being called.
    verify(mockCommitter).setupTask(eq(mockTaskAttemptContext));
  }

  /** Test that getOutputFileURIs returns the correct data. */
  @Test
  public void testGetOutputFileURIs() throws IOException {
    // Setup the sample directory.
    generateSampleFiles();

    List<String> outputFileURIs = committer.getOutputFileURIs();

    // Verify the file in the output path is being returFned.
    assertThat(outputFileURIs).containsExactly(TEST_OUTPUT_FILE_STRING);
  }

  /** Test that cleanup actually cleans up. */
  @Test
  public void testCleanup() throws IOException {
    // Setup the sample directory.
    generateSampleFiles();

    committer.cleanup(job);

    // Ensure files are deleted by cleanup.
    assertThat(!ghfs.exists(outputPath)).isTrue();
    assertThat(!ghfs.exists(outputSampleFilePath)).isTrue();
  }
}
