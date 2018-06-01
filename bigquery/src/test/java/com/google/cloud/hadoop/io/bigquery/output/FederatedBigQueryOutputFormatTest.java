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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.hadoop.fs.gcs.InMemoryGoogleHadoopFileSystem;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.testing.CredentialConfigurationUtil;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
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
public class FederatedBigQueryOutputFormatTest {

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

  /** Sample raw output path for data. */
  private static final String TEST_OUTPUT_PATH_STRING = "gs://test_bucket/test_directory/";

  /** Sample output path for data. */
  private static final Path TEST_OUTPUT_PATH = new Path(TEST_OUTPUT_PATH_STRING);

  /** A sample task ID for the mock TaskAttemptContext. */
  private static final TaskAttemptID TEST_TASK_ATTEMPT_ID =
      new TaskAttemptID(new TaskID("sample_task", 100, false, 200), 1);

  /** GoogleHadoopFileSystem to use. */
  private InMemoryGoogleHadoopFileSystem ghfs;

  /** In memory file system for testing. */
  private Configuration conf;

  /** Sample Job context for testing. */
  private Job job;

  /** The output format being tested. */
  private FederatedBigQueryOutputFormat<Text, Text> outputFormat;

  // Mocks.
  @Mock private TaskAttemptContext mockTaskAttemptContext;
  @Mock private FileOutputFormat<Text, Text> mockFileOutputFormat;
  @Mock private OutputCommitter mockOutputCommitter;
  @Mock private RecordWriter<Text, Text> mockRecordWriter;

  /** Verify exceptions are being thrown. */
  /** Sets up common objects for testing before each test. */
  @Before
  public void setUp() throws IOException, InterruptedException {
    // Generate Mocks.
    MockitoAnnotations.initMocks(this);

    // Create the file system.
    ghfs = new InMemoryGoogleHadoopFileSystem();

    // Create the configuration, but setup in the tests.
    job = Job.getInstance(InMemoryGoogleHadoopFileSystem.getSampleConfiguration());
    conf = job.getConfiguration();
    CredentialConfigurationUtil.addTestConfigurationSettings(conf);
    BigQueryOutputConfiguration.configureWithAutoSchema(
        conf,
        QUALIFIED_TEST_TABLE_ID,
        TEST_OUTPUT_PATH_STRING,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS);

    // Configure mocks.
    when(mockTaskAttemptContext.getConfiguration()).thenReturn(conf);
    when(mockTaskAttemptContext.getTaskAttemptID()).thenReturn(TEST_TASK_ATTEMPT_ID);
    when(mockFileOutputFormat.getOutputCommitter(eq(mockTaskAttemptContext)))
        .thenReturn(mockOutputCommitter);
    when(mockFileOutputFormat.getRecordWriter(eq(mockTaskAttemptContext)))
        .thenReturn(mockRecordWriter);

    // Create and setup the output format.
    outputFormat = new FederatedBigQueryOutputFormat<Text, Text>();
    outputFormat.setDelegate(mockFileOutputFormat);
  }

  @After
  public void tearDown() throws IOException {
    verifyNoMoreInteractions(mockFileOutputFormat);
    verifyNoMoreInteractions(mockOutputCommitter);

    // File system changes leak between tests, always clean up.
    ghfs.delete(TEST_OUTPUT_PATH, true);
  }

  /** Test the correct committer is returned. */
  @Test
  public void testCreateCommitter() throws IOException {
    FederatedBigQueryOutputCommitter committer =
        (FederatedBigQueryOutputCommitter) outputFormat.createCommitter(mockTaskAttemptContext);

    assertThat(committer.getDelegate(), is(mockOutputCommitter));
    verify(mockFileOutputFormat).getOutputCommitter(eq(mockTaskAttemptContext));
  }
}
