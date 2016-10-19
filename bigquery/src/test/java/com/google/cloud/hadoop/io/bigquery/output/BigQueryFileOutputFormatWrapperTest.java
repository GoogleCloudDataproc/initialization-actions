package com.google.cloud.hadoop.io.bigquery.output;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class BigQueryFileOutputFormatWrapperTest {

  /** Sample projectId for output. */
  private static final String TEST_PROJECT_ID = "domain:project";

  /** Sample datasetId for output. */
  private static final String TEST_DATASET_ID = "dataset";

  /** Sample tableId for output. */
  private static final String TEST_TABLE_ID = "table";

  /** Sample output file format for the committer. */
  private static final BigQueryFileFormat TEST_FILE_FORMAT =
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON;

  /** Sample output format class for the configuration. */
  @SuppressWarnings("rawtypes")
  private static final Class<? extends FileOutputFormat> TEST_OUTPUT_CLASS = TextOutputFormat.class;

  /** Sample GCS temporary path for IO. */
  private static final Path GCS_TEMP_PATH = new Path("gs://test_bucket/indirect/path/");

  /** A sample task ID for the mock TaskAttemptContext. */
  private static final TaskAttemptID TEST_TASK_ATTEMPT_ID =
      new TaskAttemptID(new TaskID("sample_task", 100, false, 200), 1);

  /** GoogleHadoopGlobalRootedFileSystem to use. */
  private InMemoryGoogleHadoopFileSystem ghfs;

  /** In memory file system for testing. */
  private Configuration conf;

  /** Sample Job context for testing. */
  private Job job;

  /** The output format being tested. */
  private BigQueryFileOutputFormatWrapper<Text, Text> outputFormat;

  // Mocks.
  @Mock private TaskAttemptContext mockTaskAttemptContext;
  @Mock private FileOutputFormat<Text, Text> mockFileOutputFormat;
  @Mock private OutputCommitter mockOutputCommitter;
  @Mock private RecordWriter<Text, Text> mockRecordWriter;

  /** Verify exceptions are being thrown. */
  @Rule public final ExpectedException expectedException = ExpectedException.none();

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
    BigQueryOutputConfiguration.configure(
        conf,
        TEST_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_FILE_FORMAT,
        TEST_OUTPUT_CLASS,
        null);

    // Configure mocks.
    when(mockTaskAttemptContext.getConfiguration()).thenReturn(conf);
    when(mockTaskAttemptContext.getTaskAttemptID()).thenReturn(TEST_TASK_ATTEMPT_ID);
    when(mockFileOutputFormat.getOutputCommitter(eq(mockTaskAttemptContext)))
        .thenReturn(mockOutputCommitter);
    when(mockFileOutputFormat.getRecordWriter(eq(mockTaskAttemptContext)))
        .thenReturn(mockRecordWriter);

    // Create and setup the output format.
    outputFormat = new BigQueryFileOutputFormatWrapper<Text, Text>();
    outputFormat.setDelegate(mockFileOutputFormat);
  }

  @After
  public void tearDown() throws IOException {
    verifyNoMoreInteractions(mockFileOutputFormat);
    verifyNoMoreInteractions(mockOutputCommitter);

    // File system changes leak between tests, always clean up.
    ghfs.delete(GCS_TEMP_PATH, true);
  }

  /** Test normal expected use of the function. */
  @Test
  public void testCheckOutputSpecs() throws IOException {
    // Setup configuration.
    FileOutputFormat.setOutputPath(job, GCS_TEMP_PATH);

    outputFormat.checkOutputSpecs(mockTaskAttemptContext);

    verify(mockFileOutputFormat).checkOutputSpecs(eq(mockTaskAttemptContext));
  }

  /** Test an error is thrown when the output path isn't set. */
  @Test
  public void testCheckOutputSpecsPathNotSet() throws IOException {
    expectedException.expect(IOException.class);
    expectedException.expectMessage("FileOutputFormat output path not set.");

    outputFormat.checkOutputSpecs(mockTaskAttemptContext);
  }

  /** Test an error is thrown when the output format's directory already exists. */
  @Test
  public void testCheckOutputSpecsAlreadyExists() throws IOException {
    // Setup configuration.
    FileOutputFormat.setOutputPath(job, GCS_TEMP_PATH);
    ghfs.mkdirs(GCS_TEMP_PATH);

    expectedException.expect(IOException.class);
    expectedException.expectMessage("The output path '" + GCS_TEMP_PATH + "' already exists.");

    outputFormat.checkOutputSpecs(mockTaskAttemptContext);
  }

  /** Test an error is throw when the user wants their output compressed. */
  @Test
  public void testCheckOutputSpecsCompressedOutput() throws IOException {
    // Setup configuration.
    FileOutputFormat.setOutputPath(job, GCS_TEMP_PATH);
    FileOutputFormat.setCompressOutput(job, true);

    expectedException.expect(IOException.class);
    expectedException.expectMessage("Compression isn't supported for this OutputFormat.");

    outputFormat.checkOutputSpecs(mockTaskAttemptContext);
  }

  /** Test getOutputCommitter is calling the delegate and the mock OutputCommitter is returned. */
  @Test
  public void testGetOutputCommitter() throws IOException {
    OutputCommitter committer = outputFormat.getOutputCommitter(mockTaskAttemptContext);

    // Verify the delegate is being called and the mock OutputCommitter is returned.
    assertThat(committer, is(mockOutputCommitter));
    verify(mockFileOutputFormat).getOutputCommitter(eq(mockTaskAttemptContext));
  }

  /** Test getRecordWriter is returning the mock RecordWriter. */
  @Test
  public void testGetRecordWriter() throws IOException, InterruptedException {
    RecordWriter<Text, Text> recordWriter = outputFormat.getRecordWriter(mockTaskAttemptContext);

    // Verify the delegate is being called and the mock RecordWriter is returned.
    assertThat(recordWriter, is(mockRecordWriter));
    verify(mockFileOutputFormat).getRecordWriter(eq(mockTaskAttemptContext));
  }

  /** Test createCommitter is calling the delegate and the mock OutputCommitter is returned. */
  @Test
  public void testCreateCommitter() throws IOException {
    OutputCommitter committer = outputFormat.createCommitter(mockTaskAttemptContext);

    // Verify the delegate is being called and the mock OutputCommitter is returned.
    assertThat(committer, is(mockOutputCommitter));
    verify(mockFileOutputFormat).getOutputCommitter(eq(mockTaskAttemptContext));
  }

  /** Test getDelegate is returning the correct delegate. */
  @Test
  public void testGetDelegate() throws IOException {
    // Setup configuration.
    outputFormat.setDelegate(null);

    FileOutputFormat<Text, Text> delegate = outputFormat.getDelegate(conf);

    // Verify the delegate is the correct type.
    assertTrue(delegate instanceof TextOutputFormat);
  }
}
