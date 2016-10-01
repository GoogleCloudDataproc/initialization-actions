package com.google.cloud.hadoop.io.bigquery;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.fs.gcs.InMemoryGoogleHadoopFileSystem;
import com.google.cloud.hadoop.testing.CredentialConfigurationUtil;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for BigQueryOutputCommitter. */
@RunWith(JUnit4.class)
public class IndirectBigQueryOutputCommitterTest {
  /** Sample output projectId for the destination table. */
  private static final String OUTPUT_PROJECT_ID = "final-project";

  /** Sample output datasetId for the destination table. */
  private static final String OUTPUT_DATASET_ID = "test_final_dataset";

  /** Sample output tableId for the destination table. */
  private static final String OUTPUT_TABLE_ID = "test_final_table";

  /** Sample output table schema for the destination table. */
  private static final String OUTPUT_TABLE_SCHEMA =
      "[{'name': 'Word','type': 'STRING'},{'name': 'Count','type': 'INTEGER'}]";

  /** Sample GCS temporary path for IO. */
  private static final String GCS_TEMP_PATH = "gs://test_bucket/indirect/path/";

  /** Sample GCS temporary file in the GCS_TEMP_PATH. */
  private static final String GCS_SAMPLE_FILE_PATH = GCS_TEMP_PATH + "test_file";

  /** Sample output file format for the committer. */
  private static final BigQueryFileFormat OUTPUT_FILE_FORMAT =
      BigQueryFileFormat.LINE_DELIMITED_JSON;

  /** Verify exceptions are being thrown. */
  @Rule public final ExpectedException expectedException = ExpectedException.none();

  /** The expected table reference being derived. */
  private TableReference outputTableRef;

  /** The expected table schema being derived. */
  private TableSchema outputTableSchema;

  /** In memory file system for testing. */
  private Configuration conf;

  /** Path to use for sample data. */
  private Path outputPath;

  /** Sample file in the output path. */
  private Path outputSampleFilePath;

  /** Sample Job context for testing. */
  private JobContext jobContext;

  /** A sample task ID for the mock TaskAttemptContext. */
  private TaskAttemptID taskAttemptId;

  /** Instance of the output committer being tested. */
  private IndirectBigQueryOutputCommitter committer;

  @Mock private BigQueryHelper mockBigQueryHelper;
  @Mock private TaskAttemptContext mockTaskAttemptContext;

  /** Sets up common objects for testing before each test. */
  @Before
  public void setUp() throws IOException {
    // Generate Mocks.
    MockitoAnnotations.initMocks(this);

    // Generate the configuration.
    conf = InMemoryGoogleHadoopFileSystem.getSampleConfiguration();
    CredentialConfigurationUtil.addTestConfigurationSettings(conf);
    BigQueryConfiguration.configureBigQueryOutput(
        conf, OUTPUT_PROJECT_ID, OUTPUT_DATASET_ID, OUTPUT_TABLE_ID, OUTPUT_TABLE_SCHEMA);
    conf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, GCS_TEMP_PATH);

    // Setup sample data.
    outputTableRef =
        new TableReference()
            .setProjectId(OUTPUT_PROJECT_ID)
            .setDatasetId(OUTPUT_DATASET_ID)
            .setTableId(OUTPUT_TABLE_ID);

    List<TableFieldSchema> fieldSchema = BigQueryUtils.getSchemaFromString(OUTPUT_TABLE_SCHEMA);
    outputTableSchema = new TableSchema();
    outputTableSchema.setFields(fieldSchema);

    outputPath = new Path(GCS_TEMP_PATH);
    outputSampleFilePath = new Path(GCS_SAMPLE_FILE_PATH);
    jobContext = org.apache.hadoop.mapreduce.Job.getInstance(conf);
    taskAttemptId = new TaskAttemptID(new TaskID("sample_task", 100, false, 200), 1);

    // Configure mocks.
    when(mockTaskAttemptContext.getConfiguration()).thenReturn(conf);
    when(mockTaskAttemptContext.getTaskAttemptID()).thenReturn(taskAttemptId);

    // Setup committer
    committer =
        new IndirectBigQueryOutputCommitter(outputPath, mockTaskAttemptContext, OUTPUT_FILE_FORMAT);
    committer.setBigQueryHelper(mockBigQueryHelper);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockBigQueryHelper);
  }

  /** Helper method to create basic valid output based. */
  public void generateSampleFiles() throws IOException {
    // Create the files to test with.
    FileSystem fs = outputPath.getFileSystem(conf);
    // Workaround for a FileOutputCommitter quirk.
    // Because of backwards Hadoop compatibility, this URI is hard coded.
    Path attemptPath = new Path(GCS_TEMP_PATH + "_temporary/0");
    fs.mkdirs(attemptPath);
    fs.createNewFile(outputSampleFilePath);
    fs.createNewFile(attemptPath);
    assertTrue(fs.exists(outputPath));
    assertTrue(fs.exists(outputSampleFilePath));
  }

  /**
   * Test that a BigQuery import request is made with the correct files under normal circumstances.
   */
  @Test
  public void testCommitJob() throws IOException, InterruptedException {
    // Setup the sample directory.
    generateSampleFiles();

    committer.commitJob(jobContext);

    // Setup a captor for the GCS paths argument
    @SuppressWarnings({"rawtypes", "unchecked", "cast"})
    // Class<List> is neither a sub/supertype of Class<List<String>>, the latter doesn't even exist.
    Class<List<String>> listClass = (Class<List<String>>) (Class) List.class;
    ArgumentCaptor<List<String>> gcsOutputFileCaptor = ArgumentCaptor.forClass(listClass);

    // Verify we're making the BigQuery import call.
    verify(mockBigQueryHelper)
        .importBigQueryFromGcs(
            eq(OUTPUT_PROJECT_ID),
            eq(outputTableRef),
            eq(outputTableSchema),
            eq(OUTPUT_FILE_FORMAT),
            gcsOutputFileCaptor.capture(),
            eq(true));

    // Assert the passed files contains our sample file.
    assertThat(gcsOutputFileCaptor.getValue(), containsInAnyOrder(GCS_SAMPLE_FILE_PATH));
  }

  /** Test to make sure an IOException is thrown on interrupt of the BigQuery import call. */
  @SuppressWarnings("unchecked")
  @Test
  public void testCommitJobInterrupt() throws IOException, InterruptedException {
    // Setup the sample directory.
    generateSampleFiles();

    // Setup the expected exception
    InterruptedException helperInterruptedException = new InterruptedException("Test exception");
    expectedException.expect(IOException.class);
    expectedException.expectCause(is(helperInterruptedException));

    // Configure special case mock.
    doThrow(helperInterruptedException)
        .when(mockBigQueryHelper)
        .importBigQueryFromGcs(
            any(String.class),
            any(TableReference.class),
            any(TableSchema.class),
            any(BigQueryFileFormat.class),
            any(List.class),
            eq(true));

    try {
      committer.commitJob(jobContext);
    } finally {
      // Verify we're making the BigQuery import call.
      verify(mockBigQueryHelper)
          .importBigQueryFromGcs(
              eq(OUTPUT_PROJECT_ID),
              eq(outputTableRef),
              eq(outputTableSchema),
              eq(OUTPUT_FILE_FORMAT),
              any(List.class), // Tested, no need to capture
              eq(true));
    }
  }

  /** Test that cleanup actually cleans up. */
  @Test
  public void testCleanup() throws IOException {
    // Setup the sample directory.
    generateSampleFiles();
    FileSystem fs = outputPath.getFileSystem(conf);

    committer.cleanup();

    // Ensure files are deleted by cleanup.
    assertTrue(!fs.exists(outputPath));
    assertTrue(!fs.exists(outputSampleFilePath));
  }
}
