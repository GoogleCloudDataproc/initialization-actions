package com.google.cloud.hadoop.io.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.testing.CredentialConfigurationUtil;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Unit tests for BigQueryRecordWriter.
 */
@RunWith(JUnit4.class)
public class BigQueryRecordWriterTest {
  // Logger.
  protected static final LogUtil log = new LogUtil(BigQueryRecordWriter.class);

  // Sample LongWritable and JsonObject for testing.
  private LongWritable bigqueryKey;
  private JsonObject jsonValue;

  // Sample output schema.
  private List<TableFieldSchema> fields;

  // Sample projectId for the owner of the BigQuery job.
  private String jobProjectId;

  // Sample projectId for the owner of the output table.
  private String outputProjectId;

  // Sample tableId.
  private String tableId;

  // Sample datasetId.
  private String datasetId;

  // Gson to parse jsonValue.
  private Gson gson;

  // Sample JobReference.
  private JobReference jobReference;

  // Sample job.
  private Job jobReturn;

  // Sample JobStatus.
  private JobStatus jobStatus;

  // Fake TaskAttemptID String.
  private String taskIdentifier;

  @Mock private Insert mockInsert;
  @Mock private BigQueryFactory mockFactory;
  @Mock private Bigquery mockBigQuery;
  @Mock private BigQueryHelper mockBigQueryHelper;
  @Mock private TaskAttemptContext mockContext;
  @Mock private Bigquery.Jobs mockBigQueryJobs;
  @Mock private Bigquery.Jobs.Get mockJobsGet;
  @Mock private Progressable progressable;
  @Mock private ExecutorService mockExecutorService;
  @Mock private ClientRequestHelper<Job> mockClientRequestHelper;
  @Mock private HttpHeaders mockHeaders;
  @Mock private ApiErrorExtractor mockErrorExtractor;

  // The RecordWriter being tested.
  private BigQueryRecordWriter<LongWritable, JsonObject> recordWriter;

  @Before
  public void setUp() throws IOException, GeneralSecurityException {
    // Generate Mocks.  
    MockitoAnnotations.initMocks(this);
    
    // Create Gson to parse Json.
    gson = new Gson();

    // Set input parameters for testing.
    fields = BigQueryUtils.getSchemaFromString(
        "[{'name': 'Name','type': 'STRING'},{'name': 'Number','type': 'INTEGER'}]");
    jobProjectId = "test_job_project";
    outputProjectId = "test_output_project";
    tableId = "test_table";
    datasetId = "test_dataset";
    taskIdentifier = "attempt_201501292132_0016_r_000033_0";

    // Create the key, value pair.
    bigqueryKey = new LongWritable(123);
    jsonValue = new JsonObject();
    jsonValue.addProperty("Name", "test name");
    jsonValue.addProperty("Number", "123");

    // Create the job result.
    jobReference = new JobReference();
    jobReference.setProjectId(jobProjectId);
    jobReference.setJobId(taskIdentifier + "-12345");

    jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);

    jobReturn = new Job();
    jobReturn.setStatus(jobStatus);
    jobReturn.setJobReference(jobReference);

    // Mock BigQuery.
    when(mockFactory.getBigQueryHelper(any(Configuration.class))).thenReturn(mockBigQueryHelper);
    when(mockBigQueryHelper.getRawBigquery()).thenReturn(mockBigQuery);
    when(mockBigQuery.jobs()).thenReturn(mockBigQueryJobs);
    when(mockBigQueryJobs.get(any(String.class), any(String.class)))
        .thenReturn(mockJobsGet);
    when(mockJobsGet.execute()).thenReturn(jobReturn);

    when(mockBigQueryJobs.insert(
        any(String.class), any(Job.class), any(ByteArrayContent.class)))
        .thenReturn(mockInsert);
    when(mockInsert.setProjectId(any(String.class))).thenReturn(mockInsert);

    when(mockClientRequestHelper.getRequestHeaders(any(Insert.class)))
        .thenReturn(mockHeaders);

    // Mock context result
    when(mockContext.getConfiguration()).thenReturn(
        CredentialConfigurationUtil.getTestConfiguration());
  }
  
  /**
   * Verify no more mock interactions.
   */
  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockFactory);
    verifyNoMoreInteractions(mockBigQuery);
    verifyNoMoreInteractions(mockBigQueryJobs);
    verifyNoMoreInteractions(mockJobsGet);
    verifyNoMoreInteractions(mockExecutorService);
  }

  private void initializeRecordWriter() throws IOException {
    when(mockBigQueryHelper.createJobReference(any(String.class), any(String.class)))
        .thenReturn(jobReference);
    recordWriter = new BigQueryRecordWriter<>(
        mockFactory,
        mockExecutorService,
        mockClientRequestHelper,
        new Configuration(),
        progressable,
        taskIdentifier,
        fields,
        jobProjectId,
        getSampleTableRef(),
        64 * 1024 * 1024);
    recordWriter.setErrorExtractor(mockErrorExtractor);
    verify(mockBigQueryHelper).createJobReference(eq(jobProjectId), eq(taskIdentifier));
  }
  
  /**
   * Tests the write method of BigQueryRecordWriter for a single write.
   */
  @Test
  public void testSingleWrite() throws IOException, GeneralSecurityException {
    initializeRecordWriter();

    final ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), inputStreamCaptor.capture());

    ArgumentCaptor<Runnable> runCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService).execute(runCaptor.capture());

    // Write the key, value pair.
    callWrite(recordWriter, 1);

    final byte[] readData = new byte[(int) recordWriter.getBytesWritten()];
    when(mockInsert.execute())
        .thenAnswer(new Answer<Job>() {
          @Override
          public Job answer(InvocationOnMock unused) {
            try {
              inputStreamCaptor.getValue().getInputStream().read(readData);
            } catch (IOException ioe) {
              fail(ioe.getMessage());
            }
            return jobReturn;
          }
        });
    runCaptor.getValue().run();

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), eq(jobReference.getJobId()));
    verify(mockExecutorService).shutdown();

    String readDataString = new String(readData, StandardCharsets.UTF_8);
    assertEquals("{\"Name\":\"test name\",\"Number\":\"123\"}\n", readDataString);
  }

  /**
   * Tests the write method of BigQueryRecordWriter when nothing is written.
   */
  @Test
  public void testNoWrites() throws IOException, GeneralSecurityException {
    initializeRecordWriter();

    final ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), inputStreamCaptor.capture());

    ArgumentCaptor<Runnable> runCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService).execute(runCaptor.capture());

    final byte[] readData = new byte[(int) recordWriter.getBytesWritten()];
    when(mockInsert.execute())
        .thenAnswer(new Answer<Job>() {
          @Override
          public Job answer(InvocationOnMock unused) {
            try {
              inputStreamCaptor.getValue().getInputStream().read(readData);
            } catch (IOException ioe) {
              fail(ioe.getMessage());
            }
            return jobReturn;
          }
        });
    runCaptor.getValue().run();

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), eq(jobReference.getJobId()));
    verify(mockExecutorService).shutdown();

    String readDataString = new String(readData, StandardCharsets.UTF_8);
    assertEquals("", readDataString);
  }

  /**
   * Tests the write method of BigQueryRecordWriter without writing anything but throwing a 409
   * conflict from the job-insertion.
   */
  @Test
  public void testConflictExceptionOnCreate() throws IOException, GeneralSecurityException {
    initializeRecordWriter();

    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), any(AbstractInputStreamContent.class));

    ArgumentCaptor<Runnable> runCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService).execute(runCaptor.capture());

    IOException fakeConflictException = new IOException("fake 409 conflict");
    when(mockInsert.execute())
        .thenThrow(fakeConflictException);
    when(mockErrorExtractor.itemAlreadyExists(any(IOException.class)))
        .thenReturn(true);
    runCaptor.getValue().run();

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), eq(jobReference.getJobId()));
    verify(mockExecutorService).shutdown();
  }

  /**
   * Tests the write method of BigQueryRecordWriter without writing anything but throwing an
   * unhandled exeption from the job-insertion.
   */
  @Test
  public void testUnhandledExceptionOnCreate() throws IOException, GeneralSecurityException {
    initializeRecordWriter();

    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), any(AbstractInputStreamContent.class));

    ArgumentCaptor<Runnable> runCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService).execute(runCaptor.capture());

    IOException fakeUnhandledException = new IOException("fake unhandled exception");
    when(mockInsert.execute())
        .thenThrow(fakeUnhandledException);
    when(mockErrorExtractor.itemAlreadyExists(any(IOException.class)))
        .thenReturn(false);

    // Exception gets stashed away, *not* thrown in the runner.
    runCaptor.getValue().run();

    // Close the RecordWriter; the stored exception finally propagates out.
    try {
      recordWriter.close(mockContext);
      fail("Expected IOException on close, got no exception.");
    } catch (IOException ioe) {
      assertEquals(fakeUnhandledException, ioe.getCause());
    }

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(1)).jobs();
    verify(mockExecutorService).shutdown();
  }

  @Test
  public void testMultipleWrites() throws IOException, GeneralSecurityException {
    initializeRecordWriter();

    final ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), inputStreamCaptor.capture());

    ArgumentCaptor<Runnable> runCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService).execute(runCaptor.capture());

    // Write the key, value pair.
    callWrite(recordWriter, 2);

    final byte[] readData = new byte[(int) recordWriter.getBytesWritten()];
    when(mockInsert.execute())
        .thenAnswer(new Answer<Job>() {
          @Override
          public Job answer(InvocationOnMock unused) {
            try {
              inputStreamCaptor.getValue().getInputStream().read(readData);
            } catch (IOException ioe) {
              fail(ioe.getMessage());
            }
            return jobReturn;
          }
        });
    runCaptor.getValue().run();

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), eq(jobReference.getJobId()));
    verify(mockExecutorService).shutdown();

    String readDataString = new String(readData, StandardCharsets.UTF_8);
    assertEquals(
        "{\"Name\":\"test name\",\"Number\":\"123\"}\n"
        + "{\"Name\":\"test name\",\"Number\":\"123\"}\n",
        readDataString);
  }

  /**
   * Helper method call write numWrites times on given RecordWriter.
   *
   * @param recordWriter the RecordWriter to use.
   * @param numWrites the number of times to call write on recordWriter.
   * @throws IOException
   */
  private void callWrite(BigQueryRecordWriter<LongWritable, JsonObject> recordWriter, int numWrites)
      throws IOException {
    for (int i = 0; i < numWrites; i++) {
      recordWriter.write(bigqueryKey, jsonValue);
    }
  }

  /**
   * Helper method to get the expected TableReference for the output table.
   */
  private TableReference getSampleTableRef() {
    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(datasetId);
    tableRef.setTableId(tableId);
    tableRef.setProjectId(outputProjectId);
    return tableRef;
  }

  /**
   * Helper method to get the load request to BigQuery with numValues copies of jsonValue.
   */
  private Job getExpectedJob() {
    // Configure a write job.
    JobConfigurationLoad loadConfig = new JobConfigurationLoad();
    loadConfig.setCreateDisposition("CREATE_IF_NEEDED");
    loadConfig.setWriteDisposition("WRITE_TRUNCATE");
    loadConfig.setSourceFormat("NEWLINE_DELIMITED_JSON");

    // Describe the resulting table you are importing to:
    loadConfig.setDestinationTable(getSampleTableRef());

    // Create and set the output schema.
    TableSchema schema = new TableSchema();
    schema.setFields(fields);
    loadConfig.setSchema(schema);

    // Create Job configuration.
    JobConfiguration jobConfig = new JobConfiguration();
    jobConfig.setLoad(loadConfig);

    // Set the output write job.
    Job expectedJob = new Job();
    expectedJob.setConfiguration(jobConfig);
    expectedJob.setJobReference(jobReference);
    return expectedJob;
  }
}
