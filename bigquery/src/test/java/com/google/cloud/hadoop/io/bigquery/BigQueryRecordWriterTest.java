package com.google.cloud.hadoop.io.bigquery;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.ByteArrayContent;
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
import com.google.cloud.hadoop.util.LogUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * Unit tests for BigQueryRecordWriter.
 */
@RunWith(JUnit4.class)
public class BigQueryRecordWriterTest {
  // Logger.
  protected static final LogUtil log = new LogUtil(BigQueryRecordWriter.class);

  @Mock
  private BigQueryFactory mockFactory;

  // Mock BigQuery.
  @Mock
  private Bigquery mockBigQuery;

  // Mock TaskAttemptContext.
  @Mock
  private TaskAttemptContext mockContext;

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

  // Mock BigQuery Jobs.
  @Mock
  private Bigquery.Jobs mockBigQueryJobs;

  // Mock BigQuery Jobs return.
  @Mock
  private Bigquery.Jobs.Get mockJobsGet;

  @Mock
  private Progressable progressable;

  // Sample JobStatus.
  private JobStatus jobStatus;

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

    // Create the key, value pair.
    bigqueryKey = new LongWritable(123);
    jsonValue = new JsonObject();
    jsonValue.addProperty("Name", "test name");
    jsonValue.addProperty("Number", "123");

    // Create the job result.
    jobReference = new JobReference();
    jobReturn = new Job();
    jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);
    jobReturn.setStatus(jobStatus);
    jobReturn.setJobReference(jobReference);

    // Mock BigQuery.
    when(mockJobsGet.execute()).thenReturn(jobReturn);
    when(mockBigQuery.jobs()).thenReturn(mockBigQueryJobs);
    when(mockBigQueryJobs.get(jobProjectId, jobReference.getJobId()))
        .thenReturn(mockJobsGet).thenReturn(mockJobsGet);
    when(mockJobsGet.execute()).thenReturn(jobReturn);

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
  }
  
  /**
   * Tests the write method of BigQueryRecordWriter for a single write.
   */
  @Test
  public void testSingleWrite() throws IOException, GeneralSecurityException {
    Insert mockInsert = mock(Insert.class);

    when(mockInsert.setProjectId(jobProjectId)).thenReturn(mockInsert);
    when(mockBigQueryJobs.insert(
        eq(jobProjectId), eq(getExpectedJob()), argThat(new IsNumRecords(1))))
        .thenReturn(mockInsert);
    when(mockInsert.execute()).thenReturn(jobReturn);
    when(mockFactory.getBigQuery(any(Configuration.class))).thenReturn(mockBigQuery);

    // Get the RecordWriter.
    BigQueryRecordWriter<LongWritable, JsonObject> recordWriter = new BigQueryRecordWriter<>(
        mockFactory,
        new Configuration(),
        progressable,
        fields,
        jobProjectId,
        getSampleTableRef(),
        1000);

    // Write the key, value pair.
    callWrite(recordWriter, 1);

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQuery(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), argThat(new IsNumRecords(1)));
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), any(String.class));
  }

  /**
   * Tests the write method of BigQueryRecordWriter when nothing is written.
   */
  @Test
  public void testNoWrites() throws IOException, GeneralSecurityException {
    Insert mockInsert = mock(Insert.class);

    when(mockFactory.getBigQuery(any(Configuration.class))).thenReturn(mockBigQuery);
    when(mockInsert.setProjectId(jobProjectId)).thenReturn(mockInsert);
    when(mockBigQueryJobs.insert(
        eq(jobProjectId), eq(getExpectedJob()), argThat(new IsNumRecords(0))))
        .thenReturn(mockInsert);
    when(mockInsert.execute()).thenReturn(jobReturn);

    // Get the RecordWriter.
    BigQueryRecordWriter<LongWritable, JsonObject> recordWriter = new BigQueryRecordWriter<>(
        mockFactory,
        new Configuration(),
        progressable,
        fields,
        jobProjectId,
        getSampleTableRef(),
        1000);

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Tear down verifies no calls to BigQuery, factory is invoked int he constructor, though.
    verify(mockFactory, times(1)).getBigQuery(any(Configuration.class));
  }

  /**
   * Tests the write method of BigQueryRecordWriter for multiple writes.
   */
  @Test
  public void testMultipleWrites() throws IOException, GeneralSecurityException {
    Insert mockInsert = mock(Insert.class);

    when(mockFactory.getBigQuery(any(Configuration.class))).thenReturn(mockBigQuery);
    when(mockInsert.setProjectId(jobProjectId)).thenReturn(mockInsert);
    when(mockBigQueryJobs.insert(
        eq(jobProjectId), eq(getExpectedJob()), argThat(new IsNumRecords(2))))
        .thenReturn(mockInsert);
    when(mockInsert.execute()).thenReturn(jobReturn);

    // Get the RecordWriter.
    BigQueryRecordWriter<LongWritable, JsonObject> recordWriter = new BigQueryRecordWriter<>(
        mockFactory,
        new Configuration(),
        progressable,
        fields,
        jobProjectId,
        getSampleTableRef(),
        1000);

    // Write the key, value pair.
    callWrite(recordWriter, 2);

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper call calls were to the BigQuery.
    verify(mockFactory, times(1)).getBigQuery(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), argThat(new IsNumRecords(2)));
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), any(String.class));
  }

  /**
   * Tests the batching of the write method of BigQueryRecordWriter.
   */
  @Test
  public void testBatchingSimple() throws IOException, GeneralSecurityException {
    Insert mockInsert = mock(Insert.class);

    when(mockFactory.getBigQuery(any(Configuration.class))).thenReturn(mockBigQuery);
    when(mockInsert.setProjectId(jobProjectId)).thenReturn(mockInsert);
    when(mockBigQueryJobs.insert(
        eq(jobProjectId), eq(getExpectedJob()), any(ByteArrayContent.class)))
        .thenReturn(mockInsert);
    when(mockInsert.execute()).thenReturn(jobReturn);

    // Get the RecordWriter.
    BigQueryRecordWriter<LongWritable, JsonObject> recordWriter = new BigQueryRecordWriter<>(
        mockFactory,
        new Configuration(),
        progressable,
        fields,
        jobProjectId,
        getSampleTableRef(),
        200);

    // Write the key, value pair.
    callWrite(recordWriter, 2);

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory, times(1)).getBigQuery(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockBigQueryJobs, times(1))
        .insert(eq(jobProjectId), eq(getExpectedJob()), argThat(new IsNumRecords(2)));
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), any(String.class));
  }

  /**
   * Tests the batching of the write method of BigQueryRecordWriter.
   */
  @Test
  public void testBatching() throws IOException, GeneralSecurityException {
    Insert mockInsert = mock(Insert.class);

    when(mockFactory.getBigQuery(any(Configuration.class))).thenReturn(mockBigQuery);
    when(mockInsert.setProjectId(jobProjectId)).thenReturn(mockInsert);
    when(mockBigQueryJobs.insert(
        eq(jobProjectId), eq(getExpectedJob()), any(ByteArrayContent.class)))
        .thenReturn(mockInsert);
    when(mockInsert.execute()).thenReturn(jobReturn);

    // Get the RecordWriter.
    BigQueryRecordWriter<LongWritable, JsonObject> recordWriter = new BigQueryRecordWriter<>(
        mockFactory,
        new Configuration(),
        progressable,
        fields,
        jobProjectId,
        getSampleTableRef(),
        250);
    // Get the RecordWriter.

    // Write the key, value pair.
    callWrite(recordWriter, 15);

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory, times(1)).getBigQuery(any(Configuration.class));
    verify(mockBigQuery, times(6)).jobs();
    verify(mockBigQueryJobs, times(2))
        .insert(eq(jobProjectId), eq(getExpectedJob()), argThat(new IsNumRecords(7)));
    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), argThat(new IsNumRecords(1)));
    verify(mockJobsGet, times(3)).execute();
    verify(mockBigQueryJobs, times(3)).get(eq(jobProjectId), any(String.class));
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
    loadConfig.setWriteDisposition("WRITE_APPEND");
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

    // Create Job reference.
    JobReference jobRef = new JobReference();
    jobRef.setProjectId(jobProjectId);

    // Set the output write job.
    Job expectedJob = new Job();
    expectedJob.setConfiguration(jobConfig);
    expectedJob.setJobReference(jobRef);
    return expectedJob;
  }

  /**
   * Helper method to get a ByteArrayContent of a numValues copies of jsonValue.
   *
   * @throws UnsupportedEncodingException if UTF-8 encoding not supported.
   */
  private ByteArrayContent getContents(int numValues) 
      throws UnsupportedEncodingException {
    StringBuilder outputRecords = new StringBuilder();
    for (int i = 0; i < numValues; i++) {
      outputRecords = outputRecords.append(gson.toJson(jsonValue)).append("\n");
    }
    return new ByteArrayContent(
        "application/octet-stream", outputRecords.toString().getBytes("UTF-8"));
  }

  /**
   * Helper class to check if ByteArrayContent has expected number and value for records.
   */
  private class IsNumRecords 
    extends ArgumentMatcher<ByteArrayContent> {
    private int numRecords;

    /**
     * Default constructor.
     */
    public IsNumRecords(int numRecords) {
      this.numRecords = numRecords;
    }

    /**
     * Checks if Object has expected number and value for records.
     */
    @Override
    public boolean matches(Object list) {
      try {
        return IOUtils.contentEquals(
            getContents(numRecords).getInputStream(), ((ByteArrayContent) list).getInputStream());
      } catch (IOException e) {
        log.debug("Error checking if output records match:", e);
        return false;
      }
    }
  }
}
