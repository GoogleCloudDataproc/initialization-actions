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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.expectThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

/**
 * Unit tests for BigQueryRecordWriter.
 */
@RunWith(JUnit4.class)
public class BigQueryRecordWriterTest {

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

  // Executor that is used by the other end of the pipe within the channel.
  private ExecutorService executorService;

  @Mock private Insert mockInsert;
  @Mock private BigQueryFactory mockFactory;
  @Mock private Bigquery mockBigQuery;
  @Mock private BigQueryHelper mockBigQueryHelper;
  @Mock private TaskAttemptContext mockContext;
  @Mock private Bigquery.Jobs mockBigQueryJobs;
  @Mock private Bigquery.Jobs.Get mockJobsGet;
  @Mock private Progressable progressable;
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

    // Calls the .run/.call method on the future when the future is first accessed.
    executorService = Executors.newCachedThreadPool();

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
  }

  private void initializeRecordWriter() throws IOException {
    when(mockBigQueryHelper.createJobReference(any(String.class), any(String.class)))
        .thenReturn(jobReference);
    recordWriter = new BigQueryRecordWriter<>(
        mockFactory,
        executorService,
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
    final ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    final byte[] readData = new byte[4096];
    final CountDownLatch waitTillWritesAreDoneLatch = new CountDownLatch(1);
    when(mockInsert.execute())
        .thenAnswer(new Answer<Job>() {
          @Override
          public Job answer(InvocationOnMock unused) throws Throwable {
            // We want to make sure we have a consistent update of the read data which is
            // why we synchronize on it.
            synchronized (readData) {
              waitTillWritesAreDoneLatch.await();
              inputStreamCaptor.getValue().getInputStream()
                .read(readData, 0, (int) recordWriter.getBytesWritten());
            }
            return jobReturn;
          }
        });

    initializeRecordWriter();

    // Write the key, value pair.
    callWrite(recordWriter, 1);

    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), inputStreamCaptor.capture());

    // The writes are now done, we can return from the execute method.
    // This is an issue with how PipedInputStream functions since it checks
    // to see if the sending and receiving threads are alive.
    waitTillWritesAreDoneLatch.countDown();

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), eq(jobReference.getJobId()));
    assertThat(executorService.isShutdown()).isTrue();

    // We want to make sure we have a consistent view of the read data which is
    // why we synchronize on it.
    synchronized (readData) {
      String readDataString = new String(
          Arrays.copyOfRange(readData, 0, (int) recordWriter.getBytesWritten()),
          StandardCharsets.UTF_8);
      assertThat(readDataString).isEqualTo("{\"Name\":\"test name\",\"Number\":\"123\"}\n");
    }
  }

  /**
   * Tests the write method of BigQueryRecordWriter when nothing is written.
   */
  @Test
  public void testNoWrites() throws IOException, GeneralSecurityException {
    final ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    final byte[] readData = new byte[4096];
    final CountDownLatch waitTillWritesAreDoneLatch = new CountDownLatch(1);
    when(mockInsert.execute())
        .thenAnswer(new Answer<Job>() {
          @Override
          public Job answer(InvocationOnMock unused) throws Throwable {
            // We want to make sure we have a consistent update of the read data which is
            // why we synchronize on it.
            synchronized (readData) {
              waitTillWritesAreDoneLatch.await();
              inputStreamCaptor.getValue().getInputStream()
                .read(readData, 0, (int) recordWriter.getBytesWritten());
            }
            return jobReturn;
          }
        });

    initializeRecordWriter();

    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), inputStreamCaptor.capture());

    // The writes are now done, we can return from the execute method.
    // This is an issue with how PipedInputStream functions since it checks
    // to see if the sending and receiving threads are alive.
    waitTillWritesAreDoneLatch.countDown();

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), eq(jobReference.getJobId()));
    assertThat(executorService.isShutdown()).isTrue();

    // We want to make sure we have a consistent view of the read data which is
    // why we synchronize on it.
    synchronized (readData) {
      String readDataString = new String(
          Arrays.copyOfRange(readData, 0, (int) recordWriter.getBytesWritten()),
          StandardCharsets.UTF_8);
      assertThat(readDataString).isEmpty();
    }
  }

  /**
   * Tests the write method of BigQueryRecordWriter without writing anything but throwing a 409
   * conflict from the job-insertion.
   */
  @Test
  public void testConflictExceptionOnCreate() throws IOException, GeneralSecurityException {
    IOException fakeConflictException = new IOException("fake 409 conflict");
    when(mockInsert.execute())
        .thenThrow(fakeConflictException);
    when(mockErrorExtractor.itemAlreadyExists(any(IOException.class)))
        .thenReturn(true);

    initializeRecordWriter();

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), eq(jobReference.getJobId()));
    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), any(AbstractInputStreamContent.class));
    assertThat(executorService.isShutdown()).isTrue();
  }

  /**
   * Tests the write method of BigQueryRecordWriter without writing anything but throwing an
   * unhandled exeption from the job-insertion.
   */
  @Test
  public void testUnhandledExceptionOnCreate() throws IOException, GeneralSecurityException {
    IOException fakeUnhandledException = new IOException("fake unhandled exception");
    when(mockInsert.execute())
        .thenThrow(fakeUnhandledException);
    when(mockErrorExtractor.itemAlreadyExists(any(IOException.class)))
        .thenReturn(false);

    initializeRecordWriter();

    // Close the RecordWriter; the stored exception finally propagates out.
    IOException ioe = expectThrows(IOException.class, () -> recordWriter.close(mockContext));
    assertThat(ioe).hasCauseThat().isEqualTo(fakeUnhandledException);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(1)).jobs();
    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), any(AbstractInputStreamContent.class));
    assertThat(executorService.isShutdown()).isTrue();
  }

  @Test
  public void testMultipleWrites() throws IOException, GeneralSecurityException {
    final ArgumentCaptor<AbstractInputStreamContent> inputStreamCaptor =
        ArgumentCaptor.forClass(AbstractInputStreamContent.class);
    final byte[] readData = new byte[4096];
    final CountDownLatch waitTillWritesAreDoneLatch = new CountDownLatch(1);
    when(mockInsert.execute())
        .thenAnswer(new Answer<Job>() {
          @Override
          public Job answer(InvocationOnMock unused) throws Throwable {
            // We want to make sure we have a consistent update of the read data which is
            // why we synchronize on it.
            synchronized (readData) {
              waitTillWritesAreDoneLatch.await();
              inputStreamCaptor.getValue().getInputStream()
                .read(readData, 0, (int) recordWriter.getBytesWritten());
            }
            return jobReturn;
          }
        });

    initializeRecordWriter();

    // Write the key, value pair.
    callWrite(recordWriter, 2);

    verify(mockBigQueryJobs).insert(
        eq(jobProjectId), eq(getExpectedJob()), inputStreamCaptor.capture());

    // The writes are now done, we can return from the execute method.
    // This is an issue with how PipedInputStream functions since it checks
    // to see if the sending and receiving threads are alive.
    waitTillWritesAreDoneLatch.countDown();

    // Close the RecordWriter.
    recordWriter.close(mockContext);

    // Check that the proper calls were sent to the BigQuery.
    verify(mockFactory).getBigQueryHelper(any(Configuration.class));
    verify(mockBigQuery, times(2)).jobs();
    verify(mockJobsGet, times(1)).execute();
    verify(mockBigQueryJobs, times(1)).get(eq(jobProjectId), eq(jobReference.getJobId()));
    assertThat(executorService.isShutdown()).isTrue();

    // We want to make sure we have a consistent view of the read data which is
    // why we synchronize on it.
    synchronized (readData) {
      String readDataString = new String(
          Arrays.copyOfRange(readData, 0, (int) recordWriter.getBytesWritten()),
          StandardCharsets.UTF_8);
      assertThat(readDataString)
          .isEqualTo(
              "{\"Name\":\"test name\",\"Number\":\"123\"}\n"
                  + "{\"Name\":\"test name\",\"Number\":\"123\"}\n");
    }
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
