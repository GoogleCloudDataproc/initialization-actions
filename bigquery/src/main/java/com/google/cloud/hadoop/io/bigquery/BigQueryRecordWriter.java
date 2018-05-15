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

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.HadoopToStringUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BigQueryRecordWriter writes the job outputs to the BigQuery. Accepts key, value pairs but writes
 * only the value to BigQuery as the JsonObject value should already contain any necessary key
 * value.
 *
 *  While the key is currently discarded similar to behavior in DBRecordWriter, it is possible some
 * scenarios might arise where having the key written to Bigquery as well is more efficient and this
 * behavior might need to be modified accordingly.
 *
 * @param <K> Key type.
 * @param <V> Value type (must be JsonObject or a derived type).
 */
public class BigQueryRecordWriter<K, V extends JsonObject> extends RecordWriter<K, V> {

  // Logger.
  public static final Logger LOG = LoggerFactory.getLogger(BigQueryRecordWriter.class);

  // Holds collection of counters used by this instance.
  private static Counters counters = new Counters();

  // Passes through to newly-created BigQueryAsyncWriteChannel instances before calling
  // initialize on them.
  private ClientRequestHelper<Job> clientRequestHelper;

  private final Configuration configuration;
  private final Progressable progressable;

  // A Gson object used to get Json strings.
  private Gson gson;

  // Channel to which we will write records.
  private BigQueryAsyncWriteChannel byteChannel;

  // Thread pool to use for async write operations.
  private ExecutorService threadPool = Executors.newCachedThreadPool();

  // Used for specialized handling of various API-defined exceptions.
  private ApiErrorExtractor errorExtractor = ApiErrorExtractor.INSTANCE;

  // Count of total bytes written after serialization of records.
  private long bytesWritten = 0;

  /**
   * Defines names of counters we track for each operation.
   *
   *  There are two types of counters: -- METHOD_NAME : Number of successful invocations of method
   * METHOD. -- METHOD_NAME_TIME : Total inclusive time spent in method METHOD.
   */
  public enum Counter {
    BYTES_WRITTEN,
    CLOSE_CALLS,
    CLOSE_TOTAL_TIME,
    JOBS_INSERTED,
    WRITE_CALLS,
    WRITE_TOTAL_TIME,
  }

  /**
   * A channel for streaming results to BigQuery.
   */
  private class BigQueryAsyncWriteChannel
      extends AbstractGoogleAsyncWriteChannel<Insert, Job> {

    private final BigQueryHelper bigQueryHelper;
    private final Job job;
    private final String projectId;

    public BigQueryAsyncWriteChannel(
        BigQueryHelper bigQueryHelper,
        Job job,
        String projectId,
        AsyncWriteChannelOptions options) {
      super(threadPool, options);
      this.bigQueryHelper = bigQueryHelper;
      this.job = job;
      this.projectId = projectId;

      // Set the ClientRequestHelper from the outer class instance.
      setClientRequestHelper(clientRequestHelper);
    }

    @Override
    public Insert createRequest(InputStreamContent inputStream) throws IOException {
      Insert insert = bigQueryHelper.getRawBigquery().jobs().insert(projectId, job, inputStream);
      insert.setProjectId(projectId);
      increment(Counter.JOBS_INSERTED);
      return insert;
    }

    @Override
    public void handleResponse(Job response) throws IOException {
      bigQueryHelper.checkJobIdEquality(job, response);
      JobReference jobReference = response.getJobReference();
      // Check that job is completed.
      try {
        BigQueryUtils.waitForJobCompletion(
            bigQueryHelper.getRawBigquery(), projectId, jobReference, progressable);
      } catch (InterruptedException e) {
        LOG.error(e.getMessage());
        throw new IOException(e);
      }
    }

    @Override
    public Job createResponseFromException(IOException ioe) {
      if (errorExtractor.itemAlreadyExists(ioe)) {
        // For now, only wire through the JobReference since the handleResponse method only depends
        // on the JobReference. We should update this if we depend on other parts of the Job,
        // especially if there are any pieces which are liable to diverge from the Job resource
        // we're trying to insert vs the Job resource we're supposed to get back from the server.
        return new Job().setJobReference(job.getJobReference());
      } else {
        return null;
      }
    }
  }

  /**
   * Constructs an instance of BigQueryRecordWriter that writes to the table with ID tableId in the
   * dataset denoted by datasetId under the project denoted by projectId.
   *
   *  Records are formatted according to the schema described by fields.
   *
   * Writes are flushed to the given table in batches of size numRecordsInBatch.
   *
   * @param outputRecordSchema the schema describing the output records.
   * @param projectId the id of the project under which to perform the BigQuery operations.
   * @param tableRef the fully qualified reference to the (temp) table to write to; its projectId
   *     must be specified, and may or may not match the projectId which owns the BigQuery job.
   * @param writeBufferSize The size of the upload buffer to use.
   * @throws IOException on IOError.
   */
  public BigQueryRecordWriter(
      BigQueryFactory factory,
      ExecutorService threadPool,
      ClientRequestHelper<Job> clientRequestHelper,
      Configuration configuration,
      Progressable progressable,
      String taskIdentifier,
      List<TableFieldSchema> outputRecordSchema,
      String projectId,
      TableReference tableRef,
      int writeBufferSize) throws IOException {

    LOG.debug(
        "Initialize with projectId: '{}', tableRef: '{}', writeBufferSize: {}",
        projectId,
        BigQueryStrings.toString(tableRef),
        writeBufferSize);

    // Check Preconditions.
    Preconditions.checkArgument(
        outputRecordSchema != null, "outputRecordSchema should not be not null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId), "projectId should not be not null or empty.");
    Preconditions.checkArgument(
        tableRef != null, "tableRef must not be null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableRef.getProjectId()),
        "tableRef.getProjectId() should not be not null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableRef.getDatasetId()),
        "tableRef.getDatasetId() should not be not null or empty.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableRef.getTableId()),
        "tableRef.getTableId() should not be not null or empty.");
    Preconditions.checkArgument(writeBufferSize > 0, "numRecordsInBatch should be positive.");

    // Construct the Gson for parsing JsonObjects.
    gson = new Gson();
    this.configuration = configuration;
    this.progressable = progressable;
    this.threadPool = threadPool;
    this.clientRequestHelper = clientRequestHelper;

    // Get BigQuery.
    BigQueryHelper bigQueryHelper;
    try {
      bigQueryHelper = factory.getBigQueryHelper(configuration);
    } catch (GeneralSecurityException e) {
      LOG.error("Could not connect to BigQuery:", e);
      throw new IOException(e);
    }

    // Configure a write job. We must use WRITE_TRUNCATE even though we are writing to a temporary
    // table which should only contain records from this RecordWriter, since low-level retries may
    // otherwise result in duplicate "load" jobs which append the uploaded contents multiple times.
    // With WRITE_TRUNCATE, errant "load" jobs will simply overwritten atomically with the same
    // fully-uploaded contents.
    JobConfigurationLoad loadConfig = new JobConfigurationLoad();
    loadConfig.setCreateDisposition("CREATE_IF_NEEDED");
    loadConfig.setWriteDisposition("WRITE_TRUNCATE");
    loadConfig.setSourceFormat("NEWLINE_DELIMITED_JSON");

    // Describe the resulting table you are writing data to:
    loadConfig.setDestinationTable(tableRef);

    // Parse the output schema for Json from fields.
    TableSchema schema = new TableSchema();
    schema.setFields(outputRecordSchema);
    loadConfig.setSchema(schema);

    // Create Job configuration.
    JobConfiguration jobConfig = new JobConfiguration();
    jobConfig.setLoad(loadConfig);

    // Create Job reference.
    String location =
        configuration.get(
            BigQueryConfiguration.DATA_LOCATION_KEY, BigQueryConfiguration.DATA_LOCATION_DEFAULT);
    JobReference jobRef = bigQueryHelper.createJobReference(projectId, taskIdentifier, location);

    // Set the output write job.
    Job outputJob = new Job();
    outputJob.setConfiguration(jobConfig);
    outputJob.setJobReference(jobRef);

    byteChannel = createByteChannel(
        bigQueryHelper, outputJob, projectId, writeBufferSize);
  }

  /**
   * Constructs an instance of BigQueryRecordWriter that writes to the table with ID tableId in the
   * dataset denoted by datasetId under the project denoted by projectId.
   *
   *  Records are formatted according to the schema described by fields.
   *
   * Writes are flushed to the given table in batches of size numRecordsInBatch.
   *
   * @param configuration Configuration for the job / task
   * @param progressable Progressable to which we should report status
   * @param taskIdentifier A String to help identify actions performed by this RecordWriter,
   *     for example, a TaskAttemptID. Not required to be unique, but must match "[a-zA-Z0-9-_]+".
   * @param outputRecordSchema the schema describing the output records.
   * @param projectId the id of the project.
   * @param tableRef the fully qualified reference to the (temp) table to write to; its projectId
   *     must be specified, and may or may not match the projectId which owns the BigQuery job.
   * @param writeBufferSize The size of the upload buffer to use.
   * @throws IOException on IOError.
   */
  public BigQueryRecordWriter(
      Configuration configuration,
      Progressable progressable,
      String taskIdentifier,
      List<TableFieldSchema> outputRecordSchema,
      String projectId,
      TableReference tableRef,
      int writeBufferSize) throws IOException {
    this(
        new BigQueryFactory(),
        Executors.newCachedThreadPool(),
        new ClientRequestHelper<Job>(),
        configuration,
        progressable,
        taskIdentifier,
        outputRecordSchema,
        projectId,
        tableRef,
        writeBufferSize);
  }

  private BigQueryAsyncWriteChannel createByteChannel(
      BigQueryHelper bigQueryHelper,
      Job outputJob,
      String projectId,
      int writeBufferSize) throws IOException {
    if (configuration.getBoolean(
        BigQueryConfiguration.ENABLE_ASYNC_WRITE,
        BigQueryConfiguration.ENABLE_ASYNC_WRITE_DEFAULT)) {
      LOG.debug("Using asynchronous write channel.");
    } else {
      LOG.warn(
          "Got 'false' for obsolete key '{}', using asynchronous write channel anyway.",
          BigQueryConfiguration.ENABLE_ASYNC_WRITE);
    }
    AsyncWriteChannelOptions options = AsyncWriteChannelOptions
        .newBuilder()
        .setUploadBufferSize(writeBufferSize)
        .build();

    // TODO(user): Add threadpool configurations
    BigQueryAsyncWriteChannel channel = new BigQueryAsyncWriteChannel(
        bigQueryHelper,
        outputJob,
        projectId,
        options);

    channel.initialize();

    return channel;
  }

  /**
   * Writes a key/value pair.
   *
   * @param key the key to write.
   * @param value the value to write.
   * @throws IOException on IOError.
   */
  @Override
  public void write(K key, V value) throws IOException {
    long startTime = System.nanoTime();
    String stringValue = gson.toJson(value) + "\n";
    byte[] valueBytes = stringValue.getBytes(StandardCharsets.UTF_8);
    bytesWritten += valueBytes.length;
    byteChannel.write(ByteBuffer.wrap(valueBytes));

    long duration = System.nanoTime() - startTime;
    increment(Counter.BYTES_WRITTEN, valueBytes.length);
    increment(Counter.WRITE_CALLS);
    increment(Counter.WRITE_TOTAL_TIME, duration);
  }

  /**
   * Closes this RecordWriter to future operations.
   *
   * @param context the context of the task.
   * @throws IOException on IOError.
   */
  @Override
  public void close(TaskAttemptContext context) throws IOException {
    long startTime = System.nanoTime();
    if (LOG.isDebugEnabled()) {
      LOG.debug("close({})", HadoopToStringUtil.toString(context));
    }
    threadPool.shutdown();
    byteChannel.close();

    long duration = System.nanoTime() - startTime;
    increment(Counter.CLOSE_CALLS);
    increment(Counter.CLOSE_TOTAL_TIME, duration);
  }

  /**
   * Returns the total number of bytes written so far.
   */
  public long getBytesWritten() {
    return bytesWritten;
  }

  @VisibleForTesting
  void setErrorExtractor(ApiErrorExtractor errorExtractor) {
    this.errorExtractor = errorExtractor;
  }

  /**
   * Increments by 1 the counter indicated by key.
   */
  static void increment(Counter key) {
    increment(key, 1);
  }

  /**
   * Adds value to the counter indicated by key.
   */
  static void increment(Counter key, long value) {
    counters.incrCounter(key, value);
  }

  /**
   * Gets value of all counters as a formatted string.
   */
  public static String countersToString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    double numNanoSecPerMS = TimeUnit.MILLISECONDS.toNanos(1);

    String callsSuffix = "_CALLS";
    int callsLength = callsSuffix.length();
    String timeSuffix = "_TIME";
    String totalTimeSuffix = "_TOTAL" + timeSuffix;
    String avgTimeSuffix = "_AVG" + timeSuffix;

    for (Counter c : Counter.values()) {
      String name = c.toString();
      if (name.endsWith(callsSuffix)) {
        String prefix = name.substring(0, name.length() - callsLength);

        // Log invocation counter.
        long count = counters.getCounter(c);
        sb.append(String.format("%20s = %d\n", name, count));

        // Log duration counter.
        String timeName = prefix + totalTimeSuffix;
        double totalTime =
            counters.getCounter(Enum.valueOf(Counter.class, timeName)) / numNanoSecPerMS;
        sb.append(String.format("%20s = %.2f (ms)\n", timeName, totalTime));

        // Compute and log average duration per call (== total duration / num invocations).
        String avgName = prefix + avgTimeSuffix;
        double avg = totalTime / count;
        sb.append(String.format("%20s = %.2f (ms)\n", avgName, avg));
      } else if (!name.endsWith(timeSuffix)) {
        // Log invocation counter.
        long count = counters.getCounter(c);
        sb.append(String.format("%20s = %d\n", name, count));
      }
    }
    return sb.toString();
  }

  /**
   * Logs values of all counters.
   */
  static void logCounters() {
    LOG.debug(countersToString());
  }

}
