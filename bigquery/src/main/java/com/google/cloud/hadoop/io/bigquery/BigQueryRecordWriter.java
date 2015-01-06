package com.google.cloud.hadoop.io.bigquery;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.HadoopToStringUtil;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
  public static final LogUtil log = new LogUtil(BigQueryRecordWriter.class);

  // Holds collection of counters used by this instance.
  private static Counters counters = new Counters();
  private final Configuration configuration;
  private final Progressable progressable;

  // A Gson object used to get Json strings.
  private Gson gson;

  // Channel to which we will write records.
  private WritableByteChannel byteChannel;

  // Thread pool to use for async write operations.
  private ExecutorService threadPool = Executors.newCachedThreadPool();

  /**
   * Defines names of counters we track for each operation.
   *
   *  There are two types of counters: -- METHOD_NAME : Number of successful invocations of method
   * METHOD. -- METHOD_NAME_TIME : Total inclusive time spent in method METHOD.
   */
  public enum Counter {
    WRITE_CALLS,
    WRITE_TOTAL_TIME,
    CLOSE_CALLS,
    CLOSE_TOTAL_TIME,
    JOBS_INSERTED,
  }

  /**
   * A channel that implements synchronous, batched writes to BigQuery.
   */
  public static class BigQueryBatchedWriteChannel implements WritableByteChannel {
    // Logger.
    protected static final LogUtil log = new LogUtil(BigQueryRecordWriter.class);

    public static final double OVERALLOCATION_FACTOR = 1.25;
    private final Configuration configuration;
    private final Progressable progressable;

    private final Bigquery bigQuery;
    private final int batchSizeInBytes;
    private final Job outputJob;
    private final String projectId;

    // Set numRecordsCached and outputRecords.
    int numBytesCached = 0;
    int numBytesWritten = 0;
    ByteArrayOutputStream outputStream;
    boolean isOpen = true;

    /**
     * Construct a new BigQueryBatchedWriteChannel.
     *
     * @param configuration Job and task configuration
     * @param progressable Object to provide progress updates to
     * @param bigQuery The BigQuery instance to use
     * @param batchSizeInBytes The number of bytes to buffer before flushing to BigQuery
     * @param outputJob The Job corresponding to this load into BQ
     * @param projectId The Project ID for this export
     */
    public BigQueryBatchedWriteChannel(
        Configuration configuration,
        Progressable progressable,
        Bigquery bigQuery,
        int batchSizeInBytes,
        Job outputJob,
        String projectId) {
      this.configuration = configuration;
      this.progressable = progressable;
      this.outputStream =
          new ByteArrayOutputStream((int) (batchSizeInBytes * OVERALLOCATION_FACTOR));
      this.bigQuery = bigQuery;
      this.batchSizeInBytes = batchSizeInBytes;
      this.outputJob = outputJob;
      this.projectId = projectId;
    }

    private void throwIfNotOpen() throws ClosedChannelException {
      if (!isOpen()) {
        throw new ClosedChannelException();
      }
    }

    /**
     * Write a buffer containing a complete record to this channel.
     *
     * It is assumed that it is  safe to call flush after any call to write().
     */
    @Override
    public synchronized int write(ByteBuffer src) throws IOException {
      throwIfNotOpen();

      byte[] toWrite = new byte[src.remaining()];
      src.get(toWrite);
      outputStream.write(toWrite);
      numBytesCached += toWrite.length;
      if (numBytesCached >= batchSizeInBytes) {
        flush();
      }

      return toWrite.length;
    }

    /**
     * Flush all buffered writes to BQ.
     */
    public void flush() throws IOException {
      log.debug("Writing a batch of %d bytes to %s", numBytesCached, projectId);
      if (numBytesCached > 0) {
        // Get ByteArray of output records.
        ByteArrayContent contents =
            new ByteArrayContent("application/octet-stream", outputStream.toByteArray());

        // Run Bigquery load job.
        Insert insert = bigQuery.jobs().insert(projectId, outputJob, contents);
        insert.setProjectId(projectId);
        increment(Counter.JOBS_INSERTED);
        JobReference jobId = insert.execute().getJobReference();

        // Check that job is completed.
        try {
          BigQueryUtils.waitForJobCompletion(bigQuery, projectId, jobId, progressable);
        } catch (InterruptedException e) {
          log.error(e.getMessage());
          throw new IOException(e);
        }

        outputStream.reset();
        // Reset counts.
        numBytesWritten += numBytesCached;
        numBytesCached = 0;
      }
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

    @Override
    public void close() throws IOException {
      throwIfNotOpen();
      flush();
      isOpen = false;
    }
  }

  /**
   * A channel for streaming results to BigQuery.
   */
  public static class BigQueryAsyncWriteChannel
      extends AbstractGoogleAsyncWriteChannel<Insert, Job> {

    private final Configuration configuration;
    private final Progressable progressable;
    private final Bigquery bigQuery;
    private final Job job;
    private final String projectId;

    public BigQueryAsyncWriteChannel(
        Configuration configuration,
        Progressable progressable,
        ExecutorService threadPool,
        Bigquery bigQuery,
        Job job,
        String projectId,
        AsyncWriteChannelOptions options) {
      super(threadPool, options);
      this.configuration = configuration;
      this.progressable = progressable;
      this.bigQuery = bigQuery;
      this.job = job;
      this.projectId = projectId;
    }

    @Override
    public Insert createRequest(InputStreamContent inputStream) throws IOException {
      Insert insert = bigQuery.jobs().insert(projectId, job, inputStream);
      insert.setProjectId(projectId);
      increment(Counter.JOBS_INSERTED);
      return insert;
    }

    @Override
    public void handleResponse(Job response) throws IOException {
      JobReference jobReference = response.getJobReference();
      // Check that job is completed.
      try {
        BigQueryUtils.waitForJobCompletion(bigQuery, projectId, jobReference, progressable);
      } catch (InterruptedException e) {
        log.error(e.getMessage());
        throw new IOException(e);
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
      Configuration configuration,
      Progressable progressable,
      List<TableFieldSchema> outputRecordSchema,
      String projectId,
      TableReference tableRef,
      int writeBufferSize) throws IOException {

    log.debug("Intialize with projectId: '%s', tableRef: '%s', writeBufferSize: %d",
        projectId, BigQueryStrings.toString(tableRef), writeBufferSize);

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

    // Get BigQuery.
    Bigquery bigquery;
    try {
      bigquery = factory.getBigQuery(configuration);
    } catch (GeneralSecurityException e) {
      log.error("Could not connect to BigQuery:", e);
      throw new IOException(e);
    }

    // Configure a write job.
    JobConfigurationLoad loadConfig = new JobConfigurationLoad();
    loadConfig.setCreateDisposition("CREATE_IF_NEEDED");
    loadConfig.setWriteDisposition("WRITE_APPEND");
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
    JobReference jobRef = new JobReference();
    jobRef.setProjectId(projectId);

    // Set the output write job.
    Job outputJob = new Job();
    outputJob.setConfiguration(jobConfig);
    outputJob.setJobReference(jobRef);

    byteChannel = createByteChannel(
        configuration, progressable, bigquery, outputJob, projectId, writeBufferSize);
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
      List<TableFieldSchema> outputRecordSchema,
      String projectId,
      TableReference tableRef,
      int writeBufferSize) throws IOException {
    this(
        new BigQueryFactory(),
        configuration,
        progressable,
        outputRecordSchema,
        projectId,
        tableRef,
        writeBufferSize);
  }

  private WritableByteChannel createByteChannel(
      Configuration configuration,
      Progressable progressable,
      Bigquery bigquery,
      Job outputJob,
      String projectId,
      int writeBufferSize) throws IOException {
    if (configuration.getBoolean(
        BigQueryConfiguration.ENABLE_ASYNC_WRITE,
        BigQueryConfiguration.ENABLE_ASYNC_WRITE_DEFAULT)) {
      log.debug("Using asynchronous write channel.");

      AsyncWriteChannelOptions options = AsyncWriteChannelOptions
          .newBuilder()
          .setUploadBufferSize(writeBufferSize)
          .build();

      // TODO(user): Add threadpool configurations
      BigQueryAsyncWriteChannel channel = new BigQueryAsyncWriteChannel(
          configuration,
          progressable,
          threadPool,
          bigquery,
          outputJob,
          projectId,
          options);

      channel.initialize();

      return channel;
    } else {
      log.debug("Using synchronous write channel.");

      return new BigQueryBatchedWriteChannel(
          configuration,
          progressable,
          bigquery,
          writeBufferSize,
          outputJob,
          projectId);
    }
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
    byteChannel.write(ByteBuffer.wrap(stringValue.getBytes(StandardCharsets.UTF_8)));

    long duration = System.nanoTime() - startTime;
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
    if (log.isDebugEnabled()) {
      log.debug("close(%s)", HadoopToStringUtil.toString(context));
    }
    threadPool.shutdown();
    byteChannel.close();

    long duration = System.nanoTime() - startTime;
    increment(Counter.CLOSE_CALLS);
    increment(Counter.CLOSE_TOTAL_TIME, duration);
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
    log.debug(countersToString());
  }

}
