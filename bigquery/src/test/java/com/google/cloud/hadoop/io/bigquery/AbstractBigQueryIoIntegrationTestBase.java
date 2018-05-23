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

import static com.google.cloud.hadoop.io.bigquery.BigQueryFactory.BIGQUERY_CONFIG_PREFIX;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Integration tests covering both InputFormat and OutputFormat functionality of the BigQuery
 * connector libraries.
 */
public abstract class AbstractBigQueryIoIntegrationTestBase<T> {
  // Environment variable name for the projectId with which we will run this test.
  public static final String BIGQUERY_PROJECT_ID_ENVVARNAME = "BIGQUERY_PROJECT_ID";
  // Environment variable name to enable async/resumable writes.
  public static final String BIGQUERY_ENABLE_ASYNC_WRITE_ENVVARNAME =
      "ENABLE_BIGQUERY_ASYNC_WRITES";
  protected static final String MARKET_CAP_FIELD_NAME = "MarketCap";
  protected static final String COMPANY_NAME_FIELD_NAME = "CompanyName";

  // Logger.
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(AbstractBigQueryIoIntegrationTestBase.class);

  // Populated by command-line projectId and falls back to env.
  private String projectIdvalue;

  private TestBucketHelper bucketHelper;

  // DatasetId derived from testId; same for all test methods.
  private String testDataset;

  // Bucket name derived from testId, shared between all test methods.
  private String testBucket;

  // Instance of Bigquery API hook to use during test setup/teardown.
  private Bigquery bigqueryInstance;

  // Configuration object for passing settings through to the connector.
  private Configuration config;

  // Only use mocks to redirect the IO classes to grabbing our fake Configuration object. They
  // basically only use the task/job contexts to retrieve the configuration values.
  @Mock private TaskAttemptContext mockTaskAttemptContext;
  @Mock private JobContext mockJobContext;

  // The InputFormat and OutputFormat handles with which we will invoke the underlying "connector"
  // library methods.
  private InputFormat inputFormat;
  private OutputFormat outputFormat;

  // TableId derived from testId, a unique one should be used for each test method.
  private String testTable;

  /** True to enable using an AsyncWriteChannel. */
  public boolean enableAsyncWrites;

  public AbstractBigQueryIoIntegrationTestBase(Boolean enableAsyncWrites, InputFormat inputFormat) {
    this.inputFormat = inputFormat;
    this.enableAsyncWrites = enableAsyncWrites;
  }

  /**
   * Read the current value from the given record reader and return record fields in a Map.
   */
  protected abstract Map<String, Object> readReacord(RecordReader<?, T> recordReader)
      throws IOException, InterruptedException;

  /**
   * Helper method for grabbing service-account email and private keyfile name based on settings
   * intended for BigQueryFactory and adding them as GCS-equivalent credential settings.
   */
  public static Configuration getConfigForGcsFromBigquerySettings(
      String projectIdvalue, String testBucket) {
    TestConfiguration configuration = TestConfiguration.getInstance();

    String bigqueryServiceAccount = configuration.getServiceAccount();
    if (Strings.isNullOrEmpty(bigqueryServiceAccount)) {
      bigqueryServiceAccount = System.getenv(BigQueryFactory.BIGQUERY_SERVICE_ACCOUNT);
    }

    String bigqueryPrivateKeyFile = configuration.getPrivateKeyFile();
    if (Strings.isNullOrEmpty(bigqueryPrivateKeyFile)) {
      bigqueryPrivateKeyFile = System.getenv(BigQueryFactory.BIGQUERY_PRIVATE_KEY_FILE);
    }
    Configuration config = new Configuration();
    config.set(
        BIGQUERY_CONFIG_PREFIX + HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX,
        "true");
    config.set(
        BIGQUERY_CONFIG_PREFIX + HadoopCredentialConfiguration.SERVICE_ACCOUNT_EMAIL_SUFFIX,
        bigqueryServiceAccount);
    config.set(
        BIGQUERY_CONFIG_PREFIX + HadoopCredentialConfiguration.SERVICE_ACCOUNT_KEYFILE_SUFFIX,
        bigqueryPrivateKeyFile);
    config.set(GoogleHadoopFileSystemBase.SERVICE_ACCOUNT_AUTH_KEYFILE_KEY, bigqueryPrivateKeyFile);
    config.set(GoogleHadoopFileSystemBase.SERVICE_ACCOUNT_AUTH_EMAIL_KEY, bigqueryServiceAccount);
    config.set(GoogleHadoopFileSystemBase.GCS_PROJECT_ID_KEY, projectIdvalue);
    config.set(GoogleHadoopFileSystemBase.GCS_SYSTEM_BUCKET_KEY, testBucket);

    config.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    return config;
  }

  private void setConfigForGcsFromBigquerySettings() {
    Configuration conf = getConfigForGcsFromBigquerySettings(projectIdvalue, testBucket);
    for (Entry<String, String> entry : conf) {
      config.set(entry.getKey(), entry.getValue());
    }
    config.setBoolean(BigQueryConfiguration.ENABLE_ASYNC_WRITE, enableAsyncWrites);
  }

  @Before
  public void setUp()
      throws IOException, GeneralSecurityException {
    MockitoAnnotations.initMocks(this);

    TestConfiguration configuration = TestConfiguration.getInstance();

    Logger.getLogger(GsonBigQueryInputFormat.class).setLevel(Level.DEBUG);
    Logger.getLogger(BigQueryOutputCommitter.class).setLevel(Level.DEBUG);
    Logger.getLogger(BigQueryOutputFormat.class).setLevel(Level.DEBUG);
    Logger.getLogger(BigQueryRecordWriter.class).setLevel(Level.DEBUG);
    Logger.getLogger(BigQueryUtils.class).setLevel(Level.DEBUG);
    Logger.getLogger(GsonRecordReader.class).setLevel(Level.DEBUG);

    bucketHelper = new TestBucketHelper("bq_integration_test");
    // A unique per-setUp String to avoid collisions between test runs.
    String testId = bucketHelper.getUniqueBucketPrefix();

    projectIdvalue = configuration.getProjectId();
    if (Strings.isNullOrEmpty(projectIdvalue)) {
      projectIdvalue = System.getenv(BIGQUERY_PROJECT_ID_ENVVARNAME);
    }

    checkArgument(
        !Strings.isNullOrEmpty(projectIdvalue), "Must provide %s", BIGQUERY_PROJECT_ID_ENVVARNAME);
    testDataset = testId + "_dataset";
    testBucket = testId + "_bucket";

    // We have to create the output dataset ourselves.
    // TODO(user): Extract dataset creation into a library which is also used by
    // BigQueryOutputCommitter.
    Dataset outputDataset = new Dataset();
    DatasetReference datasetReference = new DatasetReference();
    datasetReference.setProjectId(projectIdvalue);
    datasetReference.setDatasetId(testDataset);

    config = getConfigForGcsFromBigquerySettings(projectIdvalue, testBucket);
    BigQueryFactory factory = new BigQueryFactory();
    bigqueryInstance = factory.getBigQuery(config);

    Bigquery.Datasets datasets = bigqueryInstance.datasets();
    outputDataset.setDatasetReference(datasetReference);
    LOG.info("Creating temporary dataset '{}' for project '{}'", testDataset, projectIdvalue);
    datasets.insert(projectIdvalue, outputDataset).execute();

    Path toCreate = new Path(String.format("gs://%s", testBucket));
    FileSystem fs = toCreate.getFileSystem(config);
    LOG.info("Creating temporary test bucket '{}'", toCreate);
    fs.mkdirs(toCreate);

    // Since the TaskAttemptContext and JobContexts are mostly used just to access a
    // "Configuration" object, we'll mock the two contexts to just return our fake configuration
    // object with which we'll provide the settings we want to test.
    config.clear();
    setConfigForGcsFromBigquerySettings();

    when(mockTaskAttemptContext.getConfiguration())
        .thenReturn(config);
    when(mockJobContext.getConfiguration())
        .thenReturn(config);

    // Have a realistic-looking fake TaskAttemptID.
    int taskNumber = 3;
    int taskAttempt = 2;
    int jobNumber = 42;
    String jobIdString = "jobid" + System.currentTimeMillis();
    JobID jobId = new JobID(jobIdString, jobNumber);
    TaskAttemptID taskAttemptId =
        new TaskAttemptID(new TaskID(jobId, false, taskNumber), taskAttempt);
    when(mockTaskAttemptContext.getTaskAttemptID())
        .thenReturn(taskAttemptId);
    when(mockJobContext.getJobID()).thenReturn(jobId);

    testTable = testId + "_table_" + jobIdString;

    // Instantiate an OutputFormat and InputFormat instance.
    outputFormat = new BigQueryOutputFormat();
  }

  @After
  public void tearDown() throws IOException {
    // Delete the test dataset along with all tables inside it.
    // TODO(user): Move this into library shared by BigQueryOutputCommitter.
    Bigquery.Datasets datasets = bigqueryInstance.datasets();
    LOG.info("Deleting temporary test dataset '{}' for project '{}'", testDataset, projectIdvalue);
    datasets.delete(projectIdvalue, testDataset).setDeleteContents(true).execute();

    // Recursively delete the testBucket.
    setConfigForGcsFromBigquerySettings();
    Path toDelete = new Path(String.format("gs://%s", testBucket));
    FileSystem fs = toDelete.getFileSystem(config);
    if (fs instanceof GoogleHadoopFileSystemBase) {
      bucketHelper.cleanup(((GoogleHadoopFileSystemBase) fs).getGcsFs().getGcs());
    } else {
      LOG.info("Deleting temporary test bucket '{}'", toDelete);
      fs.delete(toDelete, true);
    }
  }

  @Test
  public void testBasicWriteAndRead()
      throws IOException, InterruptedException {
    // Prepare the output settings.
    BigQueryConfiguration.configureBigQueryOutput(
        config, projectIdvalue, testDataset, testTable,
        "[{'name': 'CompanyName','type': 'STRING'},{'name': 'MarketCap','type': 'INTEGER'}]");

    // TODO(user): This shouldn't be necessary, but it's a bug where output committer assumes
    // the inputformat was also BQ and thus tries to find a GCS path to delete. We should make
    // the input and output independent from each other.
    config.setBoolean(BigQueryConfiguration.DELETE_INTERMEDIATE_TABLE_KEY, false);

    // First, obtain the "committer" and call the "setup" methods which are expected to create
    // the temporary dataset.
    OutputCommitter committer = outputFormat.getOutputCommitter(mockTaskAttemptContext);
    committer.setupJob(mockJobContext);
    committer.setupTask(mockTaskAttemptContext);

    // Write some data records into the bare RecordWriter interface.
    RecordWriter<Text, JsonObject> writer = outputFormat.getRecordWriter(mockTaskAttemptContext);
    JsonObject value = new JsonObject();
    value.addProperty(COMPANY_NAME_FIELD_NAME, "Google");
    value.addProperty(MARKET_CAP_FIELD_NAME, 409);
    writer.write(new Text("unused"), value);
    value = new JsonObject();
    value.addProperty(COMPANY_NAME_FIELD_NAME, "Microsoft");
    value.addProperty(MARKET_CAP_FIELD_NAME, 314);
    writer.write(new Text("unused"), value);
    value = new JsonObject();
    value.addProperty(COMPANY_NAME_FIELD_NAME, "Facebook");
    value.addProperty(MARKET_CAP_FIELD_NAME, 175);
    writer.write(new Text("unused"), value);

    // Calling close should flush the data in a new load job request.
    writer.close(mockTaskAttemptContext);

    // Run the "commit" methods in order of task, then job. These should copy from the temporary
    // table into the final destination.
    assertThat(committer.needsTaskCommit(mockTaskAttemptContext)).isTrue();
    committer.commitTask(mockTaskAttemptContext);
    committer.commitJob(mockJobContext);

    // Clear the config before preparing the input settings to ensure we're not relying on an
    // unexpected carryover of a config value from the output settings; input and output
    // should each be able to operate fully independently.
    // Set up the InputFormat to do a direct read of a table; no "query" or temporary extra table.
    config.clear();
    setConfigForGcsFromBigquerySettings();
    BigQueryConfiguration.configureBigQueryInput(
        config, projectIdvalue, testDataset, testTable);
    config.set(BigQueryConfiguration.GCS_BUCKET_KEY, testBucket);

    // Invoke the export/read flow by calling getSplits and createRecordReader.
    List<InputSplit> splits = inputFormat.getSplits(mockJobContext);
    RecordReader<?, T> reader =
        inputFormat.createRecordReader(splits.get(0), mockTaskAttemptContext);

    reader.initialize(splits.get(0), mockTaskAttemptContext);
    // Place the read values into a map since they may arrive in any order.
    Map<String, Integer> readValues = Maps.newHashMap();
    while (reader.nextKeyValue()) {
      Map<String, Object> record = readReacord(reader);
      assertThat(record).containsKey(COMPANY_NAME_FIELD_NAME);
      assertThat(record).containsKey(MARKET_CAP_FIELD_NAME);
      readValues.put(
          (String) record.get(COMPANY_NAME_FIELD_NAME),
          (int) record.get(MARKET_CAP_FIELD_NAME));
    }
    assertThat(readValues).hasSize(3);
    assertThat(readValues.get("Google")).isEqualTo(409);
    assertThat(readValues.get("Microsoft")).isEqualTo(314);
    assertThat(readValues.get("Facebook")).isEqualTo(175);
  }
}
