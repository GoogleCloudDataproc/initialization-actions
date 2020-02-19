package com.google.cloud.hadoop.io.bigquery;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.HTTP_TRANSPORT;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageTestUtils.JSON_FACTORY;
import static com.google.cloud.hadoop.io.bigquery.BigQueryFactory.BQC_ID;
import static org.junit.Assume.assumeFalse;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystemIntegrationHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper;
import com.google.cloud.hadoop.gcsio.integration.GoogleCloudStorageTestHelper.TestBucketHelper;
import com.google.cloud.hadoop.gcsio.testing.TestConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An example Hadoop WordCount program that counts the number of times a word appears in a BigQuery
 * table column.
 */
@RunWith(JUnit4.class)
public class RegionalIntegrationTest {

  private static final String RANDOM_STRING = UUID.randomUUID().toString().substring(0, 4);
  private static final String LOCATION = "asia-northeast1";
  private static final String TEST_BUCKET_PREFIX = "bq_integration_test";
  private static final String PROJECT_ID = TestConfiguration.getInstance().getProjectId();
  private static final String DATASET_ID =
      (TEST_BUCKET_PREFIX + "_" + LOCATION + "_" + RANDOM_STRING).replace('-', '_');
  private static final String QUALIFIED_TABLE_ID_FMT = "%s:%s.%s";

  private Configuration conf;

  private Bigquery bigquery;
  private GoogleCloudStorage gcs;
  private TestBucketHelper bucketHelper;

  private Job job;

  /**
   * This is the mapper for our WordCount job. For input, it consumes a LongWritable and JsonObject
   * as the key and value. These correspond to a row identifier and Json representation of the row's
   * values/columns. For output, it produces Text and a LongWritable as the key and value. these
   * correspond to the word and a count for the number of times it has occurred.
   */
  public static class Map extends Mapper<LongWritable, JsonObject, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1);

    private final Text word = new Text();

    @Override
    public void map(
        LongWritable key,
        JsonObject value,
        Mapper<LongWritable, JsonObject, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {
      JsonElement countElement = value.get("word");
      if (countElement != null) {
        String wordInRecord = countElement.getAsString();
        word.set(wordInRecord);
        context.write(word, ONE);
      }
    }
  }

  /**
   * This is the reducer for our WordCount job. For input, it consumes the Text and LongWritable
   * that the mapper produced. For output, it produces a JsonObject and NullWritable. The JsonObject
   * represents the data that will be loaded into BigQuery.
   */
  public static class Reduce extends Reducer<Text, LongWritable, JsonObject, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      // Add up the values to get a total number of occurrences of our word.
      long count = 0;
      for (LongWritable val : values) {
        count += val.get();
      }

      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("Word", key.toString());
      jsonObject.addProperty("Count", count);
      context.write(jsonObject, NullWritable.get());
    }
  }

  @Before
  public void setup() throws Exception {
    assumeFalse(
        "Test is not VPCSC compatible",
        Boolean.parseBoolean(System.getenv("GOOGLE_CLOUD_TESTS_IN_VPCSC")));

    bucketHelper = new TestBucketHelper(TEST_BUCKET_PREFIX);
    String testBucket = bucketHelper.getUniqueBucketName(LOCATION);

    // Define the schema we will be using for the output BigQuery table.
    List<BigQueryTableFieldSchema> outputTableFieldSchema = new ArrayList<>();
    outputTableFieldSchema.add(new BigQueryTableFieldSchema().setName("Word").setType("STRING"));
    outputTableFieldSchema.add(new BigQueryTableFieldSchema().setName("Count").setType("INTEGER"));
    BigQueryTableSchema outputSchema = new BigQueryTableSchema().setFields(outputTableFieldSchema);

    conf =
        AbstractBigQueryIoIntegrationTestBase.getConfigForGcsFromBigquerySettings(
            PROJECT_ID, testBucket);
    conf.set(BigQueryConfiguration.GCS_BUCKET.getKey(), testBucket);

    String qualifiedInputTableId =
        String.format(QUALIFIED_TABLE_ID_FMT, PROJECT_ID, DATASET_ID, "shakespeare");
    String qualifiedOutputTableId =
        String.format(QUALIFIED_TABLE_ID_FMT, PROJECT_ID, DATASET_ID, "shakespeare_word_count");
    String outputGcsPath = "gs://" + testBucket + "/test-output";

    // Create the job and get it's configuration.
    job = Job.getInstance(conf, "wordcount");
    conf = job.getConfiguration();

    // Set the job-level projectId.
    conf.set(BigQueryConfiguration.PROJECT_ID.getKey(), PROJECT_ID);

    // Set region for job to run in
    conf.set(BigQueryConfiguration.DATA_LOCATION.getKey(), LOCATION);

    // Configure input and output.
    BigQueryConfiguration.configureBigQueryInput(conf, qualifiedInputTableId);
    BigQueryOutputConfiguration.configure(
        conf,
        qualifiedOutputTableId,
        outputSchema,
        outputGcsPath,
        BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
        TextOutputFormat.class);

    // Tell the job what data the mapper will output.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(GsonBigQueryInputFormat.class);
    job.setOutputFormatClass(IndirectBigQueryOutputFormat.class);

    // Prepare test data
    bigquery = new BigQueryFactory().getBigQuery(conf);
    BigQueryHelper bigqueryHelper = new BigQueryHelper(bigquery);

    Storage storage =
        new Storage.Builder(
                HTTP_TRANSPORT,
                JSON_FACTORY,
                new RetryHttpInitializer(GoogleCloudStorageTestHelper.getCredential(), BQC_ID))
            .setApplicationName(BQC_ID)
            .build();
    GoogleCloudStorageFileSystem gcsFs =
        GoogleCloudStorageFileSystemIntegrationHelper.createGcsFs(PROJECT_ID);
    gcs = gcsFs.getGcs();

    // Create input dataset in `LOCATION`
    String exportBucket = bucketHelper.getUniqueBucketName("us");
    storage
        .buckets()
        .insert(PROJECT_ID, new Bucket().setName(exportBucket).setLocation("US"))
        .executeUnparsed();
    bigqueryHelper.exportBigQueryToGcs(
        PROJECT_ID,
        new TableReference()
            .setProjectId("bigquery-public-data")
            .setDatasetId("samples")
            .setTableId("shakespeare"),
        ImmutableList.of("gs://" + exportBucket + "/shakespeare"),
        /* awaitCompletion= */ true);

    storage
        .buckets()
        .insert(PROJECT_ID, new Bucket().setName(testBucket).setLocation(LOCATION))
        .executeUnparsed();
    gcsFs.rename(
        new URI("gs://" + exportBucket + "/shakespeare"),
        new URI("gs://" + testBucket + "/shakespeare"));

    bigquery
        .datasets()
        .insert(
            PROJECT_ID,
            new Dataset()
                .setLocation(LOCATION)
                .setDatasetReference(
                    new DatasetReference().setProjectId(PROJECT_ID).setDatasetId(DATASET_ID)))
        .executeUnparsed();
    bigqueryHelper.importFromGcs(
        PROJECT_ID,
        new TableReference()
            .setProjectId(PROJECT_ID)
            .setDatasetId(DATASET_ID)
            .setTableId("shakespeare"),
        /* schema= */ null,
        /* timePartitioning= */ null,
        /* kmsKeyName= */ null,
        BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
        BigQueryConfiguration.OUTPUT_TABLE_CREATE_DISPOSITION.getDefault(),
        "WRITE_EMPTY",
        ImmutableList.of("gs://" + testBucket + "/shakespeare"),
        /* awaitCompletion= */ true);
  }

  @Test
  public void testRegionalMapReduce() throws Exception {
    job.waitForCompletion(true);
  }

  @After
  public void tearDown() throws Exception {
    if (job == null) {
      return;
    }
    // After the job completes, make sure to clean up the Google Cloud Storage export paths.
    try {
      GsonBigQueryInputFormat.cleanupJob(job.getConfiguration(), job.getJobID());
    } finally {
      bucketHelper.cleanup(gcs);
      bigquery.datasets().delete(PROJECT_ID, DATASET_ID).setDeleteContents(true).executeUnparsed();
    }
  }
}
