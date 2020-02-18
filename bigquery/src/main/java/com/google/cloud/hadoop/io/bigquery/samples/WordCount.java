package com.google.cloud.hadoop.io.bigquery.samples;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * An example Hadoop WordCount program that counts the number of times a word appears in a BigQuery
 * table column.
 */
public class WordCount {

  /** The configuration key used to specify the BigQuery field name ("column name"). */
  public static final String WORDCOUNT_WORD_FIELDNAME_KEY = "mapred.bq.samples.wordcount.word.key";

  /**
   * Default value for the configuration entry specified by {@code WORDCOUNT_WORD_FIELDNAME_KEY}.
   * Examples: {@code word} in {@code publicdata:samples.shakespeare} or {@code repository_name} in
   * {@code publicdata:samples.github_timeline}.
   */
  public static final String WORDCOUNT_WORD_FIELDNAME_VALUE_DEFAULT = "word";

  /**
   * Mapper function for the WordCount job. For input, it consumes a LongWritable and JsonObject as
   * the key and value. These correspond to a row identifier and Json representation of the row's
   * values/columns. For output, it produces Text and a LongWritable as the key and value. These
   * correspond to the word and a count for the number of times it has occurred.
   */
  public static class Map extends Mapper<LongWritable, JsonObject, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1);
    private Text word = new Text();
    private String wordKey;

    @Override
    public void setup(Mapper<LongWritable, JsonObject, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {
      // Find the runtime-configured key for the field name we're looking for in the map task.
      Configuration conf = context.getConfiguration();
      wordKey = conf.get(WORDCOUNT_WORD_FIELDNAME_KEY, WORDCOUNT_WORD_FIELDNAME_VALUE_DEFAULT);
    }

    @Override
    public void map(
        LongWritable key,
        JsonObject value,
        Mapper<LongWritable, JsonObject, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {
      JsonElement countElement = value.get(wordKey);
      if (countElement != null) {
        String wordInRecord = countElement.getAsString();
        word.set(wordInRecord);
        // Write out the key, value pair (write out a value of 1, which will be
        // added to the total count for this word in the Reducer).
        context.write(word, ONE);
      }
    }
  }

  /**
   * Reducer function for the WordCount job. For input, it consumes the Text and LongWritable that
   * the mapper produced. For output, it produces a JsonObject and NullWritable. The JsonObject
   * represents the data that will be loaded into BigQuery.
   */
  public static class Reduce extends Reducer<Text, LongWritable, JsonObject, NullWritable> {

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      // Add up the values to get a total number of occurrences of our word.
      long count = Streams.stream(values).mapToLong(LongWritable::get).sum();

      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("Word", key.toString());
      jsonObject.addProperty("Count", count);
      // Key does not matter.
      context.write(jsonObject, NullWritable.get());
    }
  }

  /**
   * Configures and runs the main Hadoop job. Takes a String[] of 5 parameters with the format:
   * {@code [ProjectId] [QualifiedInputTableId] [InputTableFieldName] [QualifiedOutputTableId]
   * [GcsOutputPath]}
   *
   * <p><strong>ProjectId</strong> - Project under which to issue the BigQuery operations. Also
   * serves as the default project for table IDs that don't explicitly specify a project for the
   * table.
   *
   * <p><strong>QualifiedInputTableId</strong> - Input table ID of the form {@code (Optional
   * ProjectId):[DatasetId].[TableId]}
   *
   * <p><strong>InputTableFieldName</strong> - Name of the field to count in the input table, e.g.
   * {@code word} in {@code publicdata:samples.shakespeare} or {@code repository_name} in {@code
   * publicdata:samples.github_timeline}.
   *
   * <p><strong>QualifiedOutputTableId</strong> - Input table ID of the form {@code (Optional
   * ProjectId):[DatasetId].[TableId]}
   *
   * <p><strong>GcsOutputPath</strong> - The output path to store temporary Cloud Storage data,
   * e.g., {@code gs://bucket/dir/}
   *
   * @param args a String[] containing ProjectId, QualifiedInputTableId, InputTableFieldName,
   *     QualifiedOutputTableId, and GcsOutputPath.
   * @throws IOException on IO Error.
   * @throws InterruptedException on Interrupt.
   * @throws ClassNotFoundException if not all classes are present.
   */
  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {

    // GenericOptionsParser is a utility to parse command line arguments
    // generic to the Hadoop framework. This example doesn't cover the specifics,
    // but recognizes several standard command line arguments, enabling
    // applications to easily specify a NameNode, a ResourceManager, additional
    // configuration resources, etc.
    GenericOptionsParser parser = new GenericOptionsParser(args);
    args = parser.getRemainingArgs();

    // Make sure we have the right parameters.
    if (args.length != 5) {
      System.out.println(
          "Usage: hadoop jar bigquery_wordcount.jar [ProjectId] [QualifiedInputTableId] "
              + "[InputTableFieldName] [QualifiedOutputTableId] [GcsOutputPath]\n"
              + "    ProjectId - Project under which to issue the BigQuery operations. Also serves "
              + "as the default project for table IDs that don't explicitly specify a project for "
              + "the table.\n"
              + "    QualifiedInputTableId - Input table ID of the form "
              + "(Optional ProjectId):[DatasetId].[TableId]\n"
              + "    InputTableFieldName - Name of the field to count in the input table, e.g., "
              + "'word' in publicdata:samples.shakespeare or 'repository_name' in "
              + "publicdata:samples.github_timeline.\n"
              + "    QualifiedOutputTableId - Output table ID of the form "
              + "(Optional ProjectId):[DatasetId].[TableId]\n"
              + "    GcsOutputPath - The output path to store temporary Cloud Storage data, e.g., "
              + "gs://bucket/dir/");
      System.exit(1);
    }

    // Get the individual parameters from the command line.
    String projectId = args[0];
    String inputQualifiedTableId = args[1];
    String inputTableFieldId = args[2];
    String outputQualifiedTableId = args[3];
    String outputGcsPath = args[4];

    // Define the schema we will be using for the output BigQuery table.
    BigQueryTableSchema outputSchema =
        new BigQueryTableSchema()
            .setFields(
                ImmutableList.of(
                    new BigQueryTableFieldSchema().setName("Word").setType("STRING"),
                    new BigQueryTableFieldSchema().setName("Count").setType("INTEGER")));

    // Create the job and get its configuration.
    Job job = Job.getInstance(parser.getConfiguration(), "wordcount");
    Configuration conf = job.getConfiguration();

    // Set the job-level projectId.
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);

    // Configure input and output.
    BigQueryConfiguration.configureBigQueryInput(conf, inputQualifiedTableId);
    BigQueryOutputConfiguration.configure(
        conf,
        outputQualifiedTableId,
        outputSchema,
        outputGcsPath,
        BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
        TextOutputFormat.class);

    conf.set(WORDCOUNT_WORD_FIELDNAME_KEY, inputTableFieldId);

    // This helps Hadoop identify the Jar which contains the mapper and reducer
    // by specifying a class in that Jar. This is required if the jar is being
    // passed on the command line to Hadoop.
    job.setJarByClass(WordCount.class);

    // Tell the job what data the mapper will output.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(GsonBigQueryInputFormat.class);
    // Instead of using BigQueryOutputFormat, we use the newer
    // IndirectBigQueryOutputFormat, which works by first buffering all the data
    // into a Cloud Storage temporary file, and then on commitJob, copies all data from
    // Cloud Storage into BigQuery in one operation. Its use is recommended for large jobs
    // since it only requires one BigQuery "load" job per Hadoop/Spark job, as
    // compared to BigQueryOutputFormat, which performs one BigQuery job for each
    // Hadoop/Spark task.
    job.setOutputFormatClass(IndirectBigQueryOutputFormat.class);

    job.waitForCompletion(true);

    // After the job completes, clean up the Cloud Storage export paths.
    GsonBigQueryInputFormat.cleanupJob(job.getConfiguration(), job.getJobID());

    // You can view word counts in the BigQuery output table at
    // https://bigquery.cloud.google.com.
  }
}
