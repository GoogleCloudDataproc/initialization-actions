/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.hadoop.io.bigquery.samples;

import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.PROJECT_ID;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.SELECTED_FIELDS;
import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.SQL_FILTER;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.DirectBigQueryInputFormat;
import java.io.IOException;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * An example Hadoop WordCount program that counts the number of times a word appears in a BigQuery
 * table column.
 */
public class DirectBigQueryWordCount {

  /** The mapper for our WordCount job. */
  public static class Map extends Mapper<NullWritable, GenericRecord, Text, LongWritable> {
    private final Text word = new Text();
    private final LongWritable count = new LongWritable();

    @Override
    public void setup(Context context) {}

    @Override
    public void map(NullWritable unusedKey, GenericRecord row, Context context)
        throws IOException, InterruptedException {
      word.set(((Utf8) row.get("word")).toString());
      count.set((Long) row.get("word_count"));
      context.write(word, count);
    }
  }

  /** The reducer for our WordCount job. */
  public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final LongWritable count = new LongWritable();

    @Override
    public void reduce(Text word, Iterable<LongWritable> counts, Context context)
        throws IOException, InterruptedException {
      // Add up the values to get a total number of occurrences of our word.
      count.set(
          StreamSupport.stream(counts.spliterator(), false).mapToLong(LongWritable::get).sum());
      context.write(word, count);
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {

    // GenericOptionsParser is a utility to parse command line arguments generic to the Hadoop
    // framework. This example won't cover the specifics, but will recognize several standard
    // command line arguments, enabling applications to easily specify a namenode, a
    // ResourceManager, additional configuration resources etc.
    GenericOptionsParser parser = new GenericOptionsParser(args);
    args = parser.getRemainingArgs();

    // Make sure we have the right parameters.
    if (args.length != 3) {
      System.out.println(
          "Usage: hadoop jar bigquery_wordcount.jar [ProjectId] [QualifiedInputTableId] "
              + "[GcsOutputPath]\n"
              + "    ProjectId - Project under which to issue the BigQuery operations. Also "
              + "serves as the default project for table IDs which don't explicitly specify a "
              + "project for the table.\n"
              + "    QualifiedInputTableId - Input table ID of the form "
              + "(Optional ProjectId):[DatasetId].[TableId]\n"
              + "    OutputPath - The output path to write data, e.g. "
              + "gs://bucket/dir/");
      System.exit(1);
    }

    // Get the individual parameters from the command line.
    String projectId = args[0];
    String inputQualifiedTableId = args[1];
    String outputPath = args[2];

    // Create the job and get its configuration.
    Job job = new Job(parser.getConfiguration(), "wordcount");
    Configuration conf = job.getConfiguration();

    // Set the job-level projectId.
    conf.set(PROJECT_ID.getKey(), projectId);

    // Configure input and output.
    BigQueryConfiguration.configureBigQueryInput(conf, inputQualifiedTableId);

    // Set column and predicate filters
    conf.set(SELECTED_FIELDS.getKey(), "word,word_count");
    conf.set(SQL_FILTER.getKey(), "word >= 'A' AND word <= 'zzz'");
    conf.set(MRJobConfig.NUM_MAPS, "999");

    // This helps Hadoop identify the Jar which contains the mapper and reducer by specifying a
    // class in that Jar. This is required if the jar is being passed on the command line to Hadoop.
    job.setJarByClass(DirectBigQueryWordCount.class);

    // Tell the job what the output will be.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(DirectBigQueryInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.waitForCompletion(true);
  }
}
