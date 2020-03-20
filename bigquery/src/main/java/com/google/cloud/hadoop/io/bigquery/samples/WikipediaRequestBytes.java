/*
 * Copyright 2020 Google LLC
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

import static com.google.common.collect.Streams.stream;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.JsonObject;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Sample MapReduce which computes SUM(bytes_transferred) grouping by "title" from a
 * wikipedia_pageviews table. The following is the BigQuery schema of the table:
 *   datetime_epoch INTEGER NULLABLE
 *   year INTEGER NULLABLE
 *   month INTEGER NULLABLE
 *   day INTEGER NULLABLE
 *   hour INTEGER NULLABLE
 *   wikimedia_project STRING NULLABLE
 *   language STRING NULLABLE
 *   title STRING NULLABLE
 *   views INTEGER NULLABLE
 *   bytes_transferred INTEGER NULLABLE
 */
public class WikipediaRequestBytes {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Mapper which grabs extracts 'title' and 'bytes_transferred' from the input record and emits
   * them as key/value, respectively.
   */
  public static class TitleBytesMapper
      extends Mapper<LongWritable, JsonObject, Text, LongWritable> {

    private final Text pageTitleText = new Text();
    private final LongWritable pageBytesWritable = new LongWritable();

    @Override
    public void map(
        LongWritable lineNumber,
        JsonObject record,
        Mapper<LongWritable, JsonObject, Text, LongWritable>.Context context)
        throws IOException, InterruptedException {
      long pageBytes = -1;
      String pageTitle = null;
      try {
        pageBytes = record.get("bytes_transferred").getAsLong();
        pageTitle = record.get("title").getAsString();
      } catch (NullPointerException | ClassCastException e) {
        logger.atSevere().withCause(e).log(
            "Malformed record, bad title or bytes_transferred: '%s'", record);
      }

      if (pageTitle != null) {
        pageTitleText.set(pageTitle);
        pageBytesWritable.set(pageBytes);
        context.write(pageTitleText, pageBytesWritable);
      }
    }
  }

  /** Reducer which sums the bytes_transferred values by page title. */
  public static class TitleBytesSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private final LongWritable bytesCountWritable = new LongWritable();

    @Override
    public void reduce(Text title, Iterable<LongWritable> bytesTransferred, Context context)
        throws IOException, InterruptedException {
      long count = stream(bytesTransferred).mapToLong(LongWritable::get).sum();
      bytesCountWritable.set(count);
      context.write(title, bytesCountWritable);
    }
  }

  public static void main(String[] args)
      throws IOException, InterruptedException, ClassNotFoundException {
    GenericOptionsParser parser = new GenericOptionsParser(args);
    String[] customArgs = parser.getRemainingArgs();
    Configuration config = parser.getConfiguration();

    if (customArgs.length != 5) {
      System.out.println(
          "Usage: hadoop jar wikipedia_bytes_deploy.jar "
              + "[projectId] [inputDatasetId] [inputTableId] [exportGcsBucket] [jobOutputPath]");
      System.exit(1);
    }

    String projectId = customArgs[0];
    String inputDatasetId = customArgs[1];
    String inputTableId = customArgs[2];
    String exportGcsBucket = customArgs[3];
    String jobOutputPath = customArgs[4];

    JobConf conf = new JobConf(config, WikipediaRequestBytes.class);
    BigQueryConfiguration.configureBigQueryInput(conf, projectId, inputDatasetId, inputTableId);
    conf.set(BigQueryConfiguration.GCS_BUCKET.getKey(), exportGcsBucket);

    Job job = new Job(conf, "WikipediaRequestBytes");
    job.setJarByClass(WikipediaRequestBytes.class);

    job.setMapperClass(TitleBytesMapper.class);
    job.setCombinerClass(TitleBytesSumReducer.class);
    job.setReducerClass(TitleBytesSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileOutputFormat.setOutputPath(job, new Path(jobOutputPath));

    // Read from BigQuery, write with plan TextOutputFormat to provided 'Path'.
    job.setInputFormatClass(GsonBigQueryInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.waitForCompletion(true);

    // Make sure to clean up the GCS export paths if desired, and possibly an intermediate input
    // table if we did sharded export and thus didn't clean it up at setup time.
    GsonBigQueryInputFormat.cleanupJob(job.getConfiguration(), job.getJobID());
  }
}
