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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Get;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods to interact with BigQuery.
 */
public class BigQueryUtils {
  // Logger.
  public static final Logger LOG = LoggerFactory.getLogger(BigQueryUtils.class);

  // Initial wait interval
  public static final int POLL_WAIT_INITIAL_MILLIS =
      (int) TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  // Maximum interval:
  public static final int POLL_WAIT_INTERVAL_MAX_MILLIS =
      (int) TimeUnit.MILLISECONDS.convert(180, TimeUnit.SECONDS);
  // Maximum time to wait for a job to complete. This value has
  // no basis in reality. The intention of using this value was to
  // pick a value greater than the default backoff max elapsed
  // time of 900 seconds.
  public static final int POLL_WAIT_MAX_ELAPSED_MILLIS =
      (int) TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

  /**
   * Polls job until it is completed.
   *
   * @param bigquery the Bigquery instance to poll.
   * @param projectId the project that is polling.
   * @param jobReference the job to poll.
   * @param progressable to get progress of task.
   *
   * @throws IOException on IO Error.
   * @throws InterruptedException on sleep interrupt.
   */
  public static void waitForJobCompletion(
      Bigquery bigquery,
      String projectId,
      JobReference jobReference,
      Progressable progressable)
      throws IOException, InterruptedException {

    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff pollBackOff =
        new ExponentialBackOff.Builder()
            .setMaxIntervalMillis(POLL_WAIT_INTERVAL_MAX_MILLIS)
            .setInitialIntervalMillis(POLL_WAIT_INITIAL_MILLIS)
            .setMaxElapsedTimeMillis(POLL_WAIT_MAX_ELAPSED_MILLIS)
            .build();

    // Get starting time.
    long startTime = System.currentTimeMillis();
    long elapsedTime = 0;
    boolean notDone = true;

    // While job is incomplete continue to poll.
    while (notDone) {
      BackOff operationBackOff = new ExponentialBackOff.Builder().build();
      Get get = bigquery.jobs().get(projectId, jobReference.getJobId());
      
      Job pollJob = ResilientOperation.retry(
          ResilientOperation.getGoogleRequestCallable(get),
          operationBackOff,
          RetryDeterminer.RATE_LIMIT_ERRORS,
          IOException.class,
          sleeper);

      elapsedTime = System.currentTimeMillis() - startTime;
      LOG.debug("Job status ({} ms) {}: {}", elapsedTime, jobReference.getJobId(),
          pollJob.getStatus().getState());
      if (pollJob.getStatus().getState().equals("DONE")) {
        notDone = false;
        if (pollJob.getStatus().getErrorResult() != null) {
          throw new IOException(pollJob.getStatus().getErrorResult().getMessage());
        }
      } else {
        long millisToWait = pollBackOff.nextBackOffMillis();
        if (millisToWait == BackOff.STOP) {
          throw new IOException(
              String.format(
                  "Job %s failed to complete after %s millis.",
                  jobReference.getJobId(),
                  elapsedTime));
        }
        // Pause execution for the configured duration before polling job status again.
        Thread.sleep(millisToWait);
        // Call progress to ensure task doesn't time out.
        progressable.progress();
      }
    }
  }

  /**
   * Parses the given JSON string and returns the extracted schema.
   *
   * @param fields a string to read the TableSchema from.
   * @return the List of TableFieldSchema described by the string fields.
   */
  public static List<TableFieldSchema> getSchemaFromString(String fields) {
    LOG.debug("getSchemaFromString('{}')", fields);

    // Parse the output schema for Json from fields.
    JsonParser jsonParser = new JsonParser();
    JsonArray json = jsonParser.parse(fields).getAsJsonArray();
    List<TableFieldSchema> fieldsList =  new ArrayList<>();

    // For each item in the list of fields.
    for (JsonElement jsonElement : json) {
      Preconditions.checkArgument(jsonElement.isJsonObject(),
          "Expected JsonObject for element, got '%s'.", jsonElement);
      JsonObject jsonObject = jsonElement.getAsJsonObject();

      // Set the name and type.
      Preconditions.checkArgument(jsonObject.get("name") != null,
          "Expected non-null entry for key 'name' in JsonObject '%s'", jsonObject);
      Preconditions.checkArgument(jsonObject.get("type") != null,
          "Expected non-null entry for key 'type' in JsonObject '%s'", jsonObject);
      TableFieldSchema fieldDef = new TableFieldSchema();
      fieldDef.setName(jsonObject.get("name").getAsString());
      fieldDef.setType(jsonObject.get("type").getAsString());

      // If mode is not null, set mode.
      if (jsonObject.get("mode") != null) {
        fieldDef.setMode(jsonObject.get("mode").getAsString());
      }

      // If the type is RECORD set the fields.
      if (jsonObject.get("type").getAsString().equals("RECORD")) {
        Preconditions.checkArgument(
            jsonObject.get("fields") != null,
            "Expected non-null entry for key 'fields' in JsonObject of type RECORD: '%s'",
            jsonObject);
        fieldDef.setFields(getSchemaFromString(jsonObject.get("fields").toString()));
      }

      fieldsList.add(fieldDef);
    }
    // Return list of TableFieldSchema.
    return fieldsList;
  }
}
