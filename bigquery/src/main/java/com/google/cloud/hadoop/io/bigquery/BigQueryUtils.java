package com.google.cloud.hadoop.io.bigquery;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Get;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.cloud.hadoop.util.OperationWithRetry;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Helper methods to interact with BigQuery.
 */
public class BigQueryUtils {
  // Logger.
  public static final LogUtil log = new LogUtil(BigQueryUtils.class);

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
    ApiErrorExtractor errorExtractor = new ApiErrorExtractor();

    // Get starting time.
    long startTime = System.currentTimeMillis();
    long elapsedTime = 0;
    boolean notDone = true;

    // While job is incomplete continue to poll.
    while (notDone) {
      BackOff operationBackOff = new ExponentialBackOff.Builder().build();
      Get get = bigquery.jobs().get(projectId, jobReference.getJobId());
      OperationWithRetry<Get, Job> pollJobOperation =
          new OperationWithRetry<>(
              sleeper,
              operationBackOff,
              get,
              OperationWithRetry.createRateLimitedExceptionPredicate(errorExtractor));

      Job pollJob = pollJobOperation.execute();
      elapsedTime = System.currentTimeMillis() - startTime;
      log.debug("Job status (%d ms) %s: %s", elapsedTime, jobReference.getJobId(),
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
    log.debug("getSchemaFromString('%s')", fields);

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
