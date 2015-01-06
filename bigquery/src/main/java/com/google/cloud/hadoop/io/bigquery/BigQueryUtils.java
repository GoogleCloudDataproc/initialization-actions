package com.google.cloud.hadoop.io.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper methods to interact with BigQuery.
 */
public class BigQueryUtils {
  // Logger.
  public static final LogUtil log = new LogUtil(BigQueryUtils.class);

  // Time to wait between polling.
  public static final long POLL_WAIT_DURATION = 3000;

  /**
   * Polls job until it is completed.
   *
   * TODO(user): implement exponential back-off and/or configurable polling.
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

    // Get starting time.
    long startTime = System.currentTimeMillis();
    long elapsedTime = 0;
    boolean notDone = true;

    // While job is incomplete continue to poll.
    while (notDone) {
      Job pollJob = bigquery.jobs().get(projectId, jobReference.getJobId()).execute();
      elapsedTime = System.currentTimeMillis() - startTime;
      log.debug("Job status (%d ms) %s: %s", elapsedTime, jobReference.getJobId(),
          pollJob.getStatus().getState());
      if (pollJob.getStatus().getState().equals("DONE")) {
        notDone = false;
        if (pollJob.getStatus().getErrorResult() != null) {
          throw new IOException(pollJob.getStatus().getErrorResult().getMessage());
        }
      } else {
        // Pause execution for the configured duration before polling job status again.
        Thread.sleep(POLL_WAIT_DURATION);
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
