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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TableFieldSchema;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for BigQueryUtils.
 */
@RunWith(JUnit4.class)
public class BigQueryUtilsTest {
  // Mock BigQuery.
  private Bigquery mockBigQuery;

  // Mock TaskAttemptContext.
  private Progressable mockProgressable;

  // Sample projectId for testing.
  private String projectId = "Test";

  // Mock JobReference.
  private JobReference mockJobReference;

  // Mock BigQuery Jobs.
  private Bigquery.Jobs mockBigQueryJobs;

  // Mock BigQuery Jobs return.
  private Bigquery.Jobs.Get mockJobsGet;

  // Sample completed JobStatus.
  private JobStatus jobStatus;

  // Sample unfinished JobStatus.
  private JobStatus notDoneJobStatus;

  // Sample completed Job.
  private Job job;

  // Sample unfinished Job.
  private Job notDoneJob;

  // For exceptions expected per test method.
  /**
   * Mocks result of BigQuery for polling for job completion.
   *
   * @throws IOException on IOError.
   */
  @Before
  public void setUp() throws IOException {

    // Set mock JobReference
    mockJobReference = new JobReference();

    // Create the unfinished job result.
    notDoneJob = new Job();
    notDoneJobStatus = new JobStatus();
    notDoneJobStatus.setState("NOT DONE");
    notDoneJobStatus.setErrorResult(null);
    notDoneJob.setStatus(notDoneJobStatus);
    notDoneJob.setJobReference(mockJobReference);

    // Create the finished job result.
    job = new Job();
    jobStatus = new JobStatus();
    jobStatus.setState("DONE");
    jobStatus.setErrorResult(null);
    job.setStatus(jobStatus);
    job.setJobReference(mockJobReference);

    // Mock BigQuery.
    mockBigQuery = mock(Bigquery.class);
    mockBigQueryJobs = mock(Bigquery.Jobs.class);
    mockJobsGet = mock(Bigquery.Jobs.Get.class);
    when(mockBigQuery.jobs()).thenReturn(mockBigQueryJobs);
    when(mockBigQueryJobs.get(projectId, mockJobReference.getJobId()))
        .thenReturn(mockJobsGet).thenReturn(mockJobsGet);
    when(mockJobsGet.setLocation(any(String.class))).thenReturn(mockJobsGet);
    when(mockJobsGet.execute()).thenReturn(job);

    // Constructor coverage
    new BigQueryUtils();

    // Mock Progressable.
    mockProgressable = mock(Progressable.class);
  }

  /**
   * Tests waitForJobCompletion method of BigQueryUtils when the job has been completed.
   */
  @Test
  public void testWaitForJobCompletion()
      throws IOException, InterruptedException {
    // Return completed job.
    when(mockJobsGet.execute()).thenReturn(job);

    // Run waitForJobCompletion.
    BigQueryUtils.waitForJobCompletion(mockBigQuery, projectId, mockJobReference, mockProgressable);

    // Verify that the method terminates and that the correct calls were sent to the mock BigQuery.
    verify(mockBigQuery).jobs();
    verify(mockBigQueryJobs).get(projectId, mockJobReference.getJobId());
    verify(mockJobsGet).execute();
    verify(mockProgressable, never()).progress();
  }

  /**
   * Tests waitForJobCompletion method of BigQueryUtils when the job status changes.
   */
  @Test
  public void testWaitForJobCompletionChange()
      throws IOException, InterruptedException {
    // Return unfinished job the return finished job.
    when(mockJobsGet.execute()).thenReturn(notDoneJob).thenReturn(job);

    // Run waitForJobCompletion.
    BigQueryUtils.waitForJobCompletion(mockBigQuery, projectId, mockJobReference, mockProgressable);

    // Verify that the method terminates and that the correct calls were sent to the mock BigQuery.
    verify(mockBigQuery, times(2)).jobs();
    verify(mockBigQueryJobs, times(2)).get(projectId, mockJobReference.getJobId());
    verify(mockJobsGet, times(2)).execute();
    verify(mockProgressable, atLeastOnce()).progress();
  }

  /**
   * Tests waitForJobCompletion method of BigQueryUtils when the job returns an error.
   */
  @Test
  public void testWaitForJobCompletionError()
      throws InterruptedException, IOException {
    // Return completed job.
    when(mockJobsGet.execute()).thenReturn(job);

    // Set error result to not null.
    jobStatus.setErrorResult(new ErrorProto());

    // Run waitForJobCompletion and assert failure.
    assertThrows(
        IOException.class,
        () ->
            BigQueryUtils.waitForJobCompletion(
                mockBigQuery, projectId, mockJobReference, mockProgressable));
  }

  /**
   * Tests getSchemaFromString method of BigQueryUtils for simple schema.
   */
  @Test
  public void testGetSchemaFromString() {
    // Set fields schema for testing.
    String fields =
        "[{'name': 'MyName', 'type': 'STRING'},"
        + "{'name': 'Number', 'type': 'INTEGER', 'mode': 'sample'}]";
    List<TableFieldSchema> list = BigQueryUtils.getSchemaFromString(fields);
    assertThat(list).hasSize(2);
    assertThat(list.get(0).getName()).isEqualTo("MyName");
    assertThat(list.get(0).getType()).isEqualTo("STRING");

    assertThat(list.get(1).getName()).isEqualTo("Number");
    assertThat(list.get(1).getType()).isEqualTo("INTEGER");
    assertThat(list.get(1).getMode()).isEqualTo("sample");
  }

  /**
   * Tests getSchemaFromString method of BigQueryUtils for nested schema.
   */
  @Test
  public void testGetSchemaFromStringNested() {
    // Set fields schema for testing.
    String fields =
        "[{'name': 'MyName', 'type': 'STRING'},"
        + "{'name': 'MyNestedField', 'type': 'RECORD', 'mode': 'repeated', 'fields': ["
            + "{'name': 'field1', 'type': 'INTEGER'}, {'name': 'field2', 'type': 'STRING'}"
        + "]}]";
    List<TableFieldSchema> list = BigQueryUtils.getSchemaFromString(fields);
    assertThat(list).hasSize(2);
    assertThat(list.get(0).getName()).isEqualTo("MyName");
    assertThat(list.get(0).getType()).isEqualTo("STRING");

    assertThat(list.get(1).getName()).isEqualTo("MyNestedField");
    assertThat(list.get(1).getType()).isEqualTo("RECORD");
    assertThat(list.get(1).getMode()).isEqualTo("repeated");

    List<TableFieldSchema> nestedList = list.get(1).getFields();
    assertThat(nestedList).isNotNull();
    assertThat(nestedList).hasSize(2);

    assertThat(nestedList.get(0).getName()).isEqualTo("field1");
    assertThat(nestedList.get(0).getType()).isEqualTo("INTEGER");
    assertThat(nestedList.get(1).getName()).isEqualTo("field2");
    assertThat(nestedList.get(1).getType()).isEqualTo("STRING");
  }

  /**
   * Tests getSchemaFromString method of BigQueryUtils for schema with a missing 'name' field.
   */
  @Test
  public void testGetSchemaFromStringWithMissingName() {
    // Set bad schema for testing; missing 'name' for the first schema entry.
    String fields =
        "[{'type': 'STRING'},"
        + "{'name': 'Number', 'type': 'INTEGER', 'mode': 'sample'}]";
    assertThrows(IllegalArgumentException.class, () -> BigQueryUtils.getSchemaFromString(fields));
  }

  /**
   * Tests getSchemaFromString method of BigQueryUtils for schema with a missing 'type' field.
   */
  @Test
  public void testGetSchemaFromStringWithMissingType() {
    // Bad schema, missing 'type' in the second entry.
    String fields =
        "[{'name': 'MyName', 'type': 'STRING'},"
        + "{'name': 'Number', 'mode': 'sample'}]";
    assertThrows(IllegalArgumentException.class, () -> BigQueryUtils.getSchemaFromString(fields));
  }

  /**
   * Tests getSchemaFromString for a schema where a top-level entry is not a JsonObject.
   */
  @Test
  public void testGetSchemaFromStringWithTopLevelNonJsonObject() {
    // Bad schema, missing 'type' in the second entry.
    String fields =
        "[{'name': 'MyName', 'type': 'STRING'},"
        + "foo,"
        + "{'name': 'Number', 'type': 'INTEGER', 'mode': 'sample'}]";
    assertThrows(IllegalArgumentException.class, () -> BigQueryUtils.getSchemaFromString(fields));
  }

  /**
   * Tests getSchemaFromString for a schema where a "RECORD" entry lacks 'fields'.
   */
  @Test
  public void testGetSchemaFromStringRecordTypeLacksFields() {
    // Missing 'fields' entry for an entry of type "RECORD".
    String fields =
        "[{'name': 'MyName', 'type': 'STRING'},"
        + "{'name': 'MyNestedField', 'type': 'RECORD', 'mode': 'repeated'}]";
    assertThrows(IllegalArgumentException.class, () -> BigQueryUtils.getSchemaFromString(fields));
  }
}
