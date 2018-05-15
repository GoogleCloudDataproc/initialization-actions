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


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;

/**
 * A shim class that allows us to construct a Job object with a Configuration object and
 * then set the jobID across both Hadoop 1 where setJobID is package private  and Hadoop 2 where
 * it is public.
 */
public class BigQueryJobWrapper extends Job {

  private Configuration configuration;
  private JobID jobId;

  public BigQueryJobWrapper() throws IOException {
  }

  public BigQueryJobWrapper(Configuration configuration) throws IOException {
    super(configuration);
    this.configuration = configuration;
  }

  public BigQueryJobWrapper(Configuration configuration, String jobNmae) throws IOException {
    super(configuration, jobNmae);
    this.configuration = configuration;
  }

  public void setJobID(JobID jobId) {
    this.jobId = jobId;
  }

  @Override
  public JobID getJobID() {
    return jobId;
  }

  @Override
  public Configuration getConfiguration() {
    return configuration;
  }
}
