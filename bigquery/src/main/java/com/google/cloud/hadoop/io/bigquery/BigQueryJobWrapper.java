package com.google.cloud.hadoop.io.bigquery;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;

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
