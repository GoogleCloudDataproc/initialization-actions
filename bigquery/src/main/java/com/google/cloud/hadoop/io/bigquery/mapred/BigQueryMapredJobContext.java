package com.google.cloud.hadoop.io.bigquery.mapred;

import com.google.cloud.hadoop.io.bigquery.BigQueryJobWrapper;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Utility to create a JobContext for use with our MRV1 wrapper.
 */
public class BigQueryMapredJobContext {
  protected static final Logger LOG =
      LoggerFactory.getLogger(BigQueryMapredJobContext.class);

  /**
   * Create a mapreduce.JobContext from a mapred.JobConf.
   */
  public static JobContext from(Configuration jobConf) throws IOException {
    String jobDirString = jobConf.get("mapreduce.job.dir");
    Preconditions.checkArgument(jobDirString != null,
        "mapreduce.job.dir must not be null");
    String jobIdString = new File(jobDirString).getName();
    LOG.debug("jobIdString = {}", jobIdString);
    // JobID.forName will throw an explicit exception if
    // the job string is the wrong format.
    JobID jobId = JobID.forName(jobIdString);
    LOG.debug("jobId = {}", jobId);

    BigQueryJobWrapper wrapper = new BigQueryJobWrapper(jobConf);
    wrapper.setJobID(jobId);
    return wrapper;
  }
}
