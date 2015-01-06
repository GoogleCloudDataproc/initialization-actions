package com.google.cloud.hadoop.io.bigquery.mapred;

import com.google.cloud.hadoop.io.bigquery.BigQueryJobWrapper;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

import java.io.File;
import java.io.IOException;

/**
 * Utility to create a JobContext for use with our MRV1 wrapper.
 */
public class BigQueryMapredJobContext {
  protected static final LogUtil log =
      new LogUtil(BigQueryMapredJobContext.class);

  /**
   * Create a mapreduce.JobContext from a mapred.JobConf.
   */
  public static JobContext from(Configuration jobConf) throws IOException {
    String jobDirString = jobConf.get("mapreduce.job.dir");
    Preconditions.checkArgument(jobDirString != null,
        "mapreduce.job.dir must not be null");
    String jobIdString = new File(jobDirString).getName();
    log.debug("jobIdString = %s", jobIdString);
    // JobID.forName will throw an explicit exception if
    // the job string is the wrong format.
    JobID jobId = JobID.forName(jobIdString);
    log.debug("jobId = %s", jobId);

    BigQueryJobWrapper wrapper = new BigQueryJobWrapper(jobConf);
    wrapper.setJobID(jobId);
    return wrapper;
  }
}
