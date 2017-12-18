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
package com.google.cloud.hadoop.io.bigquery.mapred;

import com.google.cloud.hadoop.io.bigquery.BigQueryJobWrapper;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
