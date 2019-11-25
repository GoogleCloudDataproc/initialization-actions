# Disable Noisy Timeline Server Logs

## Overview of Issue
Dataproc images in the 1.3 and 1.4 minor version tracks are affected by an
issue that causes the YARN Timeline Server to log Exceptions when the YARN
Resource Manager attempts to use the
[POST Timeline Entities REST API](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/TimelineServer.html#Posting_Timeline_Entities).

This excessive logging is made worse by a lack of log rotation for these logs,
caused by an older version of
[Jersey](https://eclipse-ee4j.github.io/jersey/) utilizing the Java
util logging (JUL) system, which does not respect the log4j properties respected
by the rest of the system. This is similar to this
[JIRA issue](https://issues.apache.org/jira/browse/HADOOP-11461) for the HDFS
NameNode.

Between the extremely noisy logging and the lack of rotation, this can cause
larger issues for longer running Dataproc Clusters where the Master node's disk
fills up, causing HDFS to enter
[Safemode](https://hadoop.apache.org/docs/r2.9.2/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html#Safemode),
which can impact running workloads.

## Fixing the Issue
The provided script can be used as an initialization action or run on live
clusters. Do note that there is always some risk associated with restarting
hadoop services on a running cluster, but this has been tested to be generally
safe for use on running Master nodes that may be effected.

### Using this Initialization Action
You can use this initialization action to create a new Dataproc Cluster that is
not impacted by this issue:

1. Stage this initialization action in a GCS bucket.

   ```bash
   git clone https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git
   gsutil cp \
     dataproc-initialization-actions/hotfix/disable-noisy-timeline-server-logs/disable-noisy-timeline-server-logs.sh \
     gs://<YOUR_GCS_BUCKET>/disable-noisy-timeline-server-logs.sh
   ```

1. Create a cluster using the staged initialization action.

   ```bash
   gcloud dataproc clusters create <CLUSTER_NAME> \
     --initialization-actions=gs://<YOUR_GCS_BUCKET>/disable-noisy-timeline-server-logs.sh
   ```

