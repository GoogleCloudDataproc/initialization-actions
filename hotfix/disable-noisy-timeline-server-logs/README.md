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

## Identifying the Issue
The real impact of this issue is caused by HDFS entering Safemode. You can
determine if your HDFS has entered Safemode by running the following command
from your Dataproc Master node.

```bash
hdfs dfsadmin -safemode get
```

If your HDFS has entered Safemode, you can further verify that the cause was
disk space by looking in the HDFS Namenode logs on your Dataproc Master node.

```bash
grep 'NameNode low on available disk space' /var/log/hadoop-hdfs/hadoop-hdfs-namenode-$(hostname).log*
```

Once you know that HDFS entered Safemode due to lack of disk space, you can
further verify that the size of the YARN Timeline Server logs were indeed the
cause of your disk filling up by checking their size from your Dataproc Master
node.

```bash
du -h /var/log/hadoop-yarn/yarn-yarn-timelineserver-$(hostname).out*
```

If the size of those files represent a significant portion of your disk space,
this issue is probably your root cause.

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

### Fixing a Running Cluster
If your running Cluster has entered Safemode and you wish to recover it without
recreating it, you can run the above script on your Dataproc Cluster's Master
nodes as root and then manually exit Safemode.

1. Run the initialization action as a script (on your Master nodes).

   ```bash
   sudo bash ./disable-noisy-timeline-server-logs.sh
   ```

1. Exit HDFS Safemode.

   ```bash
   hdfs dfsadmin -safemode leave
   ```

