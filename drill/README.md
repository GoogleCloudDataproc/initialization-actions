# Apache Drill Initialization Action

This initialization action installs [Apache Drill](http://drill.apache.org) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The script will also start drillbits on all nodes of the cluster.

## Using this initialization action

Check the variables set in the script to ensure they're to your liking.

Once you have configured a copy of this script, you can use this initialization action to create a new Dataproc cluster with Drill installed by:

1. Uploading a copy of the [initialization action for zookeeper](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/zookeeper) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Uploading a copy of the initialization action (`drill.sh`) to GCS.
1. Using the `gcloud` command to create a new cluster with zookeeper and this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`, and increase the timeout to 5 minutes.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/zookeeper.sh,gs://<GCS_BUCKET>/drill.sh
    --initialization-action-timeout 5m
    ```
1. Once the cluster has been created, Drillbits will start on all nodes. You can log into any node of the cluster to run Drill queries. Drill is installed in `/usr/lib/drill` (unless you change the setting) which contains a `bin` directory with `sqlline`.

You can run the following to get into sqlline, the Drill CLI query tool:

`sudo -u drill /usr/lib/drill/bin/sqlline -u jdbc:drill:`

If you prefer to run drill as your user, set `DRILL_LOG_DIR` to someplace you have permission to write to:

`DRILL_LOG_DIR=~/logs /usr/lib/drill/bin/sqlline -u jdbc:drill:`

Once in sqlline, you can see what storage plugins are available. Out of the box, this initialization action supports GCS (gs), HDFS (hdfs), local linux file system (dfs) and Hive (hive):

```
$ DRILL_LOG_DIR=~/logs /usr/lib/drill/bin/sqlline -u jdbc:drill:
OpenJDK 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
apache drill 1.9.0
"just drill it"
0: jdbc:drill:> show databases;
+---------------------+
|     SCHEMA_NAME     |
+---------------------+
| INFORMATION_SCHEMA  |
| cp.default          |
| dfs.default         |
| dfs.root            |
| dfs.tmp             |
| gs.default          |
| gs.root             |
| hdfs.default        |
| hdfs.root           |
| hdfs.tmp            |
| hive.default        |
| sys                 |
+---------------------+
12 rows selected (3.943 seconds)
```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script must be updated based on which Drill version you wish you install
* This script must be updated based on your Cloud Dataproc cluster
* Access to the Drill UI is possible via SSH forwarding to port 8047 on any drillbit, or with a [SOCKS proxy via SSH](https://cloud.google.com/solutions/connecting-securely#socks-proxy-over-ssh).
* By default your Drill query profiles are stored in GCS in your cluster's dataproc bucket, as returned by `/usr/share/google/get_metadata_value attributes/dataproc-bucket`. You can change this in `drill.sh`.
