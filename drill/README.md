# Apache Drill Initialization Action

This initialization action installs [Apache Drill](http://drill.apache.org) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The script will also start drillbits on all nodes of the cluster.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

Check the variables set in the script to ensure they're to your liking.

1. Use the `gcloud` command to create a new cluster with Drill installed. Run one of the following commands depending on your desired cluster type.

    Standard cluster (requires Zookeeper init action)

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/zookeeper/zookeeper.sh,gs://goog-dataproc-initialization-actions-${REGION}/drill/drill.sh
    ```

    High availability cluster (Zookeeper comes pre-installed)

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --num-masters 3 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/drill/drill.sh
    ```

    Single node cluster (Zookeeper is unnecessary)

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --single-node \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/drill/drill.sh
    ```

1. Once the cluster has been created, Drillbits will start on all nodes. You can log into any node of the cluster to run Drill queries. Drill is installed in `/usr/lib/drill` (unless you change the setting) which contains a `bin` directory with `sqlline`.

You can run the following to get into sqlline, the Drill CLI query tool:

`/usr/lib/drill/bin/sqlline -u jdbc:drill:`

Once in sqlline, you can see what storage plugins are available. Out of the box, this initialization action supports GCS (gs), HDFS (hdfs), local linux file system (dfs) and Hive (hive):

```
$ /usr/lib/drill/bin/sqlline -u jdbc:drill:
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

### On single node clusters

In order to use Drill on single node cluster, run `/usr/lib/drill/bin/drill-embedded` or run sqlline with zk=local: `/usr/lib/drill/bin/sqlline -u jdbc:drill:zk=local`.

## Important notes
* This script must be updated based on which Drill version you wish you install
* This script must be updated based on your Cloud Dataproc cluster
* Access to the Drill UI is possible via SSH forwarding to port 8047 on any node, or with a [SOCKS proxy via SSH](https://cloud.google.com/solutions/connecting-securely#socks-proxy-over-ssh).
* By default your Drill query profiles are stored in GCS in your cluster's dataproc bucket, as returned by `/usr/share/google/get_metadata_value attributes/dataproc-bucket`. You can change this in `drill.sh`.
