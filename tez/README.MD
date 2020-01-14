# Apache Tez

This initialization action installs [Apache Tez](https://tez.apache.org/) on 1.0 to 1.2 [Google Cloud Dataproc](https://cloud.google.com/dataproc) clusters.

Note that Tez is pre-installed on Dataproc 1.3+ clusters, so you should not run this initialization action on 1.3+ clusters.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with Apache Tez installed:

1. Use the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/tez/tez.sh
    ```
1. On Dataproc 1.3+ clusters in order to use pre-installed Tez is necessary to add the flag `--properties 'hadoop-env:HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/etc/tez/conf:/usr/lib/tez/*:/usr/lib/tez/lib/*'`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --image-version 1.3 \
        --properties 'hadoop-env:HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/etc/tez/conf:/usr/lib/tez/*:/usr/lib/tez/lib/*'
    ```
1. Hive is be configured to use Tez, rather than MapReduce, as its execution engine. This can significantly speed up some Hive queries. Read more in the [Hive on Tez documentation](https://cwiki.apache.org/confluence/display/Hive/Hive+on+Tez).

## Tez Web UI

The Tez UI is served by the YARN App Timeline Server (port 8188) at `/tez-ui`. Follow the instructions at [connect to cluster web interfaces]() to create a SOCKS5 proxy to view `http://clustername-m:8188/tez-ui`. On high availability clusters, this is at `http://clustername-m-0:8188/tez-ui`.

## Important notes

* This script has a number of user-defined variables at the top, such as the Tez version and install location.
* This script builds from source which may cause issues if you use an unstable release.
* Be careful with the [`protobuf`](https://github.com/google/protobuf) version - Tez often requires a *very specific* version to function properly.
