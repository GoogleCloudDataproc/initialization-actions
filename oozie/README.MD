# Oozie

This initialization action installs the [Oozie](http://oozie.apache.org) workflow scheduler on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The Oozie server, client, and web interface are installed.

## Alternatives to Oozie for workflow orchestration

* Google Cloud Dataproc provides native support for executing a DAG of jobs via
[Workflow Templates](https://cloud.google.com/dataproc/docs/concepts/workflows/overview).
* In addition, [Google Cloud Composer](https://cloud.google.com/composer/) provides managed workflow orchestration built on top of [Apache Airflow](https://airflow.apache.org/).

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with Oozie installed:

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER-NAME>`:

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/oozie/oozie.sh
    ```
1. Once the cluster has been created Oozie should be running on the master node.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Testing Oozie

You can test this Oozie installation by running the `oozie-examples` included with Oozie. The examples are in an archive at `/usr/share/doc/oozie/oozie-examples.tar.gz`. To run the MapReduce example, you can do the following:

1. Move the examples to your home directory:
    ```
    cp /usr/share/doc/oozie/oozie-examples.tar.gz ~
    ```
1. Decompress the archive:<br/>
    ```
    tar -xzf oozie-examples.tar.gz
    ```
1. Edit the MapReduce example (`~/examples/apps/map-reduce/job.properties`) with details for your cluster:

    On standard and single node clusters, use the master node hostname:
    ```
    nameNode=hdfs://<cluster-name-m>:8020
    jobTracker=<cluster-name-m>:8032
    ```
    
    On high availability clusters use the nameservice ids (by default, the cluster name):
    ```
    nameNode=hdfs://<cluster-name>:8020
    jobTracker=<cluster-name>:8032
    ```
1. Move the Oozie examples to HDFS:
    ```
    hadoop fs -put ~/examples/ /user/<username>/
    ```
1. Run the example on the command line with:<br/>
    ```
    oozie job -oozie http://127.0.0.1:11000/oozie -config ~/examples/apps/map-reduce/job.properties -run
    ```

## Oozie web interface

The Oozie web interface is available on port `11000` on the master node of the cluster. For example, the Oozie web intarface would be available at the following address for a cluster named `my-dataproc-cluster`:

    http://my-dataproc-cluster-m:11000/oozie

To connect to the web interface you will need to create an SSH tunnel and use a SOCKS proxy. Instructions on how to do this are available in the [cloud dataproc documentation](https://cloud.google.com/dataproc/cluster-web-interfaces).

## Important notes

* As Oozie is updated in BigTop the version of Oozie which is installed with this action will change.
* HDFS is running on port `8020` and the (YARN) JobTracker is on port `8032` which may be useful information for some jobs.
* The [`hive2` action](https://oozie.apache.org/docs/4.3.0/DG_Hive2ActionExtension.html) is recommended over the [`hive` action](https://oozie.apache.org/docs/4.3.0/DG_HiveActionExtension.html).
  * The `hive2` action connects to the clusters Hive Server 2 and behaves like Dataproc Hive jobs.
  * The `hive` action uses Oozie's bundled version of Hive (1.2 in Oozie 4.3) and does not by default use the cluster's Hive metastore, which will cause tables metadata to be lost.
