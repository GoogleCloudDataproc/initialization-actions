# Apache Zeppelin

This initialization action installs the latest version of [Apache Zeppelin](https://zeppelin.incubator.apache.org/) on a master node within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Apache Zeppelin installed by:

1. Uploading a copy of this initialization action (`zeppelin.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. **Note** - you will need to increase the `initialization-action-timeout` to at least 15 minutes to allow for Zeppelin to compile and install. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`, and increase the timeout to 15 minutes.
   
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/zeppelin.sh   
    --initialization-action-timeout 15m
    ```
1. Once the cluster has been created, Zeppelin is configured to run on port `8080` on the master node in a Dataproc cluster. To connect to the Apache Zeppelin web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script uses two variables `SPARK_VERSION` and `HADOOP_VERSION` to specify which versions of Spark and Hadoop  are installed on your Cloud Dataproc cluster. You may need to adjust these values depending on which version of Spark and Hadoop are installed on your Cloud Dataproc cluster.
