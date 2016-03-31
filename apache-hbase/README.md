# Apache HBase

This initialization action installs the latest version of [Apache HBase](https://hbase.apache.org/) on a master node within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Apache HBase installed by:

1. Uploading a copy of this initialization action (`HBase.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. **Note** - you will need to increase the `initialization-action-timeout` to at least 15 minutes to allow for HBase to download and install. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`, and increase the timeout to 15 minutes.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/hbase.sh   
    --initialization-action-timeout 15m
    ```
1. Once the cluster has been created, HBase is configured to run on port `60010` on the master node in a Dataproc cluster. To connect to the Apache HBase web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script uses a variable `HBASE_VERSION`  to specify which versions of Hbase  is installed on your Cloud Dataproc cluster. You may need to adjust these values depending on which version of Hadoop is installed on your Cloud Dataproc cluster.
