# Presto 

This initialization action installs the latest version of [Presto](prestodb.io) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. Additionally, this script will configure Presto to work with Hive on the cluster. The master Cloud Dataproc node will be the coordinator and all Cloud Dataproc workers will be Presto workers.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Presto installed by:

1. Uploading a copy of this initialization action (`presto.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. **Note** - you may need to increase the `initialization-action-timeout` if this script takes a long time to run. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`, and increase the timeout to 5 minutes.
   
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/presto.sh   
    --initialization-action-timeout 5m
    ```
1. Once the cluster has been created, Presto is configured to run on port `8080` (though you can change this int he script) on the master node in a Cloud Dataproc cluster. To connect to the Presto web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. You can also use the [Presto command line interface](https://prestodb.io/docs/current/installation/cli.html) using the `presto` command on the master node.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script must be updated based on which Presto version you wish you install
* You may need to adjust the memory settings in `jvm.config` based on your needs
* Presto is set to use HTTP port `8080` by default
* Only the Hive connector is configured by default
