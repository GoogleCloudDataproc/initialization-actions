# Presto

This initialization action installs the latest version of [Presto](prestodb.io) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. Additionally, this script will configure Presto to work with Hive on the cluster. The master Cloud Dataproc node will be the coordinator and all Cloud Dataproc workers will be Presto workers.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Presto installed:

1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://$MY_BUCKET/presto/presto.sh
    ```
1. Once the cluster has been created, Presto is configured to run on port `8080` (though you can change this in the script) on the master node in a Cloud Dataproc cluster. To connect to the Presto web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. You can also use the [Presto command line interface](https://prestodb.io/docs/current/installation/cli.html) using the `presto` command on the master node.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script must be updated based on which Presto version you wish to install
* You may need to adjust the memory settings in `jvm.config` based on your needs
* Presto is set to use HTTP port `8080` by default
* Only the Hive connector is configured by default
* High-Availability configuration is discouraged as coordinator is started only on `m-0` and other master nodes are idle
