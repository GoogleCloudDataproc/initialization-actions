--------------------------------------------------------------------------------


# Trino

This initialization action installs the latest version of [Trino](trino.io)
on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
Additionally, this script will configure Trino to work with Hive on the
cluster and will include sample BigQuery configuration with which you can define your project-id and use BigQuery as a data source. The master Dataproc node will be the coordinator and all
Dataproc workers will be Trino workers.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
Trino installed:

1.  Using the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/trino/trino.sh
    ```

1.  Once the cluster has been created, Trino is configured to run on port
    `8080` (though you can change this in the script) on the master node in a
    Cloud Dataproc cluster. To connect to the Trino web interface, you will
    need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the
    [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces)
    documentation. You can also use the
    [Trino command line interface](https://trinodb.io/docs/current/installation/cli.html)
    using the `trino` command on the master node.

You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes

*   This script must be updated based on which Trino version you wish to
    install
*   You may need to adjust the memory settings in `jvm.config` based on your
    needs
*   Trino is set to use HTTP port `8080` by default, but can be changed using
    `--metadata trino-port=8060`
*   Only the Hive connector is configured by default
*   You need to set project-id for BigQuery connector in /opt/trino-server/etc/catalog/bigquery.properties
*   High-Availability configuration is discouraged as coordinator is started
    only on `m-0` and other master nodes are idle

