--------------------------------------------------------------------------------

# NOTE: *The Presto initialization action has been deprecated. Please use the Presto Component*

**The
[Presto Component](https://cloud.google.com/dataproc/docs/concepts/components/presto)
is the best way to use Presto with Cloud Dataproc. To learn more about Dataproc
Components see
[here](https://cloud.google.com/dataproc/docs/concepts/components/overview).**

--------------------------------------------------------------------------------

# Presto

This initialization action installs the latest version of [Presto](prestodb.io)
on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
Additionally, this script will configure Presto to work with Hive on the
cluster. The master Cloud Dataproc node will be the coordinator and all Cloud
Dataproc workers will be Presto workers.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
Presto installed:

1.  Using the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/presto/presto.sh
    ```

1.  Once the cluster has been created, Presto is configured to run on port
    `8080` (though you can change this in the script) on the master node in a
    Cloud Dataproc cluster. To connect to the Presto web interface, you will
    need to create an SSH tunnel and use a SOCKS 5 Proxy as described in the
    [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces)
    documentation. You can also use the
    [Presto command line interface](https://prestodb.io/docs/current/installation/cli.html)
    using the `presto` command on the master node.

You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes

*   This script must be updated based on which Presto version you wish to
    install
*   You may need to adjust the memory settings in `jvm.config` based on your
    needs
*   Presto is set to use HTTP port `8080` by default, but can be changed using
    `--metadata presto-port=8060`
*   Only the Hive connector is configured by default
*   High-Availability configuration is discouraged as coordinator is started
    only on `m-0` and other master nodes are idle

