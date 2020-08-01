# Presto

This initialization action installs
[Starburst Presto](https://www.starburstdata.com) on a
[Dataproc](https://cloud.google.com/dataproc) cluster.
This script also configures Presto to work with Apache Hive on the
cluster. The master Dataproc node will be the coordinator, and all
Dataproc workers will be Presto workers.

## Using this initialization action

**:warning: NOTICE:** See [How initialization actions are used](/README.md#how-initialization-actions-are-used) and [Important considerations and guidelines](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions#important_considerations_and_guidelines) for additional information.

Use this initialization action to create a Dataproc cluster with Presto installed:

1.  Use the `gcloud` command to create a new cluster that runs this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/starburst-presto/presto.sh
    ```

1.  Presto is configured to run on port `8080` on the cluster'a master node (you can change the
    port assignment in the script). To connect to the Presto web interface,
    create an SSH tunnel and use a SOCKS5 Proxy&mdash; see
    [Dataproc cluster web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces).
    You can also use the [`presto` command line interface](https://docs.starburstdata.com/latest/installation/cli.html)
    on the master node.

## Important notes

*   Update the script to specify a Presto version to install.
*   You can adjust the memory settings in `jvm.config`.
*   By default, Presto uses HTTP port `8080`. Use `presto-port` metadata value to configure a different port, for example,
    `--metadata presto-port=8060`.
*   The Hive connector is configured by default.
*   [Dataproc High-Availability mode](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/high-availability)
    is not recommended because the coordinator is started
    only on `m-0`, and other master nodes will be idle.

