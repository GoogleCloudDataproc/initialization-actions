--------------------------------------------------------------------------------

# NOTE: *The Zookeeper initialization action has been deprecated. Please use the Zookeeper Component*

**The
[Zookeeper Component](https://cloud.google.com/dataproc/docs/concepts/components/zookeeper)
is the best way to use Apache Zookeeper with Cloud Dataproc. To learn more about
Dataproc Components see
[here](https://cloud.google.com/dataproc/docs/concepts/components/overview).**

--------------------------------------------------------------------------------

# ZooKeeper

Though High Availability clusters have ZooKeeper pre-configured, Standard [Google Cloud Dataproc](https://cloud.google.com) clusters do not have [ZooKeeper](http://zookeeper.apache.org) insalled. This may change in the future; in the interim, this initialization action installs ZooKeeper on a Standard and Single node Cloud Dataproc cluster.

This script installs ZooKeeper on the **three required** nodes for a Cloud Dataproc Cluster:

* Master node (`-m`)
* Worker 1 (`-w-0`)
* Worker 2 (`-w-1`)

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with ZooKeeper installed:

1. Use the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/zookeeper/zookeeper.sh \
        [--properties zookeeper:<key>=<value>,...]
    ```

1. Once the cluster has been created, ZooKeeper is configured to run on port `2181` (though you can change this in the script.)

## Important notes
* This script does not optimize ZooKeeper and you may wish to further configure ZooKeeper based on the [official documentation](https://zookeeper.apache.org/doc/trunk/).
* This script will work on Single Node clusters with Zookeeper Server running on single instance.
* This script should not be run on High Availability clusters, which already have Zookeeper pre-configured.
