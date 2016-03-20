# ZooKeeper 

Presently, [Google Cloud Dataproc](https://cloud.google.com) clusters do not have [ZooKeeper](http://zookeeper.apache.org) insalled. This may change in the future; in the interim, this initialization action installs ZooKeeper on a Cloud Dataproc cluster.

Specifically, this script will (unless changed) install ZooKeeper on the **three required** nodes for a Cloud Dataproc Cluster:

* Master node (`-m`)
* Worker 1 (`-w-0`)
* Worker 2 (`-w-1`)

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with ZooKeeper installed by:

1. Uploading a copy of this initialization action (`zookeeper.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. **Note** - you may need to increase the `initialization-action-timeout` if this script takes a long time to run. The following command will create a new cluster named `<CLUSTER_NAME>`, and specify the initialization action stored in `<GCS_BUCKET>`.
   
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/zookeeper.sh
    ```
    Since ZooKeeper installs quickly, it's unlikely you will need to increase the `initialization-action-timeout` unless you have a very large cluster or are running more than one initialization action.
1. Once the cluster has been created, ZooKeeper is configured to run on port `2181` (though you can change this int he script.)

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script does not optimize ZooKeeper and you may wish to further configure ZooKeeper based on the [official documentation](https://zookeeper.apache.org/doc/trunk/).
