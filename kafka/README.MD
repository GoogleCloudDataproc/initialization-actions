# Kafka 

This initialization action installs [Kafka](http://kafka.apache.org) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. To install Kafka you must first install ZooKeeper. In the future Cloud Dataproc clusters may include ZooKeeper by default but in the meantime you can use the ZooKeeper initialization action in [this GitHub repository](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions).


## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Kafka installed by:

1. Uploading a copy of this initialization action (`kafka.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
1. Using the `gcloud` command to create a new cluster with this initialization action. **Note** - you may need to increase the `initialization-action-timeout` if this script takes a long time to run. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`, and increase the timeout to 5 minutes.
   
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://<GCS_BUCKET>/kafka.sh   
    --initialization-action-timeout 5m
    ```
1. Once the cluster has been created Kafka should be running on all worker nodes in the cluster. Kafka will use the three ZooKeeper nodes setup in `zookeeper.sh`.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script will install Kafka on all **worker nodes** by default (but not the master)
* This script assumes you have run the `zookeeper.sh` script to instal ZooKeeper on the master and two (required) worker nodes
* The `delete.topic.enable` property has been set to `true` by default so topics can be deleted
