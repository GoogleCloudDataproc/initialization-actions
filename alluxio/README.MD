# Alluxio

This initialization action installs Alluxio (https://www.alluxio.io/) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
The master Cloud Dataproc node will be the Alluxio master and all Cloud Dataproc workers will be Alluxio workers.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Alluxio installed:

1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://$MY_BUCKET/alluxio/alluxio.sh
    ```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* This script must be updated to specify the Alluxio version and configuration to install.
