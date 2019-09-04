# Alluxio

This initialization action installs Alluxio (https://www.alluxio.io/) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
The master Cloud Dataproc node will be the Alluxio master and all Cloud Dataproc workers will be Alluxio workers.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Alluxio installed:

1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.
  ```bash
  gcloud dataproc clusters create <cluster_name> \
    --initialization-actions gs://$my_bucket/alluxio/alluxio.sh \
    --metadata root_ufs_uri=<UNDERSTORAGE_ADDRESS>
  ```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Notes
* This script must be updated to specify the Alluxio version to install.
* `root_ufs_uri` is a required parameter to specify the root under storage location for Alluxio.
* Additional properties can be specified using the metadata key `delimited_properties` delimited using `;` as follows:
  ```bash
  gcloud dataproc clusters create <cluster_name> \
    --initialization-actions gs://$my_bucket/alluxio/alluxio.sh \
    --metadata root_ufs_uri=<understorage address>,delimited_properties="alluxio.master.mount.table.root.option.fs.gcs.accessKeyId=<GCS_ACCESS_KEY_ID>;alluxio.master.mount.table.root.option.fs.gcs.secretAccessKey=<GCS_SECRET_ACCESS_KEY>"
  ```
