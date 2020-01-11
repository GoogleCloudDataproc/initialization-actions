# Alluxio

This initialization action installs Alluxio (https://www.alluxio.io/) on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The master
Cloud Dataproc node will be the Alluxio master and all Cloud Dataproc workers
will be Alluxio workers.

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with
Alluxio installed:

1.  Using the `gcloud` command to create a new cluster with this initialization
    action. The following command will create a new cluster named
    `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <cluster_name> \
      --initialization-actions gs://$my_bucket/alluxio/alluxio.sh \
      --metadata alluxio_root_ufs_uri=<UNDERSTORAGE_ADDRESS>
    ```

You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Spark on Alluxio

To run a Spark application accessing data from Alluxio, simply refer to the path
as `alluxio://<cluster_name>-m:19998/<path_to_file>`; where `<cluster_name>-m`
is the dataproc master hostname. Refer to Alluxio on Spark
[documentation](https://docs.alluxio.io/os/user/stable/en/compute/Spark.html#examples-use-alluxio-as-input-and-output)
for additional getting started resources.

## Presto on Alluxio

If installing the optional Presto component, Presto must be installed before
Alluxio. Initialization action are executed sequentially and the Presto action
must precede the Alluxio action.

## Notes

*   This script must be updated to specify the Alluxio version to install.
*   `alluxio_version` is an an optional parameter to override the default
    Alluxio version to install.
*   `alluxio_root_ufs_uri` is a required parameter to specify the root under
    storage location for Alluxio.
*   Additional properties can be specified using the metadata key
    `alluxio_site_properties` delimited using `;`.

    ```bash
    gcloud dataproc clusters create <cluster_name> \
      --initialization-actions gs://$my_bucket/alluxio/alluxio.sh \
      --metadata alluxio_root_ufs_uri=<UNDERSTORAGE_ADDRESS>
      --metadata alluxio_site_properties="alluxio.master.mount.table.root.option.fs.gcs.accessKeyId=<GCS_ACCESS_KEY_ID>;alluxio.master.mount.table.root.option.fs.gcs.secretAccessKey=<GCS_SECRET_ACCESS_KEY>"
    ```

*   Additional files can be downloaded into `/opt/alluxio/conf` using the
    metadata key `alluxio_download_files_list` by specifying `http(s)` or `gs`
    uris delimited using `;`.

    ```bash
    gcloud dataproc clusters create <cluster_name> \
      --initialization-actions gs://$my_bucket/alluxio/alluxio.sh \
      --metadata alluxio_root_ufs_uri=<UNDERSTORAGE_ADDRESS> \
      --metadata alluxio_download_files_list="gs://$my_bucket/$my_file;https://$server/$file"
    ```
