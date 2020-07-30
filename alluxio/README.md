# Alluxio

This initialization action installs Alluxio (https://www.alluxio.io/) on a
[Dataproc](https://cloud.google.com/dataproc) cluster. The master
Dataproc node will be the Alluxio master, and all Dataproc workers
will be Alluxio workers.

## Using this initialization action

**:warning: NOTICE:** See [How initialization actions are used](/README.md#how-initialization-actions-are-used) and [Important considerations and guidelines](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions#important_considerations_and_guidelines) for additional information.

You can use this initialization action to create a new Dataproc cluster with
Alluxio installed:

1.  Using the `gcloud` command to create a new cluster that runs this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/alluxio/alluxio.sh \
        --metadata alluxio_root_ufs_uri=<UNDERSTORAGE_ADDRESS>
    ```

See the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions) for more information.

## Spark on Alluxio

To run a Spark application accessing data from Alluxio, refer to the path
as `alluxio://<cluster_name>-m:19998/<path_to_file>`; where `<cluster_name>-m`
is the Dataproc master hostname. See the
[Alluxio on Spark documentation](https://docs.alluxio.io/os/user/stable/en/compute/Spark.html#examples-use-alluxio-as-input-and-output)
for additional resources.

## Presto on Alluxio

Presto must be installed before Alluxio. Use of the [Optional Dataproc Presto component](https://cloud.google.com/dataproc/docs/concepts/components/presto) is recommended for faster component installation. Optional components are installed on the cluster before initialization actions are run; multiple initialization actions are installed on each node in the order specified in the `gcloud dataproc clusters create` command.

## Notes

*   `alluxio_version` is an an optional parameter to override the default
    Alluxio version that otherwise will be installed.
*   `alluxio_root_ufs_uri` is a required parameter to specify the root under
    the storage location for Alluxio.
*   Additional properties can be specified using the metadata key,
    `alluxio_site_properties`, delimited using `;`.

    ```bash
    REGION=<region>
    CLUSTER=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/alluxio/alluxio.sh \
        --metadata alluxio_root_ufs_uri=<UNDERSTORAGE_ADDRESS>
        --metadata alluxio_site_properties="alluxio.master.mount.table.root.option.fs.gcs.accessKeyId=<GCS_ACCESS_KEY_ID>;alluxio.master.mount.table.root.option.fs.gcs.secretAccessKey=<GCS_SECRET_ACCESS_KEY>"
    ```

*   Additional files can be downloaded into `/opt/alluxio/conf` using the
    metadata key, `alluxio_download_files_list`, specifying `http(s)` or `gs`
    URIs delimited with `;`.

    ```bash
    REGION=<region>
    CLUSTER=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/alluxio/alluxio.sh \
        --metadata alluxio_root_ufs_uri=<UNDERSTORAGE_ADDRESS> \
        --metadata alluxio_download_files_list="gs://goog-dataproc-initialization-actions-${REGION}/$my_file;https://$server/$file"
    ```
