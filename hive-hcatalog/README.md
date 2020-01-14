# HCatalog initialization action

This initialization action installs [Hive HCatalog](https://cwiki.apache.org/confluence/display/Hive/HCatalog) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The script will also configure Pig to use HCatalog.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Cloud Dataproc cluster with HCatalog installed by doing the following.

1. Use the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/hive-hcatalog/hive-hcatalog.sh
    ```

1. Once the cluster has been created HCatalog should be installed and configured for use with Pig.

