# HCatalog initialization action

This initialization action installs [Hive HCatalog](https://cwiki.apache.org/confluence/display/Hive/HCatalog) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The script will also configure Pig to use HCatalog.

## Using this initialization action

You can use this initialization action to create a new Cloud Dataproc cluster with HCatalog installed by doing the following.

1. Uploading a copy of this initialization action (`hive-hcatalog.sh`) to [Google Cloud Storage](https://cloud.google.com/storage). Alternatively, you can use the Google-hosted copy of this initialization action at `gs://dataproc-initialization-actions/hive-hcatalog/hive-hcatalog.sh`
2. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER-NAME>`, specify the initialization action stored in `<GCS-BUCKET>`:

    ```bash
    gcloud dataproc clusters create <CLUSTER-NAME> \
    --initialization-actions gs://<GCS-BUCKET>/hive-hcatalog.sh
    ```
3. Once the cluster has been created HCatalog should be installed and configured for use with Pig.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).
