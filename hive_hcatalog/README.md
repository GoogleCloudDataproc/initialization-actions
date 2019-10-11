# HCatalog initialization action

This initialization action installs [Hive HCatalog](https://cwiki.apache.org/confluence/display/Hive/HCatalog) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The script will also configure Pig to use HCatalog.

## Using this initialization action

You can use this initialization action to create a new Cloud Dataproc cluster with HCatalog installed by doing the following.

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER-NAME> \
      --initialization-actions gs://$MY_BUCKET/hive-hcatalog/hive-hcatalog.sh
    ```

1. Once the cluster has been created HCatalog should be installed and configured for use with Pig.

