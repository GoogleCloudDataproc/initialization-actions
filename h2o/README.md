# H2O Sparkling Water Initialization Action

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs version
`3.26.2-2.4` of [H2O Sparkling Water](http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/deployment/sw_google_cloud_dataproc.html) on all nodes within
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster with version `1.4-debian9`.

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with H2O Sparkling Water installed by:

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`. Update cluster attributes like zone subnet etc. as needed.

    ```bash
    gcloud beta dataproc clusters create <CLUSTER_NAME> \
    --enable-component-gateway \
    --subnet default \
    --zone us-central1-a \
    --image-version 1.4-debian9 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --initialization-actions 'gs://<GCS BUCKET>/<PATH>/h2o-dataproc-install-python_pip.sh','gs://<GCS BUCKET>/<PATH>/h2o-dataproc-tune.sh'
    ```

    You can also install Anaconda as an optional component and use it with H2O.

    ```bash
    gcloud beta dataproc clusters create <CLUSTER_NAME> \
    --enable-component-gateway \
    --subnet default \
    --zone us-central1-a \
    --image-version 1.4-debian9 \
    --optional-components ANACONDA \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --initialization-actions 'gs://<GCS BUCKET>/<PATH>/h2o-dataproc-install-conda_pip.sh','gs://<GCS BUCKET>/<PATH>/h2o-dataproc-tune.sh'
    ```

1. Submit jobs (assuming above mentioned cluster was created with [global endpoint](https://cloud.google.com/dataproc/docs/concepts/regional-endpoints))

    ```bash
    gcloud dataproc jobs submit pyspark --cluster <CLUSTER_NAME> --region global  sample-script.py
    ```
