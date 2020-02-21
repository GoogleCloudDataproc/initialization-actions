# H2O Sparkling Water Initialization Action

This [initialization action](https://cloud.google.com/dataproc/init-actions)
installs
[H2O Sparkling Water](http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/deployment/sw_google_cloud_dataproc.html)
on all nodes of [Google Cloud Dataproc](https://cloud.google.com/dataproc)
cluster. This initialization works with Dataproc image version `1.3` and newer.

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with H2O
Sparkling Water installed by:

1.  Use the `gcloud` command to create a new cluster with this initialization
    action. The following command will create a new cluster named
    `<CLUSTER_NAME>`.

    To create Dataproc 1.3 cluster and older use `conda` initialization action:

    ```bash
    MY_BUCKET=<gcs-bucket>
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --image-version 1.3 \
        --metadata 'H2O_SPARKLING_WATER_VERSION=3.28.0.1-1' \
        --scopes "https://www.googleapis.com/auth/cloud-platform" \
        --initialization-actions "gs://$MY_BUCKET/conda/bootstrap-conda.sh,gs://$MY_BUCKET/h2o/h2o.sh"
    ```

    To create Dataproc 1.4 cluster and newer use `ANACONDA` optional component:

    ```bash
    MY_BUCKET=<gcs-bucket>
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --image-version 1.4 \
        --optional-components ANACONDA \
        --metadata 'H2O_SPARKLING_WATER_VERSION=3.28.0.3-1' \
        --scopes "https://www.googleapis.com/auth/cloud-platform" \
        --initialization-actions "gs://$MY_BUCKET/h2o/h2o.sh"
    ```

1.  Submit sample job.

    ```bash
    MY_BUCKET=<gcs-bucket>
    gcloud dataproc jobs submit pyspark --cluster <CLUSTER_NAME> "gs://$MY_BUCKET/h2o/sample-script.py"
    ```
