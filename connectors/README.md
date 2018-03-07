# Google Cloud Storage and BigQuery connectors

This initialization action installs specified version of
[Google Cloud Storage connector](https://github.com/GoogleCloudPlatform/bigdata-interop/tree/master/gcs) and updates
[BigQuery connector](https://github.com/GoogleCloudPlatform/bigdata-interop/tree/master/bigquery)
to the matching version from the same [release](https://github.com/GoogleCloudPlatform/bigdata-interop) on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with specific version of GCS connector installed:
```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/connectors/connectors.sh \
    --metadata 'gcs-connector-version=1.7.0'
```

This script downloads specified versions of GCS connector and matching version of BigQuery connector (if BigQuery connector
was installed before) from the same [release](https://github.com/GoogleCloudPlatform/bigdata-interop/releases).

To specify connector version, find the needed released GCS connector version from the
[connectors releases page](https://github.com/GoogleCloudPlatform/bigdata-interop/releases),
and set it as `gcs-connector-version` metadata key value.

If cluster already has BigQuery connector installed then it will be updated together with GCS connector, otherwise BigQuery
connector will not be updated.

For example, if GCS connector 1.7.0 version will be specified then BigQuery connector will be updated to 0.11.0 version, because they are from the same [release](https://github.com/GoogleCloudPlatform/bigdata-interop/releases).

You can find more information about using initialization actions with Dataproc in the
[Dataproc documentation](https://cloud.google.com/dataproc/init-actions).
