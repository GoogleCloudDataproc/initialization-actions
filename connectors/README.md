# Google Cloud Storage and BigQuery connectors

This initialization action installs specified versions of
[Google Cloud Storage connector](https://github.com/GoogleCloudPlatform/bigdata-interop/tree/master/gcs)
and [BigQuery connector](https://github.com/GoogleCloudPlatform/bigdata-interop/tree/master/bigquery)
on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with specific version of
Google Cloud Storage and BigQuery connector installed:
```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/connectors/connectors.sh \
    --metadata 'gcs-connector-version=1.7.0' \
    --metadata 'bigquery-connector-version=0.11.0'   
```

This script downloads specified version of Google Cloud Storage and BigQuery connector and deletes
old version of these connectors.

To specify connector version, find the needed released connector version on the
[connectors releases page](https://github.com/GoogleCloudPlatform/bigdata-interop/releases),
and set it as the `gcs-connector-version` or `bigquery-connector-version` metadata key value.

If only one connector version is specified (Google Cloud Storage or Bigquery) then only this connector
will be updated.

For example, if Google Cloud Storage connector 1.7.0 version is specified and BigQuery connector version
is not specified, then only Google Cloud Storage connector will be updated and BigQuery connector will be
left intact:
```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/connectors/connectors.sh \
    --metadata 'gcs-connector-version=1.7.0'
```

You can find more information about using initialization actions with Dataproc in the
[Dataproc documentation](https://cloud.google.com/dataproc/init-actions).
