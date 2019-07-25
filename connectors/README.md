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
    --initialization-actions gs://$MY_BUCKET/connectors/connectors.sh \
    --metadata gcs-connector-version=1.9.16 \
    --metadata bigquery-connector-version=0.13.16
```

This script downloads specified version of Google Cloud Storage and BigQuery connector and deletes
old version of these connectors.

To specify connector version, find the needed released connector version on the
[connectors releases page](https://github.com/GoogleCloudPlatform/bigdata-interop/releases),
and set it as the `gcs-connector-version` or `bigquery-connector-version` metadata key value.

If only one connector version is specified (Google Cloud Storage or Bigquery) then only this connector
will be updated, but Google Cloud Storage connector 1.7.0 and BigQuery connector 0.11.0 are always
updated together if only one of them is specified and another is not specified.

For example:
* if Google Cloud Storage connector 1.7.0 version is specified and BigQuery connector version is not
  specified, then Google Cloud Storage connector will be updated to 1.7.0 version and BigQuery connector
  will be updated to 0.11.0 version:
  ```
  gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://$MY_BUCKET/connectors/connectors.sh \
      --metadata gcs-connector-version=1.7.0
  ```
* if Google Cloud Storage connector 1.8.0 version is specified and BigQuery connector version is not
  specified, then only Google Cloud Storage connector will be updated to 1.8.0 version and BigQuery
  connector will be left intact:
  ```
  gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://$MY_BUCKET/connectors/connectors.sh \
      --metadata gcs-connector-version=1.8.0
  ```
