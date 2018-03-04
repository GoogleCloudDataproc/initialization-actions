# BigQuery and Google Cloud Storage connectors

This initialization action installs specified versions of [BigQuery and Google Cloud Storage connectors](https://github.com/GoogleCloudPlatform/bigdata-interop)
on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization to create a new Dataproc cluster with specific versions of BigQuery and/or GCS connectors installed.

```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/connectors/connectors.sh \
    --metadata 'gcs-connector-version=1.7.0'
```

This script downloads specified versions of BigQuery and/or GCS connectors.
To specify connector version, find the needed released connector versions from the
[BigQuery and GCS connectors releases page](https://github.com/GoogleCloudPlatform/bigdata-interop/releases),
and set one or both metadata keys `gcs-connector-version` and/or `bigquery-connector-version`.
If version only for one connector (BigQuery or GCS) is set then only this connector will be installed, but both old BigQuery and GCS connectors will be removed.
If both versions set then both connectors will be installed and in this case they should be from the same release.

For example, for BigQuery connector 0.11.0 and GCS connector 1.7.0:
```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/connectors/connectors.sh \
    --metadata 'bigquery-connector-version=0.11.0' 
    --metadata 'gcs-connector-version=1.7.0'
```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).
