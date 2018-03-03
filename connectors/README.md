# BigQuery and GCS connectors

This initialization action installs updates [BigDL](https://github.com/intel-analytics/BigDL)
on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization to create a new Dataproc cluster with latest BigQuery and GCS connectors installed.

```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/connectors/connectors.sh
```

This script downloads latest released BigQuery and GCS connector versions.
To download a different connector versions, find the needed released connector versions from the
[BigQuery and GCS connectors releases page](https://github.com/GoogleCloudPlatform/bigdata-interop/releases),
and set the metadata keys `gcs-connector-version` and `bigquery-connector-version`. The URL should end in `-dist.zip`.
If only one will be provided, only one will be installed, but both old BigQuery and GCS connectors will be deleted.
If both provided both will be installed and they should be from the same release.

For example, for BigQuery connector v0.11.0 and GCS connectro v1.7.0:

```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/connectors/connectors.sh \
    --metadata 'gcs-connector-version=1.7.0'
    --metadata 'bigquery-connector-version=0.11.0'
```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important Notes

