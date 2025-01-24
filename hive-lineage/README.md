# Hive Lineage Initialization Action

## Using the initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with Lineage enabled for Hive jobs.
Note that this feature is in preview for now.

```shell 
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region ${REGION} \
  --scopes cloud-platform \
  --initialization-actions gs://dataproc-hive-lineage-prototype/v3/initialization-actions/enable_lineage.sh
```

(TODO: Update the gs bucket with the regionalized `goog-dataproc-initialization-actions-`)

If you want to run Hive jobs involving bigquery tables, hive-bigquery connector needs to be installed as well.
(TODO: Link to connectors init action)

```shell 
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region ${REGION} \
  --scopes cloud-platform \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh,gs://dataproc-hive-lineage-prototype/v3/initialization-actions/enable_lineage.sh \
  --metadata hive-bigquery-connector-version=2.0.3
```

(TODO: Once multiple versions are supported, update instructions accordingly)