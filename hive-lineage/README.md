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
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/hive-lineage/hive-lineage.sh
```

If you want to run Hive jobs involving bigquery tables, hive-bigquery connector needs to be installed as well.
See [connectors](../connectors/README.md) init action.

```shell 
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region ${REGION} \
  --scopes cloud-platform \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh,gs://goog-dataproc-initialization-actions-${REGION}/hive-lineage/hive-lineage.sh \
  --metadata hive-bigquery-connector-version=2.0.3
```