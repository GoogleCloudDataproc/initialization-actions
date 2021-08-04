# Google Cloud Storage connector

You can update Cloud Storage connector on Dataproc clusters through
`GCS_CONNECTOR_VERSION` metadata value on supported Dataproc images without
using initialization actions:

```shell
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --metadata GCS_CONNECTOR_VERSION=2.2.2
```

# BigQuery connectors

This initialization action installs specified versions of
[Hadoop BigQuery connector](https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/bigquery)
and
[Spark BigQuery connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)
on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with an
updated Hadoop BigQuery connector and Spark BigQuery connector installed:

*   to install connector by specifying version, use `bigquery-connector-version`
    and `spark-bigquery-connector-version` metadata values:

    ```shell
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
        --metadata bigquery-connector-version=1.2.0 \
        --metadata spark-bigquery-connector-version=0.21.0
    ```

*   to update connector by specifying URL, use `bigquery-connector-url`and
    `spark-bigquery-connector-url` metadata values:

    ```shell
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
        --metadata bigquery-connector-url=gs://path/to/custom/hadoop/bigquery/connector.jar \
        --metadata spark-bigquery-connector-url=gs://path/to/custom/spark/bigquery/connector.jar
    ```

This script downloads specified Hadoop BigQuery connector and Spark BigQuery
connector and deletes an old version of these connectors if they were installed.

To specify connector version, find the connector version on the
[Hadoop connectors releases page](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases)
and
[Spark BigQuery connector releases page](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases),
and set it as the `bigquery-connector-version` or
`spark-bigquery-connector-version` metadata key value.

If only one connector version is specified (Hadoop BigQuery or Spark BigQuery)
then only this connector will be updated.

For example:

*   if Spark BigQuery connector 0.21.1 version is specified and Hadoop BigQuery
    connector version is not specified, then only Spark BigQuery connector
    version 0.21.1 will be installed:

    ```shell
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
        --metadata spark-bigquery-connector-version=0.21.1
    ```
