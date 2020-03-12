--------------------------------------------------------------------------------

# NOTE: *Updating Cloud Storage connector with this initialization action is not recommended*

**You can update Cloud Storage Connector through `GCS_CONNECTOR_VERSION`
metadata value on supported Dataproc images.**

--------------------------------------------------------------------------------

# Google Cloud Storage and BigQuery connectors

This initialization action installs specified versions of
[Google Cloud Storage connector](https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/gcs),
[Hadoop BigQuery connector](https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/bigquery)
and
[Spark BigQuery connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)
on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with an
updated Google Cloud Storage connector, Hadoop BigQuery connector and Spark
BigQuery connector installed:

-   to update connector by specifying version, use `gcs-connector-version`,
    `bigquery-connector-version` and `spark-bigquery-connector-version` metadata
    values:

    ```
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
        --metadata gcs-connector-version=2.1.1 \
        --metadata bigquery-connector-version=1.1.1 \
        --metadata spark-bigquery-connector-version=0.13.1-beta
    ```

-   to update connector by specifying URL, use `gcs-connector-url`,
    `bigquery-connector-url`and `spark-bigquery-connector-url` metadata values:

    ```
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
        --metadata gcs-connector-url=gs://path/to/custom/gcs/connector.jar \
        --metadata bigquery-connector-url=gs://path/to/custom/hadoop/bigquery/connector.jar \
        --metadata spark-bigquery-connector-url=gs://path/to/custom/spark/bigquery/connector.jar
    ```

This script downloads specified Google Cloud Storage connector, Hadoop BigQuery
connector and Spark BigQuery connector and deletes an old version of these
connectors if they were installed.

To specify connector version, find the connector version on the
[Hadoop connectors releases page](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases)
and
[Spark BigQuery connector releases page](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases),
and set it as the `gcs-connector-version`, `bigquery-connector-version` or
`spark-bigquery-connector-version` metadata key value.

If only one connector version is specified (Google Cloud Storage, Hadoop
BigQuery or Spark BigQuery) then only this connector will be updated.

For example:

*   if Google Cloud Storage connector 2.1.1 version is specified and neither
    Hadoop BigQuery connector not Spark BigQuery connector versions are
    specified, then only Google Cloud Storage connector will be updated to 2.1.1
    version:

    ```
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
        --metadata gcs-connector-version=2.1.1
    ```
