# spark-bigquery-connector (Beta)

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs the latest version of the [spark-bigquery-connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)
on all nodes within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. With this initialization action, you will no longer need to include this package with the spark `--packages` flag when submitting jobs to your cluster.

## Using this initialization action

You can use this initialization action to create a new Cloud Dataproc cluster with the latest version of the [spark-bigquery-connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) installed. 

Example 1: You can use the `gcloud` command to create a new cluster with this initialization action.  The following command will create a new cluster with the latest version of the connector installed:

    ```bash
    gcloud dataproc clusters create my_cluster \
    --region ${REGION} \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/spark-bigquery-connector/install_connector.sh
    ```
Example 2: You can install any version of the connector that's available in the public repo gs://spark-lib/bigquery with the metadata key `SPARK_BIGQUERY_CONNECTOR_VERSION` by running the following command: 

    ```bash
    gcloud dataproc clusters create my_cluster \
    --region ${REGION} \
    --metadata 'SPARK_BIGQUERY_CONNECTOR_VERSION=spark-bigquery-latest_2.12.jar' 
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/spark-bigquery-connector/install_connector.sh
    ```

## For more information

Please refer to the [spark-bigquery-connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) repo for more information and other ways to utilize the connector in your projects. 