# Apache Livy Initialization Action

This [initialization action](https://cloud.google.com/dataproc/init-actions)
installs [Apache Livy](https://livy.incubator.apache.org/) on a master node
within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
Livy installed:

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/livy/livy.sh
    ```

1.  To change installed Livy version, use `livy-version` metadata value:
    
    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/livy/livy.sh \
        --metadata livy-version=0.7.0
    ```

1.  Once the cluster has been created, Livy is configured to run on port `8998`
    on the master node in a Dataproc cluster.

1.  To learn about how to use Livy read the documentation for the
    [Rest API](https://livy.incubator.apache.org/docs/latest/rest-api.html)
