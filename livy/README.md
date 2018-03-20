# Apache Livy Initialization Action

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs version
`0.5.0` of [Apache Livy](https://livy.incubator.apache.org/) on a master node within a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Livy installed by:

1. Using the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-actions gs://dataproc-initialization-actions/livy/livy.sh
    ```

Alternatively, you can start a regular dataproc cluster,
[ssh to the master node](https://cloud.google.com/dataproc/submit-job) (see SSH into instance),
clone this repository and run ./livy.sh (as sudo)

2. Once the cluster has been created, Livy is configured to run on port `8998` on the master node
   in a Dataproc cluster.

3. To learn about how to use Livy read the documentation for the
   [Rest API](https://livy.incubator.apache.org/docs/latest/rest-api.html)
