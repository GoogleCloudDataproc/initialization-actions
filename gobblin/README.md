# Apache Gobblin Initialization Action

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs version
`0.12.0 RC2` of [Apache Gobblin](https://gobblin.incubator.apache.org/) on all nodes within
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

The distribution is hosted in Dataproc-team owned Google Cloud Storage bucket `gobblin-dist`.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with Gobblin installed by:

1. Use the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gobblin/gobblin.sh
    ```

1. Submit jobs

    ```bash
    gcloud dataproc jobs submit hadoop --cluster=<CLUSTER_NAME> \
        --class org.apache.gobblin.runtime.mapreduce.CliMRJobLauncher \
        --properties mapreduce.job.user.classpath.first=true \
        -- \
        -sysconfig /usr/local/lib/gobblin/conf/gobblin-mapreduce.properties \
        -jobconfig gs://<PATH_TO_JOB_CONFIG>
    ```

   Alternatively, you can submit jobs through Gobblin launcher scripts located in
   `/usr/local/lib/gobblin/bin`. By default, Gobblin is only configured for mapreduce mode.

1. To learn about how to use Gobblin read the documentation for the
   [Getting Started](https://gobblin.readthedocs.io/en/latest/) guide.

## Important notes

1. For Gobblin to work with Dataproc Job API, any additional client libraries
   (for example: Kafka, MySql) would have to be symlinked into `/usr/lib/hadoop/lib` directory on
   each node.

