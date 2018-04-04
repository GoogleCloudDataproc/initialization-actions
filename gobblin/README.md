# Apache Gobblin Initialization Action

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs version
`0.12.0 RC2` of [Apache Gobblin](https://gobblin.incubator.apache.org/) on all nodes within
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Gobblin installed by:

1. Using the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-actions gs://dataproc-initialization-actions/gobblin/gobblin.sh
    ```

2. Submit jobs

    ```bash
    gcloud dataproc jobs submit hadoop --cluster=<CLUSTER_NAME> \
        --class org.apache.gobblin.runtime.mapreduce.CliMRJobLauncher \
        --properties mapreduce.job.user.classpath.first=true -- \
        -sysconfig /usr/local/lib/gobblin/conf/gobblin-mapreduce.properties \
        -jobconfig gs://<PATH_TO_JOB_CONFIG>
    ```

   Alternatively, you can submit jobs through Gobblin launcher scripts located in
   `/usr/local/lib/gobblin/bin`. By default, Gobblin is only configured for mapreduce mode.

3. To learn about how to use Gobblin read the documentation for the
   [Getting Started](https://gobblin.readthedocs.io/en/latest/) guide.

## Important Notes

1. For Gobblin to work with Dataproc Job API, any additional client libraries
   (for example: Kafka, MySql) would have to be symlinked into `/usr/lib/hadoop/lib` directory on
   each node.

