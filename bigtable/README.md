# Google Cloud Bigtable via Apache HBase

This initialization action installs Apache HBase libraries and the
[Google Cloud Bigtable](https://cloud.google.com/bigtable/)
[HBase Client](https://github.com/GoogleCloudPlatform/cloud-bigtable-client).

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a Dataproc cluster configured
to connect to Cloud Bigtable:

1.  Create a Bigtable instance by following
    [these directions](https://cloud.google.com/bigtable/docs/creating-instance).
1.  Using the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/bigtable/bigtable.sh \
        --metadata bigtable-instance=<BIGTABLE INSTANCE>
    ```

1.  The cluster will have HBase libraries, the Bigtable client, and the
    [Apache Spark - Apache HBase Connector](https://github.com/hortonworks-spark/shc)
    installed.

1.  In addition to running Hadoop and Spark jobs, you can SSH to the master
    (`gcloud compute ssh ${CLUSTER_NAME}-m`) and use `hbase shell` to
    [connect](https://cloud.google.com/bigtable/docs/installing-hbase-shell#connect)
    to your Bigtable instance.

## Running an example MR job on cluster

1.  Get the code:

    ```bash
    git clone https://github.com/GoogleCloudPlatform/cloud-bigtable-examples/
    ```

1.  Compile the example. This will create two jars: with and without
    dependencies included.

    ```bash
    cd cloud-bigtable-examples/java/dataproc-wordcount/
    mvn clean package -Dbigtable.projectID=<BIGTABLE PROJECT> -Dbigtable.instanceID=<BIGTABLE INSTANCE>
    ```

1.  Submit the jar with dependencies as a Dataproc job. Note that `OUTPUT_TABLE`
    should not already exist. This job will create the table with the correct
    column family.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc jobs submit hadoop --cluster ${CLUSTER_NAME} \
        --class com.example.bigtable.sample.WordCountDriver
        --jars target/wordcount-mapreduce-0-SNAPSHOT-jar-with-dependencies.jar \
        -- \
        wordcount-hbase gs://goog-dataproc-initialization-actions-${REGION}/README.md <OUTPUT_TABLE>
    ```

## Running an example Spark job on cluster using SHC

See
[Apache Spark - Apache HBase Connector](https://github.com/hortonworks-spark/shc)
for more information on using this connector in your own Spark jobs.

1.  Submit the example jar as a Dataproc job:

    ```bash
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc jobs submit spark --cluster ${CLUSTER_NAME} \
        --class org.apache.spark.sql.execution.datasources.hbase.examples.HBaseSource \
        --jars file:///usr/lib/spark/examples/jars/shc-examples.jar
    ```

## Important notes

*   You can edit and upload your own copy of `bigtable.sh` to Google Cloud
    Storage and use that instead.
*   If you wish to use an instance in another project you can specify
    `--metadata bigtable-project=<PROJECT>` (this will set
    `google.bigtable.project.id`). Make sure your cluster's
    [service account](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts)
    is authorized to access the instance, by default service account that
    created cluster is being used.
*   If you specify custom service account scopes, make sure to add
    [appropriate Bigtable scopes](https://cloud.google.com/bigtable/docs/creating-compute-instance#choosing_title_short_scopes)
    or `cloud-platform`. Clusters have `bigtable.admin.table` and
    `bigtable.data` by default.
*   Apache Spark - Apache HBase Connector version is `1.1.1-2.1-s_2.11`.
