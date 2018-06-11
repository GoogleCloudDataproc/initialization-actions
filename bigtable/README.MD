# Google Cloud Bigtable via Apache HBase
This initialization action installs Apache HBase libraries and the [Google Cloud Bigtable](https://cloud.google.com/bigtable/) [HBase Client](https://github.com/GoogleCloudPlatform/cloud-bigtable-client).


## Using this initialization action
You can use this initialization action to create a Dataproc cluster configured to connect to Cloud Bigtable:

1. Create a Bigtable instance by following [these directions](https://cloud.google.com/bigtable/docs/creating-instance).
1. Using the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/bigtable/bigtable.sh \
    --metadata bigtable-instance=<BIGTABLE INSTANCE>
    ```
1. The cluster will have HBase libraries and the Bigtable client after it is created.
1. You can validate the deployment by SSHing to the master (`gcloud compute ssh <CLUSTER_NAME>-m`) and running `hbase shell`.

## Running example MR job on cluster
1. Get the code:
   ```bash
   git clone https://github.com/GoogleCloudPlatform/cloud-bigtable-examples/tree/master/java/dataproc-wordcount
   ```
1. Compile:
    ```bash
    mvn clean package -Dbigtable.projectID=<BIGTABLE PROJECT> -Dbigtable.instanceID=<BIGTABLE INSTANCE>
    ```
1. The job will create two jars - with and without dependencies included.
1. Use the jar with dependecies to submit job:
   
    ```bash
    gcloud dataproc jobs submit hadoop --cluster <CLUSTER_NAME> --class com.example.bigtable.sample.WordCountDriver --jars target/wordcount-mapreduce-0-SNAPSHOT-jar-with-dependencies.jar -- wordcount-hbase gs://dataproc-initialization-actions/README.md <HBASE_TABLE>
    ```

## Running example Spark job on cluster
1. This initialization action adds the Spark-HBase connector to the Spark classpath. Please find more details regarding compilation and example usage on [Apache Spark - Apache HBase Connector](https://github.com/hortonworks-spark/shc)
1. Submit example spark job:
   ```bash
   gcloud dataproc jobs submit spark --cluster <CLUSTER_NAME> --class org.apache.spark.sql.execution.datasources.hbase.examples.HBaseSource --jars examples/target/shc-examples-1.1.2-2.2-s_2.11-SNAPSHOT.jar
   ```

## Important notes
* You can edit and upload your own copy of `bigtable.sh` to Google Cloud Storage and use that instead.
* If you wish to use an instance in another project you can specify `--metadata bigtable-project=<PROJECT>` (this will set `google.bigtable.project.id`). Make sure your cluster's [service account](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts) is authorized to access the instance, by default service account that created cluster is being used.
* If you specify custom service account scopes, make sure to add [appropriate Bigtable scopes](https://cloud.google.com/bigtable/docs/creating-compute-instance#choosing_title_short_scopes) or `cloud-platform`. Clusters have `bigtable.admin.table` and `bigtable.data`, by default.
* Apache Spark - Apache HBase connector version is 1.1.1-2.1-s_2.11.