# Google Cloud Bigtable via Apache HBase
This initialization action installs Apache HBase libraries and the [Google Cloud Bigtable](https://cloud.google.com/bigtable/) [HBase Client](https://github.com/GoogleCloudPlatform/cloud-bigtable-client).


## Using this initialization action
You can use this initialization action to create a Dataproc cluster configured to connect to Cloud Bigtable:

1. Create a Bigtable instance by following [these directions](https://cloud.google.com/bigtable/docs/creating-instance).
2. Using the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://dataproc-initialization-actions/bigtable/bigtable.sh \
    --metadata bigtable-instance=<BIGTABLE INSTANCE>
    ```
3. When it is finished being created the cluster will have HBase libraries and the Bigtable Client.
4. You can validate the deployment by SSHing to the master (`gcloud compute ssh <CLUSTER_NAME>-m`) and running `hbase shell`.

## Running example MR job on cluster
1. Example code can be get from [cloud-bigtable-examples](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples/tree/master/java/dataproc-wordcount).
2. Run compilation with: 
    ```bash
    mvn clean package -Dbigtable.projectID=<BIGTABLE PROJECT> -Dbigtable.instanceID=<BIGTABLE INSTANCE>
    ```
3. The job will create two jars - with and without dependencies included.
4. Put some file on hdfs using:
    ```bash
    gcloud compute ssh bigtable-m --command 'hdfs dfs -copyFromLocal <FILE_NAME> /tmp/<FILE_NAME>'
    ```
5. Use the jar with dependecies to submit job:
   
    ```bash
    gcloud dataproc jobs submit hadoop --cluster <CLUSTER_NAME> --class com.example.bigtable.sample.WordCountDriver --jars target/wordcount-mapreduce-0-SNAPSHOT-jar-with-dependencies.jar -- wordcount-hbase /tmp/<FILE_NAME> <HBASE_TABLE>
    ```

## Important notes
* You can edit and upload your own copy of `bigtable.sh` to Google Cloud Storage and use that instead.
* If you wish to use an instance in another project you can specify `--metadata bigtable-project=<PROJECT>` (this will set `google.bigtable.project.id`). Make sure your cluster's [service account](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts) is authorized to access the instance, by default service account that created cluster is being used.
* If you specify custom service account scopes, make sure to add [appropriate Bigtable scopes](https://cloud.google.com/bigtable/docs/creating-compute-instance#choosing_title_short_scopes) or `cloud-platform`. Clusters have `bigtable.admin.table` and `bigtable.data`, by default.