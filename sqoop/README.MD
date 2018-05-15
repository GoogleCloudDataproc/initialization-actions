# Sqoop 

This initialization action installs [Sqoop](http://sqoop.apache.org/) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. Sqoop is being compiled from trunk branch of official [github](https://github.com/apache/sqoop) mirror.

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Sqoop installed by:

1. Uploading a copy of this initialization action (`sqoop.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
2. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`, specify the initialization action stored in `<GCS_BUCKET>`.
   
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-actions gs://<GCS_BUCKET>/sqoop.sh \
        --initialization-action-timeout 15m
    ```
3. You can also use init action stored in official google bucket.
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-actions gs://dataproc-initialization-actions/sqoop/sqoop.sh \
        --initialization-action-timeout 15m
    ```

## Using Sqoop with Cloud SQL
1. Sqoop can be used with different structured datastores. Here is an example of using Sqoop with as Cloud SQL database. It is using list of init actions to setup cloud-sql-proxy to SQL database located in GCP, and to install hive libraries to make it possible to use sqoop with hive too. Please see: [cloud-sql-proxy](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/cloud-sql-proxy) and [hive](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/hive-hcatalog) for more details.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-action-timeout 15m --initialization-actions \
        gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh,\
        gs://dataproc-initialization-actions/hive-hcatalog/hive-hcatalog.sh,\
        gs://dataproc-initialization-actions/sqoop/sqoop.sh \
        --scopes sql-admin \
        --properties hive:hive.metastore.warehouse.dir=gs://<GCS_BUCKET>/hive-warehouse \
        --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>" 
    ```

2. Then it will possible to import data from cloud SQL to hadoop using command:

    ```bash
    sqoop import --connect jdbc:mysql://localhost/<DB_NAME> --username root --table <TABLE_NAME> --m 1
    ```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Using Sqoop with BigTable
1. Sqoop can be used to import data into BigTable. In order to make it working, bigtable init action must be added to initialization-actions list.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-action-timeout 15m --initialization-actions \
        gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh,\
        gs://dataproc-initialization-actions/hive-hcatalog/hive-hcatalog.sh,\
        gs://dataproc-initialization-actions/bigtable/bigtable.sh,\
        gs://dataproc-initialization-actions/sqoop/sqoop.sh \
        --single-node --master-machine-type n1-standard-4 --scopes sql-admin,\
        "https://www.googleapis.com/auth/bigtable.admin.table","https://www.googleapis.com/auth/bigtable.data"\
        --properties hive:hive.metastore.warehouse.dir=gs://<GCS_BUCKET>/hive-warehouse \
        --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>" \
        --metadata "bigtable-project=my-dataproc-project" --metadata "bigtable-instance=my-big-table" 
    
    ```

2. If cloud-sql-proxy is configured then running import job from cloud SQL to BigTable using HBase client can be done with command:

    ```bash
    sqoop import --connect jdbc:mysql://localhost/<DB_NAME> --username root --table <TABLE_NAME> --columns "<COLUMN_LIST>" --hbase-table <HBASE_TABLE_NAME> --column-family <COLUMN_FAMILY_NAME> -hbase-row-key <ROW_ID> --hbase-create-table --m 1
    ```

## Important notes
* Some databases requires installing sqoop connectors and providing additional arguments in order to run sqoop jobs. See [Sqoop User Guide](http://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html#_compatibility_notes) for more details. Init actions that cooperates with sqoop:
    - [cloud-sql-proxy](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/cloud-sql-proxy)
    - [bigtable](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/bigtable)
* It's necessary to increase default init action timeout to 15 minutes because of additional Sqoop compilation time of 10 minutes. 
* This init action uses Sqoop source code with patch that uses new HBase API. See: [HBase-3232](https://github.com/apache/sqoop/commit/e13dd21209c26316d43350a23f5d533321b61352) for more details.
