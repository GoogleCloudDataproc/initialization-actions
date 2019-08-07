# Sqoop

This initialization action installs [Sqoop](http://sqoop.apache.org/) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. 
Sqoop was compiled using `release-1.4.6-rc3` from the official [github](https://github.com/apache/sqoop) mirror with four additional cherry-picked commits (more details in "Important Notes" paragraph).

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with Sqoop installed:

1. Using the `gcloud` command to create a new cluster with this initialization action. 
The following command will create a new standard cluster named `<CLUSTER_NAME>`.
   
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --initialization-actions gs://dataproc-initialization-actions/sqoop/sqoop.sh
    ```

## Using Sqoop with Cloud SQL

1. Sqoop can be used with different structured datastores. Here is an example of using Sqoop with a Cloud SQL database. 
Use the following extra init actions to setup cloud-sql-proxy to Cloud SQL. 
Please see: [cloud-sql-proxy](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/cloud-sql-proxy) for more details.
    
    ```bash 
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions \
    gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh,\
    gs://dataproc-initialization-actions/sqoop/sqoop.sh \
    --scopes sql-admin \
    --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>" 
    ```

1. Then it's possible to import data from Cloud SQL to Hadoop HDFS using the following command:

    ```bash
    sqoop import --connect jdbc:mysql://localhost/<DB_NAME> --username root --table <TABLE_NAME> --m 1
    ```

## Using Sqoop with Cloud BigTable

1. Sqoop can be used to import data into BigTable. Communication with BigTable is done via BigTable HBase connector 
which is installed as a part of BigTable init action. You can find more details about connecting BigTable and Dataproc
clusters [here](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/blob/master/bigtable/README.MD).
The following command will create cluster with cloud-sql-proxy and BigTable connector installed. 

    ```bash 
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions \
    gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh,\
    gs://dataproc-initialization-actions/bigtable/bigtable.sh,\
    gs://dataproc-initialization-actions/sqoop/sqoop.sh \
    --scopes cloud-platform \
    --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>" \
    --metadata "bigtable-project=my-dataproc-project" --metadata "bigtable-instance=my-big-table"
    ```

1. On that cluster it is possible to run import job from Cloud SQL to BigTable using HBase client and Sqoop.
Running import job to Bigtable requires specifying additional import parameters. 
Please find an example import command below. 
Parameters explanation and more details can be found [here](https://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html#_importing_data_into_hbase). 

    ```bash
    sqoop import --connect jdbc:mysql://localhost/<DB_NAME> --username root --table <TABLE_NAME> --columns "<COLUMN_LIST>" --hbase-table <HBASE_TABLE_NAME> --column-family <COLUMN_FAMILY_NAME> -hbase-row-key <ROW_ID> --hbase-create-table --m 1
    ```
    
## Using Sqoop with HBase
1. Importing to HBase looks the same as for Cloud BigTable. Sqoop will use the same HBase libraries which come with HBase installation.
The following command will create cluster with cloud-sql-proxy and HBase installed. 

    ```bash 
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions \
    gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh,\
    gs://dataproc-initialization-actions/zookeeper/zookeeper.sh,\
    gs://dataproc-initialization-actions/hbase/hbase.sh,\
    gs://dataproc-initialization-actions/sqoop/sqoop.sh \
    --scopes sql-admin \
    --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>"
    ```
    
1. You can run import job using the same command and parameters as for BigTable. Please find the example in the previous paragraph.

## Important notes
* Some databases require installing Sqoop connectors and providing additional arguments in order to run Sqoop jobs. 
See [Sqoop User Guide](http://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html#_compatibility_notes) for more details. 
Init actions which cooperate with Sqoop:
    - [cloud-sql-proxy](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/cloud-sql-proxy)
    - [BigTable](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/bigtable) 
    - [HBase](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/hbase)
* This init action uses Sqoop source code with patches which use new HBase API. 
See: 
[SQOOP-3232](https://github.com/apache/sqoop/commit/e13dd21209c26316d43350a23f5d533321b61352),
[SQOOP-3222](https://github.com/apache/sqoop/commit/18445290810b1df035e06fb074064d6b9c1d6e90),
[SQOOP-3149](https://github.com/apache/sqoop/commit/4ab7b60caf2c3d9fda72c85e6427912584986970) and
[SQOOP-2952](https://github.com/apache/sqoop/commit/b4afcf4179b13c25b5e9bd182d75cab5d2e6c8d1) for more details.
* Please note different scopes required to run certain import jobs. Importing to and from Cloud SQL requires adding `sql-admin` scope. 
Using BigTable requires additional permission, so `cloud-platform` scope is added. Finally, importing between Cloud SQL and HBase again 
`sql-admin` scope is sufficient because HBase uses locally available Hadoop HDFS as storage backend which has no additional scope's requirements.
You can learn more about scopes [here](https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create#--scopes).