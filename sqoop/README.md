# Sqoop

This initialization action installs [Sqoop](http://sqoop.apache.org/) on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
Sqoop installed:

1.  Using the `gcloud` command to create a new cluster with this initialization
    action. The following command will create a new standard cluster named
    `${CLUSTER_NAME}`.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/sqoop/sqoop.sh
    ```

## Using Sqoop with Cloud SQL

1.  Sqoop can be used with different structured data stores. Here is an example
    of using Sqoop with a Cloud SQL database. Use the following extra init
    actions to setup cloud-sql-proxy. Please see
    [Cloud SQL Proxy](/cloud-sql-proxy) for more details.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    CLOUD_SQL_PROJECT=<cloud_sql_project_id>
    CLOUD_SQL_INSTANCE=<cloud_sql_instance_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh,gs://goog-dataproc-initialization-actions-${REGION}/sqoop/sqoop.sh \
        --metadata "hive-metastore-instance=${CLOUD_SQL_PROJECT}:${REGION}:${CLOUD_SQL_INSTANCE}" \
        --scopes sql-admin
    ```

1.  Then it's possible to import data from Cloud SQL to Hadoop HDFS using the
    following command:

    ```bash
    sqoop import --connect jdbc:mysql://localhost/<DB_NAME> --username root --table <TABLE_NAME> --m 1
    ```

## Using Sqoop with Cloud Bigtable

1.  Sqoop can be used to import data into Bigtable. Communication with Bigtable
    is done via Bigtable HBase connector which is installed as a part of
    Bigtable initialization action. You can find more details about connecting
    Bigtable and Dataproc clusters [here](/bigtable).

    The following command will create a cluster with cloud-sql-proxy and
    Bigtable connector installed.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    BIGTABLE_PROJECT=<bigtable_project_id>
    BIGTABLE_INSTANCE=<bigtable_instance_name>
    CLOUD_SQL_PROJECT=<cloud_sql_project_id>
    CLOUD_SQL_INSTANCE=<cloud_sql_instance_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/bigtable/bigtable.sh,gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh,gs://goog-dataproc-initialization-actions-${REGION}/sqoop/sqoop.sh \
        --metadata "bigtable-project=${BIGTABLE_PROJECT},bigtable-instance=${BIGTABLE_INSTANCE}" \
        --metadata "hive-metastore-instance=${CLOUD_SQL_PROJECT_ID}:${REGION}:${CLOUD_SQL_INSTANCE}" \
        --scopes cloud-platform
    ```

1.  On the created cluster it is possible to run an import job from Cloud SQL to
    Bigtable using HBase client and Sqoop. Running import job to Bigtable
    requires specifying additional import parameters. Please find an example
    import command below. Parameters explanation and more details can be found
    [here](https://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html#_importing_data_into_hbase).

    ```bash
    sqoop import \
        --connect jdbc:mysql://localhost/<DB_NAME> --username root --table <CLOUD_SQL_TABLE_NAME> --columns <CLOUD_SQL_COLUMN_LIST> \
        --hbase-table <HBASE_TABLE_NAME> --column-family <HBASE_COLUMN_FAMILY_NAME> --hbase-row-key <HBASE_ROW_ID> --hbase-create-table \
        --m 1
    ```

## Using Sqoop with HBase

1.  Importing to HBase looks the same as for Cloud Bigtable. Sqoop will use the
    same HBase libraries which come with HBase installation. The following
    command will create cluster with cloud-sql-proxy and HBase installed.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    CLOUD_SQL_PROJECT=<cloud_sql_project_id>
    CLOUD_SQL_INSTANCE=<cloud_sql_instance_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --optional-components HBASE,ZOOKEEPER \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh,gs://goog-dataproc-initialization-actions-${REGION}/sqoop/sqoop.sh \
        --metadata "hive-metastore-instance=${CLOUD_SQL_PROJECT_ID}:${REGION}:${CLOUD_SQL_INSTANCE}" \
        --scopes sql-admin
    ```

1.  You can run import job using the same command and parameters as for
    Bigtable. Please find the example in the previous paragraph.

## Important notes

*   Some databases require installing Sqoop connectors and providing additional
    arguments in order to run Sqoop jobs. See
    [Sqoop User Guide](http://sqoop.apache.org/docs/1.4.7/SqoopUserGuide.html#_compatibility_notes)
    for more details.
*   Initialization actions which cooperate with Sqoop:
    -   [Bigtable](/bigtable)
    -   [Cloud SQL Proxy](/cloud-sql-proxy)
    -   [HBase](/hbase)
*   Please note different scopes required to run certain import jobs. Importing
    to and from Cloud SQL requires adding `sql-admin` scope. Using Bigtable
    requires additional permission, so `cloud-platform` scope added. Finally,
    importing between Cloud SQL and HBase also requires `sql-admin` scope
    because HBase uses locally available Hadoop HDFS as storage backend which
    has no additional scope's requirements. You can learn more about scopes
    [here](https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create#--scopes).
