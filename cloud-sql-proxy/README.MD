# Cloud SQL I/O and Hive Metastore

This initialization action installs a [Google Cloud SQL proxy](https://cloud.google.com/sql/docs/sql-proxy) on every node in a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. It also configures the cluster to store [Apache Hive](https://hive.apache.org) metadata on a given Cloud SQL instance.

## Using this initialization action

You can use this initialization action to create a Dataproc cluster using a shared hive metastore.:

1. Using the `gcloud` command to create a new 2nd generation Cloud SQL intance (or use a previously created instance). You must enable the [Cloud SQL API](https://console.cloud.google.com/apis/library/sqladmin.googleapis.com/?q=sql) for your project.

    ```bash
    gcloud sql instances create <INSTANCE_NAME> \
        --tier db-n1-standard-1 \
        --activation-policy=ALWAYS
    ```
    a. Optionally create (or already have) other 2nd generation instances, which you wish to be accessible.

1. Using the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --scopes sql-admin \
    --initialization-actions gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh \
    --properties hive:hive.metastore.warehouse.dir=gs://<GCS_BUCKET>/hive-warehouse \
    --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>"
    ```
    a. Optionally add other instances, paired with distict TCP ports for further I/O.

    ```bash
    --metadata "additional-cloud-sql-instances=<PROJECT_ID>:<REGION>:<ANOTHER_INSTANCE_NAME>=tcp<PORT_#>[,...]"
    ```

1. Submit pyspark_metastore_test.py to the cluster to validate the metatstore and SQL proxies.
    ```bash
    gcloud dataproc jobs submit pyspark --cluster <CLUSTER_NAME> pyspark_metastore_test.py
    ```
    a. You can test connections to your other instance(s) using the url `"jdbc:mysql//localhost:<PORT_#>?user=root"`

1. Create another dataproc cluster with the same Cloud SQL metastore.
    ```bash
    gcloud dataproc clusters create <ANOTHER_CLUSTER_NAME> \
    --scopes sql-admin \
    --initialization-actions gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh \
    --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>"
    ```

1. The two clusters should now be sharing Hive Tables and Spark SQL Dataframes saved as tables.

## Important notes for Hive metastore
* Hive stores the metadata of all tables in it's metastore. It stores the contents of (non-external) tables in a HCFS directory, which is by default on HDFS. If you want to perist your tables beyond the life of the cluster, set "hive.metastore.warehouse.dir" to a shared locations (such as the Cloud Storage directory in the example above). This directory get's baked into the Cloud SQL metastore, so it is recommended to set even if you intend to mostly use external tables. If you place this direcotry in Cloud Storage. You may want to use the shared NFS consistency cache as well.
* The initialization action can be configured to install the proxy only on the master, in the case where you wish to have a shared metastore. For that, set the `enable-cloud-sql-proxy-on-workers` metadata key to `false`.
* The initialization action creates a `hive` user and `hive_metastore` database in the SQL instance if they don't already exist. It does this by logging in as root with an empty password. You can reconfigure all of this in the script.

## Using this initialization action without configuring Hive metastore

The initialization action can also be configured to install proxies without changing the Hive metastore. This is useful for jobs that directly read from or write to Cloud SQL. Set the `enable-cloud-sql-hive-metastore` metadata key to `false` and do not set the `hive-metastore-instance` metadata key. Instead, use `additional-cloud-sql-instances` to install one or more proxies. For example:

```bash
gcloud dataproc clusters create <CLUSTER_NAME> \
    --scopes sql-admin \
    --initialization-actions gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh \
    --metadata "enable-cloud-sql-hive-metastore=false"
    --metadata "additional-cloud-sql-instances=<PROJECT_ID>:<REGION>:<ANOTHER_INSTANCE_NAME>
```
