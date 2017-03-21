# Cloud SQL I/O and Hive Metastore

This initialization action installs a [Google Cloud SQL proxy](https://cloud.google.com/sql/docs/sql-proxy) on every node in a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. It also configures the cluster to store [Apache Hive](https://hive.apache.org) metadata on a given Cloud SQL instance.

## Using this initialization action
You can use this initialization action to create a Dataproc cluster using a shared hive metastore.:

1. Using the `gcloud` command to create a new 2nd generation Cloud SQL intance (or use a previously created instance)

    ```bash
    gcloud sql instances create <INSTANCE_NAME> \
        --tier db-n1-standard-1 \
        --activation-policy=ALWAYS
    ```
    a. Optionally create (or already have) other 2nd generation instances, which you wish to be accessible.

2. Uploading a copy of this initialization action (`cloud-sql-proxy.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).

3. Using the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --scopes sql-admin \
    --initialization-actions gs://<GCS_BUCKET>/cloud-sql-proxy.sh \
    --properties hive:hive.metastore.warehouse.dir=gs://<GCS_BUCKET>/hive-warehouse \
    --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>"
    ```
    a. Optionally add other instances, paired with distict TCP ports for further I/O.

    ```bash
    --metadata "additional-cloud-sql-instances=<PROJECT_ID>:<REGION>:<ANOTHER_INSTANCE_NAME>=tcp<PORT_#>[,...]"
    ```

4. Submit pyspark_metastore_test.py to the cluster to validate the metatstore and SQL proxies.
    ```bash
    gcloud dataproc jobs submit pyspark --cluster <CLUSTER_NAME> pyspark_metastore_test.py
    ```
    a. You can test connections to your other instance(s) using the url `"jdbc:mysql//localhost:<PORT_#>?user=root"`

5. Create another dataproc cluster with the same Cloud SQL metastore.
    ```bash
    gcloud dataproc clusters create <ANOTHER_CLUSTER_NAME> \
    --scopes sql-admin \
    --initialization-actions gs://<GCS_BUCKET>/cloud-sql-proxy.sh \
    --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>"
    ```

6. The two clusters should now be sharing Hive Tables and Spark SQL Dataframes saved as tables.

## Important notes
* Hive stores the metadata of all tables in it's metastore. It stores the contents of (non-external) tables in a HCFS directory, which is by default on HDFS. If you want to perist your tables beyond the life of the cluster, set "hive.metastore.warehouse.dir" to a shared locations (such as the Cloud Storage directory in the example above). This directory get's baked into the Cloud SQL metastore, so it is recommended to set even if you intend to mostly use external tables. If you place this direcotry in Cloud Storage. You may want to use the shared NFS consistency cache as well.
* This initalization action can be reconfigured to only install a proxy on the master, if you only wish to have a shared metastore. Set `ENABLE_PROXY_ON_WORKERS` to 0.
* It can be also be reconfigured to istall proxies without changing the Hive metasore, if you only wish to have the proxies. Set `ENABLE_CLOUD_SQL_METASTORE` to 0 and do not set the `hive-metastore-instance` metadata key.
* The initialization action creates a `hive` user and `hive_metastore` database in the SQL instance if they don't already exist. It does this by logging in as root with an empty password. You can reconfigure all of this in the script.
