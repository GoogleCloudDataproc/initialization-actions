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
    --properties hive:hive.metastore.warehouse.dir=gs://<HIVE_DATA_BUCKET>/hive-warehouse \
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

* Hive stores the metadata of all tables in its metastore. It stores the contents of (non-external) tables in a HCFS directory, which is by default on HDFS. If you want to persist your tables beyond the life of the cluster, set "hive.metastore.warehouse.dir" to a shared locations (such as the Cloud Storage directory in the example above). This directory gets baked into the Cloud SQL metastore, so it is recommended to set even if you intend to mostly use external tables. If you place this directory in Cloud Storage. You may want to use the shared NFS consistency cache as well.
* The initialization action can be configured to install the proxy only on the master, in the case where you wish to have a shared metastore. For that, set the `enable-cloud-sql-proxy-on-workers` metadata key to `false`.
* The initialization action creates a `hive` user and `hive_metastore` database in the SQL instance if they don't already exist. It does this by logging in as root with an empty password. You can reconfigure all of this in the script.

## Using this initialization action without configuring Hive metastore

The initialization action can also be configured to install proxies without changing the Hive metastore. This is useful for jobs that directly read from or write to Cloud SQL. Set the `enable-cloud-sql-hive-metastore` metadata key to `false` and do not set the `hive-metastore-instance` metadata key. Instead, use `additional-cloud-sql-instances` to install one or more proxies. For example:

```bash
gcloud dataproc clusters create <CLUSTER_NAME> \
    --scopes sql-admin \
    --initialization-actions gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh \
    --metadata "enable-cloud-sql-hive-metastore=false" \
    --metadata "additional-cloud-sql-instances=<PROJECT_ID>:<REGION>:<ANOTHER_INSTANCE_NAME>"
```

## Protecting passwords with KMS

If you want to protect the passwords for the `root` and `hive` MySQL users, you may use [Cloud KMS](https://cloud.google.com/kms/),
Google Cloud's key management service. You will only need to encrypt and provide a root password if the `hive` user does not already exist in MySQL. Proceed as follows:

1. Create a bucket to store the encrypted passwords:

    ```bash
    gsutil mb gs://<SECRETS_BUCKET>
    ```

2. Create a key ring:

    ```bash
    gcloud kms keyrings create my-key-ring --location global
    ```

3. Create an encryption key:

    ```bash
    gcloud kms keys create my-key \
        --location global \
        --keyring my-key-ring \
        --purpose encryption
    ```

4. Encrypt the `root` user's password (only necessary if you want the init action to create the hive user):

    ```bash
    echo "<ROOT_PASSWORD>" | \
    gcloud kms encrypt \
        --location=global  \
        --keyring=my-key-ring \
        --key=my-key \
        --plaintext-file=- \
        --ciphertext-file=admin-password.encrypted
    ```

5. Encrypt the `hive` user's password:

    ```bash
    echo "<HIVE_PASSWORD>" | \
    gcloud kms encrypt \
        --location=global  \
        --keyring=my-key-ring \
        --key=my-key \
        --plaintext-file=- \
        --ciphertext-file=hive-password.encrypted
    ```

6. Upload the encrypted passwords to your secrets GCS bucket:

    ```bash
    gsutil cp admin-password.encrypted hive-password.encrypted gs://<SECRETS_BUCKET>
    ```

7. Create the Dataproc cluster:

    If you want the init action to create the `hive` MySQL user, use the following command to specify both the `root` and `hive` passwords:

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --scopes cloud-platform \
    --initialization-actions gs://<INIT_ACTION_BUCKET>/cloud-sql-proxy.sh \
    --properties hive:hive.metastore.warehouse.dir=gs://<HIVE_DATA_BUCKET>/hive-warehouse \
    --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>" \
    --metadata "kms-key-uri=projects/<PROJECT_ID>/locations/global/keyRings/my-key-ring/cryptoKeys/my-key" \
    --metadata "db-admin-password-uri=gs://<SECRETS_BUCKET>/admin-password.encrypted" \
    --metadata "db-hive-password-uri=gs://<SECRETS_BUCKET>/hive-password.encrypted"
    ```

    If you have already created a `hive` user in MySQL, use the following command, which does not require the `root` password:

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
        --scopes cloud-platform \
        --initialization-actions gs://<INIT_ACTION_BUCKET>/cloud-sql-proxy.sh \
        --metadata "hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>" \
        --metadata "kms-key-uri=projects/<PROJECT_ID>/locations/global/keyRings/my-key-ring/cryptoKeys/my-key" \
        --metadata "db-hive-password-uri=gs://<SECRETS_BUCKET>/hive-password.encrypted"
    ```

8. Upgrading schema (create cluster step failed on new Dataproc version):

    When changing Dataproc versions, metastore may detect schema as obsolete. The initialization action log will include `*** schemaTool failed ***` and `Run /usr/lib/hive/bin/schematool -dbType mysql -upgradeSchemaFrom <schema-version> to upgrade the schema. Note that this may break Hive metastores that depend on the old schema'` messges.
    
    In this case a schema upgrade is necessary. Log into master node on a cluster with new Dataproc version, and run these commands:
    
    ```bash
    $ /usr/lib/hive/bin/schematool -dbType mysql -info
    Hive distribution version: 2.3.0
    Metastore schema version: 2.1.0
    org.apache.hadoop.hive.metastore.HiveMetaException: Metastore schema version is not compatible. Hive Version: 2.3.0, Database Schema Version: 2.1.0
    *** schemaTool failed ***

    User can upgrade their schema by running:
    $ /usr/lib/hive/bin/schematool -dbType mysql -upgradeSchemaFrom <current-version>
    ```

    Now schema is updated. Please delete and recreate the cluster

**Notes:**

* If the cluster's service account has permission to decrypt the `root` or `hive` password, then any user that
  can SSH or run jobs on this cluster can decrypt the passwords as well. If you do not want these users to have access as `root` to MySQL, create the `hive` user prior to creating the cluster, and do not specify a `root` password.
* The `hive` user password is stored in the Hive configuration file `/etc/hive/conf/hive-site.xml`.
  Therefore any user that can SSH on this cluster and has `sudo` access, and any user that can run
  jobs on this cluster, will be able to view the password in that file.
