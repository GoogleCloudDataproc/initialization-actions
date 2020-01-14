# Cloud SQL I/O and Hive Metastore

This initialization action installs a
[Google Cloud SQL proxy](https://cloud.google.com/sql/docs/sql-proxy) on every
node in a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. It
also configures the cluster to store [Apache Hive](https://hive.apache.org)
metadata on a given Cloud SQL instance.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

Prerequisite: If this is your first time using Cloud SQL, enable the
[Cloud SQL Admin API](https://cloud.google.com/sql/docs/mysql/admin-api/#enabling_the_api)
before continuing.

Choosing a region: You can find a list of regions where Cloud SQL instances are
available [here](https://cloud.google.com/sql/docs/mysql/locations) and a list
of regions where Dataproc clusters are available
[here](https://cloud.google.com/compute/docs/regions-zones/#available). It is
advisable to co-locate your Cloud SQL instances and Dataproc clusters in the
same region.

You can use this initialization action to create a Dataproc cluster using a
shared hive metastore.

1.  Use the `gcloud` command to create a new 2nd generation Cloud SQL instance
    (or use a previously created instance).

    ```bash
    gcloud sql instances create <INSTANCE_NAME> \
        --tier db-n1-standard-1 \
        --activation-policy=ALWAYS \
        --region <REGION>
    ```

    a. Optionally create (or already have) other 2nd generation instances, which
    you wish to be accessible.

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    HIVE_DATA_BUCKET=<data_bucket>
    PROJECT_ID=<project_id>
    REGION=<region>
    INSTANCE_NAME=<cloud_sql_instance_name>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --scopes sql-admin \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh \
        --properties hive:hive.metastore.warehouse.dir=gs://${HIVE_DATA_BUCKET}/hive-warehouse \
        --metadata "hive-metastore-instance=${PROJECT_ID}:${REGION}:${INSTANCE_NAME}"
    ```

    a. Optionally add other instances, paired with distict TCP ports for further
    I/O.

    ```bash
    --metadata "additional-cloud-sql-instances=<PROJECT_ID>:<REGION>:<ANOTHER_INSTANCE_NAME>=tcp<PORT_#>[,...]"
    ```

1.  Submit pyspark_metastore_test.py to the cluster to validate the metatstore
    and SQL proxies.

    ```bash
    gcloud dataproc jobs submit pyspark --cluster <CLUSTER_NAME> pyspark_metastore_test.py
    ```

    a. You can test connections to your other instance(s) using the url
    `"jdbc:mysql//localhost:<PORT_#>?user=root"`

1.  Create another dataproc cluster with the same Cloud SQL metastore.

    ```bash
    PROJECT_ID=<project_id>
    REGION=<region>
    INSTANCE_NAME=<cloud_sql_instance_name>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --scopes sql-admin \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh \
        --metadata "hive-metastore-instance=${PROJECT_ID}:${REGION}:${INSTANCE_NAME}"
    ```

1.  The two clusters should now be sharing Hive Tables and Spark SQL Dataframes
    saved as tables.

## Important notes for Hive metastore

*   Hive stores the metadata of all tables in its metastore. It stores the
    contents of (non-external) tables in a HCFS directory, which is by default
    on HDFS. If you want to persist your tables beyond the life of the cluster,
    set `hive.metastore.warehouse.dir` to a shared locations (such as the Cloud
    Storage directory in the example above). This directory gets baked into the
    Cloud SQL metastore, so it is recommended to set even if you intend to
    mostly use external tables. If you place this directory in Cloud Storage.
    You may want to use the shared NFS consistency cache as well.
*   The initialization action can be configured to install the proxy only on the
    master, in the case where you wish to have a shared metastore. For that, set
    the `enable-cloud-sql-proxy-on-workers` metadata key to `false`.
*   The initialization action creates a `hive` user and `hive_metastore`
    database in the SQL instance if they don't already exist. It does this by
    logging in as root with an empty password. You can reconfigure all of this
    in the script.

## Using this initialization action without configuring Hive metastore

The initialization action can also be configured to install proxies without
changing the Hive metastore. This is useful for jobs that directly read from or
write to Cloud SQL. Set the `enable-cloud-sql-hive-metastore` metadata key to
`false` and do not set the `hive-metastore-instance` metadata key. Instead, use
`additional-cloud-sql-instances` to install one or more proxies. For example:

```bash
PROJECT_ID=<project_id>
REGION=<region>
INSTANCE_NAME=<cloud_sql_instance_name>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --scopes sql-admin \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh \
    --metadata "enable-cloud-sql-hive-metastore=false" \
    --metadata "additional-cloud-sql-instances=${PROJECT_ID}:${REGION}:${INSTANCE_NAME}"
```

## Private IP Clusters and Cloud SQL Instances

Connecting to a cloud sql instance from a private IP cluster requires some
additional setup.

1.  Create a network.

    ```bash
    gcloud compute networks create <NETWORK>
    ```

    Note that regional subnets will be created automatically. To list these
    subnets:

    ```bash
    gcloud compute networks subnets list --network <NETWORK>
    ```

2.  Enable private IP for a subnet.

    Choose the subnet that is in the region you want to use. Enable private ip
    google access for your subnet:

    ```bash
    gcloud compute networks subnets update <SUBNET> \
        --region <REGION> \
        --enable-private-ip-google-access
    ```

3.  Create a cloud sql instance.

    ```bash
    gcloud sql instances create <INSTANCE_NAME> \
        --database-version="MYSQL_5_7" \
        --activation-policy=ALWAYS \
        --zone <ZONE>
    ```

    Make sure that the zone is in the region you chose earlier.

4.  Enable the service networking API `gcloud services enable
    servicenetworking.googleapis.com`

5.  Configure the cloud sql instance to use private IP.

    Configure your cloud sql instance to use private IP by following
    [these instructions](https://cloud.google.com/sql/docs/mysql/configure-private-ip#configuring_an_existing_instance_to_use_private_ip).

    **Important notes:**

    When selecting a network, make sure to select the same one that you created
    earlier.

    Make sure to click Save after making changes to your cloud sql instance and
    note that the update may take a while.

    You can confirm that the changes were made by describing your cloud sql
    instance: `gcloud sql instances describe <INSTANCE_NAME>`

    If both a public and a private IP appear under `ipAddresses`, then you have
    successfully completed this step.

6.  Create a cluster using this initialization action

    To create a private IP cluster that points to your cloud sql instance, do
    the following:

    ```bash
    HIVE_DATA_BUCKET=<data_bucket>
    PROJECT_ID=<project_id>
    REGION=<region>
    INSTANCE_NAME=<cloud_sql_instance_name>
    CLUSTER_NAME=<cluster_name>
    SUBNET=<subnetwork_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --scopes sql-admin \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh \
        --properties hive:hive.metastore.warehouse.dir=gs://${HIVE_DATA_BUCKET}/hive-warehouse \
        --metadata "hive-metastore-instance=${PROJECT_ID}:${REGION}:${INSTANCE_NAME}" \
        --metadata "use-cloud-sql-private-ip=true" \
        --subnet ${SUBNET} \
        --no-address
    ```

    Note that the value for `hive-metastore-instance` can be found by running:

    ```bash
    gcloud sql instances describe <INSTANCE_NAME> | grep connectionName
    ```

    **Important notes:**

    Make sure to pass the flag `--metadata use-cloud-sql-private-ip=true`. This
    tells the Cloud SQL proxy to use the private IP address of the Cloud SQL
    instance, not the public one.

    Make sure to create the cluster in the same region and subnet as you used
    earlier.

    Make sure to pass the `--no-address` flag, as this is what makes this an
    internal IP cluster.

## Protecting passwords with KMS

If you want to protect the passwords for the `root` and `hive` MySQL users, you
may use [Cloud KMS](https://cloud.google.com/kms/), Google Cloud's key
management service. You will only need to encrypt and provide a root password if
the `hive` user does not already exist in MySQL. Proceed as follows:

1.  Create a bucket to store the encrypted passwords:

    ```bash
    gsutil mb gs://<SECRETS_BUCKET>
    ```

2.  Create a key ring:

    ```bash
    gcloud kms keyrings create my-key-ring --location global
    ```

3.  Create an encryption key:

    ```bash
    gcloud kms keys create my-key \
        --location global \
        --keyring my-key-ring \
        --purpose encryption
    ```

4.  Encrypt the `root` user's password (only necessary if you want the init
    action to create the hive user):

    ```bash
    echo "<ROOT_PASSWORD>" | \
    gcloud kms encrypt \
        --location=global  \
        --keyring=my-key-ring \
        --key=my-key \
        --plaintext-file=- \
        --ciphertext-file=admin-password.encrypted
    ```

5.  Encrypt the `hive` user's password:

    ```bash
    echo "<HIVE_PASSWORD>" | \
    gcloud kms encrypt \
        --location=global  \
        --keyring=my-key-ring \
        --key=my-key \
        --plaintext-file=- \
        --ciphertext-file=hive-password.encrypted
    ```

6.  Upload the encrypted passwords to your secrets GCS bucket:

    ```bash
    gsutil cp admin-password.encrypted hive-password.encrypted gs://<SECRETS_BUCKET>
    ```

7.  Create the Dataproc cluster:

    If you want the init action to create the `hive` MySQL user, use the
    following command to specify both the `root` and `hive` passwords:

    ```bash
    HIVE_DATA_BUCKET=<data_bucket>
    SECRETS_BUCKET=<secrets_bucket>
    PROJECT_ID=<project_id>
    REGION=<region>
    INSTANCE_NAME=<cloud_sql_instance_name>
    CLUSTER_NAME=<cluster_name>
    SUBNET=<subnetwork_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --scopes cloud-platform \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy.sh \
        --properties hive:hive.metastore.warehouse.dir=gs://${HIVE_DATA_BUCKET}/hive-warehouse \
        --metadata "hive-metastore-instance=${PROJECT_ID}:${REGION}:${INSTANCE_NAME}" \
        --metadata "kms-key-uri=projects/${PROJECT_ID}/locations/global/keyRings/my-key-ring/cryptoKeys/my-key" \
        --metadata "db-admin-password-uri=gs://${SECRETS_BUCKET}/admin-password.encrypted" \
        --metadata "db-hive-password-uri=gs://${SECRETS_BUCKET}/hive-password.encrypted"
    ```

    If you have already created a `hive` user in MySQL, use the following
    command, which does not require the `root` password:

    ```bash
    SECRETS_BUCKET=<secrets_bucket>
    PROJECT_ID=<project_id>
    REGION=<region>
    INSTANCE_NAME=<cloud_sql_instance_name>
    CLUSTER_NAME=<cluster_name>
    SUBNET=<subnetwork_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --scopes cloud-platform \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy.sh \
        --metadata "hive-metastore-instance=${PROJECT_ID}:${REGION}:${INSTANCE_NAME}" \
        --metadata "kms-key-uri=projects/${PROJECT_ID}/locations/global/keyRings/my-key-ring/cryptoKeys/my-key" \
        --metadata "db-hive-password-uri=gs://${SECRETS_BUCKET}/hive-password.encrypted"
    ```

8.  Upgrading schema (create cluster step failed on new Dataproc version):

    When changing Dataproc versions, metastore may detect schema as obsolete.
    The initialization action log will include `*** schemaTool failed ***` and
    `Run /usr/lib/hive/bin/schematool -dbType mysql -upgradeSchemaFrom
    <schema-version> to upgrade the schema. Note that this may break Hive
    metastores that depend on the old schema'` messages.

    In this case a schema upgrade is necessary. Log into master node on a
    cluster with new Dataproc version, and run these commands:

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

*   If the cluster's service account has permission to decrypt the `root` or
    `hive` password, then any user that can SSH or run jobs on this cluster can
    decrypt the passwords as well. If you do not want these users to have access
    as `root` to MySQL, create the `hive` user prior to creating the cluster,
    and do not specify a `root` password.
*   The `hive` user password is stored in the Hive configuration file
    `/etc/hive/conf/hive-site.xml`. Therefore any user that can SSH on this
    cluster and has `sudo` access, and any user that can run jobs on this
    cluster, will be able to view the password in that file.
