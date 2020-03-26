--------------------------------------------------------------------------------

# NOTE: *The HBase initialization action has been deprecated. Please use the HBase Component*

**The
[HBase Component](https://cloud.google.com/dataproc/docs/concepts/components/hbase)
is the best way to use Apache HBase with Cloud Dataproc. To learn more about
Dataproc Components see
[here](https://cloud.google.com/dataproc/docs/concepts/components/overview).**

--------------------------------------------------------------------------------

# Apache HBase

This script installs [Apache HBase](https://hbase.apache.org/) on Dataproc
clusters. Apache HBase is a distributed and scalable Hadoop database.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
Apache HBase installed on every node:

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/hbase/hbase.sh \
        --num-masters 3 --num-workers 2
    ```

1.  You can validate your deployment by ssh into any node and running:

    ```bash
    hbase shell
    ```

1.  Apache HBase UI on the master node can be accessed after connecting with the
    command:

    ```bash
    gcloud compute ssh ${CLUSTER_NAME}-m-0 -- -L 16010:<CLUSTER_NAME>-m-0:16010
    ```

    Then just open a browser and type `localhost:16010` address.

1.  HBase running on Dataproc can be easily scaled up. The following command
    will add three additional workers (RegionServers) to previously created
    cluster named `${CLUSTER_NAME}`.

    ```bash
    gcloud dataproc clusters update ${CLUSTER_NAME} --region ${REGION} --num-workers 5
    ```

## Using different storage for HBase data

On Dataproc clusters HBase uses HDFS as storage backend by default. This mode
can be changed by passing `hbase-root-dir` and `hbase-wal-dir` as cluster
metadata during the cluster creation process.

1.  You can use Google Cloud Storage as data backend by passing the absolute
    path to your storage bucket.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/hbase/hbase.sh \
        --metadata 'hbase-root-dir=gs://<bucket_name>/' \
        --metadata 'hbase-wal-dir=hdfs://[host:port]/<path_to_wal>/' \
        --num-masters 3 --num-workers 2
    ```

## Using of Kerberos authentication and rpc encryption for HBase

On Dataproc clusters HBase uses no Kerberos authentication by default. This mode
can be changed by passing `enable-kerberos` and `keytab-bucket` as cluster
metadata during cluster creation process. The script automatically changes the
necessary configurations and creates all keytabs necessary for HBase.

1.  The metadata field `enable-kerberos` should be set to `true`. The metadata
    field `keytab-bucket` should be set to an storage bucket that will be used
    during cluster creation for saving the keytab files of the hbase master and
    region servers. You have to remove the keytab folder before you initiate a
    new cluster provisioning with the same cluster name.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/hbase/hbase.sh \
        --metadata 'enable-kerberos=true,keytab-bucket=gs://<BUCKET_NAME>' \
        --num-masters 3 --num-workers 2 \
        --kerberos-root-principal-password-uri "Cloud Storage URI of KMS-encrypted password for Kerberos root principal" \
        --kerberos-kms-key "The URI of the KMS key used to decrypt the root password" \
        --image-version 1.3
    ```

1.  Login to master `${CLUSTER_NAME}-m-0` and add a principal to Kerberos key
    distribution center to authenticate for HBase.

    ```bash
    sudo kadmin.local
    add_principal <USER_NAME>
    exit
    ```

1.  Get a Kerberos ticket for your user to be able to login into HBase shell

    ```bash
    kinit
    hbase shell
    ```

## Important notes

-   This initialization works with all cluster configuration on Dataproc version
    1.3 and 1.2, but it is intended to be used in the HA mode.
-   In HA clusters, HBase is using Zookeeper that is pre-installed on master
    nodes.
-   In standard and single node clusters, it is required to install and
    configure Zookeeper which could be done with zookeeper init action. You can
    pass additional init action when creating HBase standard cluster:

    ```bash
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/zookeeper/zookeeper.sh,gs://goog-dataproc-initialization-actions-${REGION}/hbase/hbase.sh
    ```

-   The Kerberos version of this initialization action should be used in the HA
    mode. Otherwise, an additional zookeeper configuration is necessary.
