--------------------------------------------------------------------------------

# NOTE: *The Ranger initialization action has been deprecated. Please use the Ranger Component*

**The
[Ranger Component](https://cloud.google.com/dataproc/docs/concepts/components/ranger)
is the best way to use Apache Ranger with Cloud Dataproc. To learn more about
Dataproc Components see
[here](https://cloud.google.com/dataproc/docs/concepts/components/overview).**

--------------------------------------------------------------------------------

# Apache Ranger

This initialization action installs [Apache Ranger](https://ranger.apache.org/) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
 Apache Ranger enables monitoring and managing data security across the Hadoop ecosystem and uses [Apache Solr](http://lucene.apache.org/solr/) for audits.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with Apache Ranger installed:

1. Use the `gcloud` command to create a new cluster with this initialization action.
The following command will create a new cluster with the Ranger Policy Manager accessible via user `admin` and `<YOUR_PASSWORD>`.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions \
            gs://goog-dataproc-initialization-actions-${REGION}/solr/solr.sh,gs://goog-dataproc-initialization-actions-${REGION}/ranger/ranger.sh \
        --metadata "default-admin-password=<YOUR_PASSWORD>"
    ```
1. Once the cluster has been created Apache Ranger Policy Manager should be running on master node and use Solr in standalone mode for audits.
1. The Policy Manager Web UI is served by default on port 6080. You can login using username `admin` and password provided in metadata.
Follow the instructions on [connect to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces)
to create a SOCKS5 proxy to view `http://clustername-m:6080` in your browser.

## Important notes
* In HA mode Ranger uses Solr in SolrCloud mode which is recommended setup for auditing and efficient querying audit logs.
* The default admin password can be configured by mandatory `default-admin-password` metadata flag. Ranger **requires password that is minimum 8 characters long with min one alphabet and one numeric character**. You can also change it after the first log in.
* You can override default 6080 port by setting metadata flag `ranger-port`.
* Apache Ranger Policy Manager and usersync plugin are installed on master nodes only(m-0 in HA mode).
* This script will install hdfs, hive and yarn plugin by default.
* Ranger is only supported on Dataproc version 1.3 and above.
