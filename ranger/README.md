# Apache Ranger

This initialization action installs [Apache Ranger](https://ranger.apache.org/) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
 Apache Ranger enables monitoring and managing data security across the Hadoop ecosystem and uses [Apache Solr](http://lucene.apache.org/solr/) for audits.

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with Apache Ranger installed:

1. Use the `gcloud` command to create a new cluster with this initialization action. 
The following command will create a new high availability cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --num-masters 3 \
      --initialization-actions gs://dataproc-initialization-actions/solr/solr.sh,\
    gs://dataproc-initialization-actions/ranger/ranger.sh \
      --image-version 1.2
    ```
1. Once the cluster has been created Apache Ranger Policy Manager should be running on **m-0** and use Solr in SolrCloud mode for audits.
1. The Policy Manager Web UI is served on port 6080. 
Follow the instructions on [connect to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces) 
to create a SOCKS5 proxy to view `http://clustername-m-0:6080` in your browser.

## Important notes
* The default admin password is `dataproc2019` and can be changed in init action script. You can also change it in after the first log in.
* Apache Ranger Policy Manager and usersync plugin are installed on master nodes only(m-0 in HA mode).
* This script will install hdfs, hive and yarn plugin by default.
* Ranger and whole plugin stack is supported only by Dataproc 1.2 and 1.1 at this moment.