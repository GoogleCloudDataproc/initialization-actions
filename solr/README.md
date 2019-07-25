# Apache Solr

This initialization action installs [Apache Solr](https://lucene.apache.org/solr/) on [Google Cloud Dataproc](https://cloud.google.com/dataproc) clusters. Solr is an enterprise search platform with REST-like API that allows to put and query documents via HTTP. You can learn more about Solr features [here](https://lucene.apache.org/solr/features.html).

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with Apache Solr installed:

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://$MY_BUCKET/solr/solr.sh
    ```

## Solr UI

The Solr Admin UI is served by the Jetty web server (port 8983) at `/solr`. Follow the instructions at [connect to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces) to create a SOCKS5 proxy to view `http://node-name:8983/solr/` in your web browser.

## Important notes

* This script installs Solr as a service only on master nodes in the cluster.
* In HA clusters Solr starts in SolrCloud mode.