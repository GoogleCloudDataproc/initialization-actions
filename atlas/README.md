# Apache Atlas

This initialization action installs [Apache Atlas](https://atlas.apache.org)
on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
Apache Atlas is a Data Governance and Metadata framework for Hadoop. More information can be found on the
[project's website](https://atlas.apache.org)

## Using this initialization action

This init-action can only be run on Dataproc version higher than 1.5. This init-action can currently run on all custer configurations, but it has different requirements depending on the configuration.

### Single-node and Standard

Atlas requires following components to be available:
* Zookeeper
* HBase
* Solr

The init action will fail if any of those services is not available.

Use the following command to create standard Dataproc cluster with Atlas:
```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --optional-component ZOOKEEPER,HBASE,SOLR \
    --initialization-actions "gs://dataproc-initialization-actions/atlas/atlas.sh" \
    --initialization-action-timeout 30m 
```

### High availability

In HA, HBase and Solr init-actions are still required. Zookeeper is installed by default thus its init-action is not needed. 
However, due to nature of HA setup, Kafka init-action has to be used in order to allow Atlas masters to communicate 
with each other.

Init-action will install Atlas on each Master node. One of them will became _ACTIVE_, others will be _PASSIVE_. 
Election is automatic and once _ACTIVE_ node goes down, one of the _PASSIVE_ nodes becomes _ACTIVE_. 

_ACTIVE_ node is the only one that handles requests, _PASSIVE_ nodes are redirecting requests to _ACTIVE_ node.

Use the following command to create HA cluster with Atlas:
```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --optional-component ZOOKEEPER,HBASE,SOLR \
    --initialization-actions "gs://dataproc-initialization-actions/kafka/kafka.sh","gs://dataproc-initialization-actions/atlas/atlas.sh" \
    --num-masters 3, \
    --initialization-action-timeout 30m 
```

## Overriding default username and password

By default, Atlas comes with `admin:admin` credentials. If you want to have custom username and password,
plese use `ATLAS_ADMIN_USERNAME` and `ATLAS_ADMIN_PASSWORD_SHA256` metadata parameters.

To create `ATLAS_ADMIN_PASSWORD_SHA256` following command can be used:
 
 `echo -n "Password" | sha256sum`
 

## Atlas web UI and REST API

Atlas serves web UI and REST API on port `21000`. Follow the instructions at [connect to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces) 
to create a SOCKS5 proxy to view `http://clustername-m:21000`.
 
On HA clusters, Atlas works on all master nodes and _PASSIVE_ nodes are redirecting to _ACTIVE_ node.
 