# Apache Atlas

This initialization action installs [Apache Atlas](https://atlas.apache.org) on
a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. Apache
Atlas is a Data Governance and Metadata framework for Hadoop. More information
can be found on the [project's website](https://atlas.apache.org).

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

This initialization action supports Dataproc image version 1.5 and higher.

This initialization action supports single node, standard and HA clusters, but
it has different requirements depending on the configuration.

### Single node and standard clusters

Atlas requires the following components to be available:

*   Zookeeper
*   HBase
*   Solr

The initialization action will fail if any of those services is not available.

Use the following command to create a standard cluster with Atlas:

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud beta dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION}
    --optional-components ZOOKEEPER,HBASE,SOLR \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/atlas/atlas.sh \
    --initialization-action-timeout 30m
```

### High availability clusters

On HA clusters Atlas still requires HBase and Solr components, but Zookeeper
component installed by default.

Also, because of the nature of High availability setup, Kafka initialization
action has to be used in order to allow Atlas masters to communicate with each
other.

Initialization action will install Atlas on each master node. One of them will
become `ACTIVE`, others will be `PASSIVE`. Election is automatic and once
`ACTIVE` node goes down, one of the `PASSIVE` nodes becomes `ACTIVE`.

Only `ACTIVE` node is handling requests, all `PASSIVE` nodes are redirecting
requests to `ACTIVE` node.

Use the following command to create HA cluster with Atlas:

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud beta dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION}
    --num-masters 3 \
    --metadata "run-on-master=true" \
    --optional-component HBASE,SOLR \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh,gs://goog-dataproc-initialization-actions-${REGION}/atlas/atlas.sh \
    --initialization-action-timeout 30m
```

## Overriding default username and password

By default, Atlas configured with `admin:admin` credentials. If you want to have
custom username and password, please use `ATLAS_ADMIN_USERNAME` and
`ATLAS_ADMIN_PASSWORD_SHA256` metadata parameters.

To create `ATLAS_ADMIN_PASSWORD_SHA256` following command can be used:

```bash
echo -n "<PASSWORD>" | sha256sum`
```

## Atlas web UI and REST API

Atlas serves web UI and REST API on port `21000`. Follow the instructions at
[connect to cluster web interfaces](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces)
to create a `SOCKS5` proxy to access web UI and REST API on
`http://{CLUSTER_NAME}-m:21000`.

On HA clusters, Atlas web UI and REST API works on all master nodes and
`PASSIVE` nodes are redirecting requests to `ACTIVE` node.
