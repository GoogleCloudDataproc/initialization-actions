# Dataproc Initialization Actions

When creating a [Google Cloud Dataproc](https://cloud.google.com/dataproc/) cluster, you can specify [initialization actions](https://cloud.google.com/dataproc/init-actions) in executables and/or scripts that Cloud Dataproc will run on all nodes in your Cloud Dataproc cluster immediately after the cluster is set up.

## How are initialization actions used?
Initialization actions are stored in a [Google Cloud Storage](https://cloud.google.com/storage) bucket and can be passed as a paramater to the `gcloud` command or the `clusters.create` API when creating a Dataproc cluster. For example, to specify an initialization action when creating a cluster with the `gcloud` command, you can run:

    gcloud dataproc clusters create CLUSTER-NAME
    [--initialization-actions [GCS_URI,...]]
    [--initialization-action-timeout TIMEOUT]

For convenience, a copy of initialization actions in this repository are stored in the following Cloud Storage bucket which is publicly-accessible:

    gs://dataproc-initialization-actions

The folder structure of this Cloud Storage bucket mirrors this repository. You should be able to use this Cloud Storage bucket (and the initialization scripts within it) for your clusters.

## Why these samples are provided
These samples are provided to show how various packages and components can be installed on Cloud Dataproc clusters. You should understand how these samples work before running them on your clusters. The initialization actions provided in this repository are provided **without support** and you **use them at your own risk**.

## Actions provided
This repository presently offers the following actions for use with Cloud Dataproc clusters.

* Install packages/software on the cluster
  * [Apache Drill](http://drill.apache.org)
  * [Apache Flink](http://flink.apache.org)
  * [Apache Kafka](http://kafka.apache.org)
  * [Apache Oozie](http://oozie.apache.org)
  * [Apache Tez](http://tez.apache.org)
  * [Apache Zeppelin](http://zeppelin.apache.org)
  * [Apache ZooKeeper](http://zookeeper.apache.org)
  * [Google Cloud Datalab](https://cloud.google.com/datalab/)
  * [Hive HCatalog](https://cwiki.apache.org/confluence/display/Hive/HCatalog)
  * [Hue](http://gethue.com)
  * [IPython](http://ipython.org)
  * [Presto](http://prestodb.io)
  * [Anaconda](https://www.continuum.io/why-anaconda)
* Configure the cluster
  * Configure a *nice* shell environment
  * Share a NFS consistency cache
  * Share a [Google Cloud SQL](https://cloud.google.com/sql/) Hive Metastore
  * Setup [Google Stackdriver](https://cloud.google.com/stackdriver/) monitoring for a cluster

## For more information
For more information, review the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions). You can also pose questions to the [Stack Overflow](http://www.stackoverflow.com) comminity with the tag `google-cloud-dataproc`.
See our other [Google Cloud Platform github
repos](https://github.com/GoogleCloudPlatform) for sample applications and
scaffolding for other frameworks and use cases.


## Contributing changes

* See [CONTRIBUTING.md](CONTRIBUTING.md)

## Licensing

* See [LICENSE](LICENSE)
