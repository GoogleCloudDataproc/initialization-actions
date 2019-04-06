# Cloud Dataproc Initialization Actions

When creating a [Google Cloud Dataproc](https://cloud.google.com/dataproc/) cluster, you can specify [initialization actions](https://cloud.google.com/dataproc/init-actions) in executables and/or scripts that Cloud Dataproc will run on all nodes in your Cloud Dataproc cluster immediately after the cluster is set up. Initialization actions often set up job dependencies, such as installing Python packages, so that jobs can be submitted to the cluster without having to install dependencies when the jobs are run.

## How initialization actions are used

Initialization actions are stored in a [Google Cloud Storage](https://cloud.google.com/storage) bucket and can be passed as a parameter to the `gcloud` command or the `clusters.create` API when creating a Cloud Dataproc cluster. For example, to specify an initialization action when creating a cluster with the `gcloud` command, you can run:

    gcloud dataproc clusters create <CLUSTER_NAME> \
      [--initialization-actions [GCS_URI,...]] \
      [--initialization-action-timeout TIMEOUT]

For convenience, copies of initialization actions in this repository are stored in the publicly accessible Cloud Storage bucket `gs://dataproc-initialization-actions`. The folder structure of this Cloud Storage bucket mirrors this repository. You should be able to use this Cloud Storage bucket (and the initialization scripts within it) for your clusters.

For example:

```bash
gcloud dataproc clusters create my-presto-cluster \
  --initialization-actions gs://dataproc-initialization-actions/presto/presto.sh
```

You are strongly encouraged to copy initialization actions to your own GCS bucket in automated pipelines to ensure hermetic deployments. For example:

```bash
MY_BUCKET=<gcs-bucket>
gsutil cp presto/presto.sh gs://$MY_BUCKET/
gcloud dataproc clusters create my-presto-cluster \
  --initialization-actions gs://$MY_BUCKET/presto.sh
```

This is also useful if you want to modify initialization actions to fit your needs.

## Why these samples are provided

These samples are provided to show how various packages and components can be installed on Cloud Dataproc clusters. You should understand how these samples work before running them on your clusters. The initialization actions provided in this repository are provided **without support** and you **use them at your own risk**.

## Actions provided

This repository presently offers the following actions for use with Cloud Dataproc clusters.

* Install additional Apache Hadoop ecosystem components
  * [Apache Drill](http://drill.apache.org)
  * [Apache Flink](http://flink.apache.org)
  * [Apache Gobblin](https://gobblin.apache.org/)
  * [Apache Hive HCatalog](https://cwiki.apache.org/confluence/display/Hive/HCatalog)
  * [Apache Kafka](http://kafka.apache.org)
  * [Apache Livy](https://livy.incubator.apache.org/)
  * [Apache Oozie](http://oozie.apache.org)
  * [Apache Tez](http://tez.apache.org)
  * [Apache ZooKeeper](http://zookeeper.apache.org)
  * [Presto](http://prestodb.io)
* Improve data science and interactive experiences
  * [Miniconda](https://conda.io/docs/)
  * [Jupyter](http://jupyter.org/)
  * [Apache Zeppelin](http://zeppelin.apache.org)
  * [Google Cloud Datalab](https://cloud.google.com/datalab/)
  * [RStudio Server](https://www.rstudio.com/products/rstudio/#Server)
  * [Intel BigDL](https://bigdl-project.github.io)
  * [Hue](http://gethue.com)
* Configure the environment
  * Configure a *nice* shell environment
  * To switch to Python 3, use the conda initialization action
* Connect to Google Cloud Platform services
  * Install alternate versions of the [Cloud Storage and BigQuery connectors](https://github.com/GoogleCloudPlatform/bigdata-interop/releases). [Specific versions](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions) of these connectors come pre-installed on Cloud Dataproc clusters.
  * Share a [Google Cloud SQL](https://cloud.google.com/sql/) Hive Metastore, or simply read/write data from Cloud SQL.
* Set up monitoring
  * [Google Stackdriver](https://cloud.google.com/stackdriver/)
  * [Ganglia](http://ganglia.info/)

## Initialization actions on single node clusters

[Single Node clusters](https://cloud.google.com/dataproc/docs/concepts/single-node-clusters) have `dataproc-role` set to `Master` and `dataproc-worker-count` set to `0`. Most of the initialization actions in this repository should work out of the box, as they run only on the master. Examples include notebooks (such as Apache Zeppelin) and libraries (such as Apache Tez). Actions that run on all nodes of the cluster (such as cloud-sql-proxy) similarly work out of the box.

Some initialization actions are known **not to work** on Single Node clusters. All of these expect to have daemons on multiple nodes.

* Apache Drill
* Apache Flink
* Apache Kafka
* Apache Zookeeper

Feel free to send pull requests or file issues if you have a good use case for running one of these actions on a Single Node cluster.

## Using cluster metadata

Cloud Dataproc sets special [metadata values](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/metadata)
for the instances that run in your cluster. You can use these values to customize the behavior of
initialization actions, for example:

```bash
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
  ... master specific actions ...
else
  ... worker specific actions ...
fi
```

You can also use the `‑‑metadata` flag of the `gcloud dataproc clusters create` command to provide your own
custom metadata:

```bash
gcloud dataproc clusters create cluster-name \
  --initialization-actions ... \
  --metadata name1=value1,name2=value2... \
  ... other flags ...
```

## For more information

For more information, review the [Cloud Dataproc documentation](https://cloud.google.com/dataproc/init-actions). You can also pose questions to the [Stack Overflow](http://www.stackoverflow.com) community with the tag `google-cloud-dataproc`.
See our other [Google Cloud Platform github
repos](https://github.com/GoogleCloudPlatform) for sample applications and
scaffolding for other frameworks and use cases.

### Mailing list

Subscribe to [cloud-dataproc-discuss@google.com](https://groups.google.com/forum/#!forum/cloud-dataproc-discuss) for announcements and discussion.

## Contributing changes

* See [CONTRIBUTING.md](CONTRIBUTING.md)

## Licensing

* See [LICENSE](LICENSE)

