# Cloud Dataproc Initialization Actions

When creating a [Dataproc](https://cloud.google.com/dataproc/) cluster, you can specify [initialization actions](https://cloud.google.com/dataproc/init-actions) in executables and/or scripts that Dataproc will run on all nodes in your Dataproc cluster immediately after the cluster is set up. Initialization actions often set up job dependencies, such as installing Python packages, so that jobs can be submitted to the cluster without having to install dependencies when the jobs are run.

## How initialization actions are used

Initialization actions must be stored in a [Cloud Storage](https://cloud.google.com/storage) bucket and can be passed as a parameter to the `gcloud` command or the `clusters.create` API when creating a Dataproc cluster. For example, to specify an initialization action when creating a cluster with the `gcloud` command, you can run:

```bash
gcloud dataproc clusters create <CLUSTER_NAME> \
    [--initialization-actions [GCS_URI,...]] \
    [--initialization-action-timeout TIMEOUT]
```

During development, you can create a Dataproc cluster using Dataproc-provided
[regional](https://cloud.google.com/dataproc/docs/concepts/regional-endpoints) initialization
actions buckets (for example `goog-dataproc-initialization-actions-us-east1`):

```bash
REGION=<region>
CLUSTER=<cluster_name>
gcloud dataproc clusters create ${CLUSTER} \
    --region ${REGION} \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/presto/presto.sh
```

**:warning: NOTICE:** For production usage, before creating clusters, it is strongly recommended
that you copy initialization actions to your own Cloud Storage bucket to guarantee consistent use of the
same initialization action code across all Dataproc cluster nodes and to prevent unintended upgrades
from upstream in the cluster:

```bash
BUCKET=<your_init_actions_bucket>
CLUSTER=<cluster_name>
gsutil cp presto/presto.sh gs://${BUCKET}/
gcloud dataproc clusters create ${CLUSTER} --initialization-actions gs://${BUCKET}/presto.sh
```

You can decide when to sync your copy of the initialization action with any changes to the initialization action that occur in the GitHub repository. Doing this is also useful if you want to modify initialization actions to meet your needs.

## Why these samples are provided

These samples are provided to show how various packages and components can be
installed on Dataproc clusters. You should understand how these samples work
before running them on your clusters. The initialization actions provided in
this repository are provided **without support** and you **use them at your own
risk**.

The initialization action scripts should be assumed not to be working with the
current version of Dataproc unless mentioned below.  Even then, the script is
only known to work with the cluster configuration on which the tests were
executed.  Your mileage may vary.

Versions on which initialization actions have been tested and the date and
version of the tests are included in the table below

| test name | test date | image version | test status |
| :---      |   :----:  |    :----:    |        ---: |
| alluxio/test_alluxio.py | N/A       | N/A   | FAIL |
| atlas/test_atlas.py | N/A       | N/A   | FAIL |
| bigtable/test_bigtable.py | N/A       | N/A   | FAIL |
| cloud-sql-proxy/test_cloud_sql_proxy.py | N/A       | N/A   | FAIL |
| conda/test_conda.py | N/A       | N/A   | FAIL |
| connectors/test_connectors.py | N/A       | N/A   | FAIL |
| dask/test_dask.py | N/A       | N/A   | FAIL |
| dr-elephant/test_dr_elephant.py | N/A       | N/A   | FAIL |
| drill/test_drill.py | N/A       | N/A   | FAIL |
| flink/test_flink.py | N/A       | N/A   | FAIL |
| ganglia/test_ganglia.py | N/A       | N/A   | FAIL |
| gpu/test_gpu.py | 2023-10-09 |  2.1.27-{debian11,ubuntu20,rocky8}  | SUCCESS |
| h2o/test_h2o.py | N/A       | N/A   | FAIL |
| hbase/test_hbase.py | N/A       | N/A   | FAIL |
| hive-hcatalog/test_hive_hcatalog.py | N/A       | N/A   | FAIL |
| hive-llap/test_hive_llap.py | N/A       | N/A   | FAIL |
| horovod/test_horovod.py | N/A       | N/A   | FAIL |
| hue/test_hue.py | N/A       | N/A   | FAIL |
| kafka/test_kafka.py | N/A       | N/A   | FAIL |
| knox/test_knox.py | N/A       | N/A   | FAIL |
| livy/test_livy.py | N/A       | N/A   | FAIL |
| mlvm/test_mlvm.py | N/A       | N/A   | FAIL |
| oozie/test_oozie.py | N/A       | N/A   | FAIL |
| otel/test_otel.py | N/A       | N/A   | FAIL |
| presto/test_presto.py | N/A       | N/A   | FAIL |
| ranger/test_ranger.py | N/A       | N/A   | FAIL |
| rapids/test_rapids.py | N/A       | N/A   | FAIL |
| rstudio/test_rstudio.py | N/A       | N/A   | FAIL |
| solr/test_solr.py | N/A       | N/A   | FAIL |
| spark-rapids/test_spark_rapids.py | N/A       | N/A   | FAIL |
| sqoop/test_sqoop.py | N/A       | N/A   | FAIL |
| starburst-presto/test_starburst_presto.py | N/A       | N/A   | FAIL |
| tony/test_tony.py | N/A       | N/A   | FAIL |
| toree/test_toree.py | N/A       | N/A   | FAIL |


## Actions provided

This repository currently offers the following actions for use with Dataproc clusters.

* Install additional Apache Hadoop ecosystem components
  * [Alluxio](https://www.alluxio.io/)
  * [Apache Drill](http://drill.apache.org)
  * [Apache Flink](http://flink.apache.org)
  * [Apache Gobblin](https://gobblin.apache.org/)
  * [Apache Hive HCatalog](https://cwiki.apache.org/confluence/display/Hive/HCatalog)
  * [Apache Kafka](http://kafka.apache.org)
  * [Apache Livy](https://livy.incubator.apache.org/)
  * [Apache Oozie](http://oozie.apache.org)
  * [Apache ZooKeeper](http://zookeeper.apache.org)
  * [Presto](http://prestodb.io)
* Improve data science and interactive experiences
  * [Miniconda](https://conda.io/docs/)
  * [Apache Zeppelin](http://zeppelin.apache.org)
  * [RStudio Server](https://www.rstudio.com/products/rstudio/#Server)
  * [Intel BigDL](https://bigdl-project.github.io)
  * [Hue](http://gethue.com)
* Configure the environment
  * Configure a *nice* shell environment
  * To switch to Python 3, use the conda initialization action
* Connect to Google Cloud Platform services
  * Install alternate versions of the [Cloud Storage and BigQuery connectors](https://github.com/GoogleCloudPlatform/bigdata-interop/releases). [Specific versions](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions) of these connectors come pre-installed on Cloud Dataproc clusters.
  * Share a [Cloud SQL](https://cloud.google.com/sql/) Hive Metastore, or simply read/write data from Cloud SQL.
* Set up monitoring
  * [Stackdriver](https://cloud.google.com/stackdriver/)
  * [Ganglia](http://ganglia.info/)

## Removed actions

Previously, this repo provided init actions for the following, which have
since been removed because equivalent functionality is now provided directly
by Dataproc:

* [Apache Tez](http://tez.apache.org): This is now pre-installed in all
  current Dataproc image versions.
* [Datalab](https://cloud.google.com/datalab/): Datalab has been replaced by
  Vertex AI Workbench, which integrates with Dataproc.
* [Jupyter](http://jupyter.org/): This has been replaced with the
  [Jupyter Optional Component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter).

## Initialization actions on single node clusters

[Single Node clusters](https://cloud.google.com/dataproc/docs/concepts/single-node-clusters) have `dataproc-role` set to `Master` and `dataproc-worker-count` set to `0`. Most of the initialization actions in this repository should work out of the box because they run only on the master. Examples include notebooks, such as Apache Zeppelin, and libraries, such as Apache Tez. Actions that run on all nodes of the cluster, such as cloud-sql-proxy, also work out of the box.

Some initialization actions are known **not to work** on Single Node clusters. All of these expect to have daemons on multiple nodes.

* Apache Drill
* Apache Flink
* Apache Kafka
* Apache Zookeeper

Feel free to send pull requests or file issues if you have a good use case for running one of these actions on a Single Node cluster.

## Using cluster metadata

Dataproc sets special [metadata values](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/metadata)
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
    --metadata name1=value1,name2=value2,... \
    ... other flags ...
```

## For more information

For more information, review the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions). You can also pose questions to the [Stack Overflow](http://www.stackoverflow.com) community with the tag `google-cloud-dataproc`.
See our other [Google Cloud Platform github
repos](https://github.com/GoogleCloudPlatform) for sample applications and
scaffolding for other frameworks and use cases.

### Mailing list

Subscribe to [cloud-dataproc-discuss@google.com](https://groups.google.com/forum/#!forum/cloud-dataproc-discuss) for announcements and discussion.

## Contributing changes

* See [CONTRIBUTING.md](CONTRIBUTING.md)

## Licensing

* See [LICENSE](LICENSE)

