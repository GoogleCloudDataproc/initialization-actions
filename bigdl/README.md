# Intel BigDL and Analytics Zoo

This initialization action installs [BigDL](https://github.com/intel-analytics/BigDL) on a [Dataproc](https://cloud.google.com/dataproc) cluster. BigDL is a distributed deep learning library for Apache Spark. See the GitHub [BigDL website](https://bigdl-project.github.io/) for more information.

This script also supports the Intel [Analytics Zoo](https://software.intel.com/content/www/us/en/develop/topics/ai/analytics-zoo.html),
which includes BigDL. See the GitHub [Analytics Zoo website](https://analytics-zoo.github.io) site for more information.

## Using the initialization action

**:warning: NOTICE:** See [How initialization actions are used](/README.md#how-initialization-actions-are-used) and [Important considerations and guidelines](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions#important_considerations_and_guidelines) for additional information.

Use this initialization action to create a Dataproc cluster with BigDL's Spark and PySpark libraries installed.

Note: In the following examples, a 10-minute timeout is set with the `--initialization-action-timeout 10m` flag to allow for the time needed to install BigDL on cluster nodes.

```
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/bigdl/bigdl.sh \
    --initialization-action-timeout 10m
```

By default, this initialization action script downloads BigDL 0.7.2 for Dataproc 1.3 (Spark 2.3.0 and Scala 2.11.8). To download a different BigDL or Analytics Zoo distribution version or one targeted to a different version of Spark/Scala, find the download URL on the [BigDL releases page](https://bigdl-project.github.io/master/#release-download) or in the [Maven repository](https://repo1.maven.org/maven2/com/intel/analytics/), then set the `bigdl-download-url` metadata key. The URL should end in `-dist.zip`.

Example for Dataproc 1.0 (Spark 1.6 and Scala 2.10) and BigDL v0.7.2:

```
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --image-version 1.0 \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/bigdl/bigdl.sh \
    --initialization-action-timeout 10m \
    --metadata 'bigdl-download-url=https://repo1.maven.org/maven2/com/intel/analytics/bigdl/dist-spark-1.6.2-scala-2.10.5-all/0.7.2/dist-spark-1.6.2-scala-2.10.5-all-0.7.2-dist.zip'
```

Example for Dataproc 1.3 (Spark 2.3) and Analytics Zoo 0.4.0 with BigDL v0.7.2:

```
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --image-version 1.3 \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/bigdl/bigdl.sh \
    --initialization-action-timeout 10m \
    --metadata 'bigdl-download-url=https://repo1.maven.org/maven2/com/intel/analytics/zoo/analytics-zoo-bigdl_0.7.2-spark_2.3.1/0.4.0/analytics-zoo-bigdl_0.7.2-spark_2.3.1-0.4.0-dist-all.zip'
```

## Important notes

* You cannot use preemptible VMs with this initilization action, and cannot scale (add or remove workers from) the cluster. BigDL expects a fixed number of Spark executors and cores per executor to make optimizations for Intel's MKL library (shipped with BigDL). This initilization action statically sets `spark.executor.instances` based on the original size of the cluster, and **disables** dynamic allocation (`spark.dynamicAllocation.enabled=false`).
* This initilization action sets `spark.executor.instances` so that a single application uses all cluster resources. To run multiple applications simulatenously, override `spark.executor.instances` on each job by adding the `--properties` flag to `gcloud dataproc jobs submit [spark|pyspark|spark-sql]` or the `--conf` flag to `spark-shell`/`spark-submit`. Note that each application schedules an app master in addition to the executors.
