# Intel BigDL and Analytics Zoo

This initialization action installs [BigDL](https://github.com/intel-analytics/BigDL)
on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
BigDL is a distributed deep learning library for Apache Spark. More information can be found on the
[project's website](https://bigdl-project.github.io/)

This script also supports Intel Analytics Zoo which includes BigDL as well. 
More information [project's website](https://analytics-zoo.github.io) 

## Using this initialization action

You can use this initialization to create a new Dataproc cluster with BigDL's Spark and PySpark libraries installed.

```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --initialization-actions gs://$MY_BUCKET/bigdl/bigdl.sh \
    --initialization-action-timeout 10m
```

This script downloads BigDL 0.7.2 for Dataproc 1.3 (Spark 2.3.0 and Scala 2.11.8). 
To download a different version of BigDL or Analytics Zoo distribution 
or one targeted to a different version of Spark/Scala, 
find the download URL from the [BigDL releases page](https://bigdl-project.github.io/master/#release-download), and set the metadata key `bigdl-download-url` 
or beside [maven packages](https://repo1.maven.org/maven2/com/intel/analytics/). 
The URL should end in `-dist.zip`.

For example, for Dataproc 1.0 (Spark 1.6 and Scala 2.10) and BigDL v0.7.2:

```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --image-version 1.0 \
    --initialization-actions gs://$MY_BUCKET/bigdl/bigdl.sh \
    --initialization-action-timeout 10m \
    --metadata 'bigdl-download-url=https://repo1.maven.org/maven2/com/intel/analytics/bigdl/dist-spark-1.6.2-scala-2.10.5-all/0.7.2/dist-spark-1.6.2-scala-2.10.5-all-0.7.2-dist.zip'
```

Or, for example, to download Analytics Zoo 0.4.0 with BigDL v0.7.2 for Dataproc 1.3 (Spark 2.3) use this:

```
gcloud dataproc clusters create <CLUSTER_NAME> \
    --image-version 1.3 \
    --initialization-actions gs://$MY_BUCKET/bigdl/bigdl.sh \
    --initialization-action-timeout 10m \
    --metadata 'bigdl-download-url=https://repo1.maven.org/maven2/com/intel/analytics/zoo/analytics-zoo-bigdl_0.7.2-spark_2.3.1/0.4.0/analytics-zoo-bigdl_0.7.2-spark_2.3.1-0.4.0-dist-all.zip'
```
 

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes

* You cannot use preemptible VMs with this init action, nor scale (add or remove workers from) the cluster. BigDL needs to know the exact number of Spark executors and cores per executor to make optimizations for Intel's MKL library (which BigDL ships with). This init action statically sets `spark.executor.instances` based on the original size of the cluster, and **disables** dynamic allocation (`spark.dynamicAllocation.enabled=false`).
* The init action sets `spark.executor.instances` such that a single application takes up all the resources in a cluster. To run multiple applications simulatenously, override `spark.executor.instances` on each job using `--properties` to `gcloud dataproc jobs submit [spark|pyspark|spark-sql]` or `--conf` to `spark-shell`/`spark-submit`. Note that each application needs to schedule an app master in addition to the executors.
