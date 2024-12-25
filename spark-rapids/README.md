# SPARK-RAPIDS

The [RAPIDS Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/) leverages GPUs
to accelerate processing via the [RAPIDS libraries](http://rapids.ai). This initialization
action supports Spark runtimes for RAPIDS on
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

### Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

This initialization action will install RAPIDS on Dataproc for Spark.
RAPIDS Accelerator For Apache Spark is supported on Dataproc 2.0+ (Spark 3.0)+.

## RAPIDS Accelerator For Apache Spark

### Prerequisites
Please find the [RAPIDS Accelerator For Apache Spark](https://nvidia.github.io/spark-rapids/) 
official document for the hardware and software [requirements](https://nvidia.github.io/spark-rapids/docs/download.html).

This section describes how to create
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster with
[RAPIDS Accelerator For Apache Spark](https://github.com/NVIDIA/spark-rapids)

### Step 1. Create Dataproc cluster with RAPIDS Accelerator For Apache Spark

The following command will create a new Dataproc cluster named `CLUSTER_NAME`
with installed GPU drivers, RAPIDS Accelerator For Apache Spark, Spark RAPIDS XGBoost
libraries and Jupyter Notebook.

A few notes:

*   for better GPU performance it's recommended to remove IO bottleneck as much
    as possible, that includes faster disk/networking.
*   Adjust Spark properties in cluster creation command to the hardware
    availability, some rule of thumb, number of task per executor should be 2x of
    `spark.rapids.sql.concurrentGpuTasks`
*   For best practice, please refer to NVIDIA
    [getting started guide](https://nvidia.github.io/spark-rapids/)
*   For some images version such as 2.1-ubuntu20 we need to disable secure boot by adding
    the config `--no-shielded-secure-boot` while creating the cluster, because it requires 
    reboot after installing GPU driver.

```bash
export CLUSTER_NAME=<cluster_name>
export GCS_BUCKET=<your bucket for the logs and notebooks>
export REGION=<region>
export NUM_GPUS=1
export NUM_WORKERS=2

gcloud dataproc clusters create $CLUSTER_NAME  \
    --region $REGION \
    --image-version=2.2-ubuntu22 \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 200 \
    --num-workers $NUM_WORKERS \
    --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
    --worker-machine-type n1-standard-8 \
    --num-worker-local-ssds 1 \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/spark-rapids/spark-rapids.sh \
    --bucket $GCS_BUCKET \
    --subnet=default \
    --enable-component-gateway
```

User can adjust Spark resource default allocation by adding following to
`--properties` flag (the numbers should be adjusted given the hardware resource
availability and Spark job requirements):

```bash
spark:spark.task.resource.gpu.amount=0.125
spark:spark.executor.cores=4
spark:spark.task.cpus=1
spark:spark.executor.memory=8G
```

After submitting this command, please go to the Google Cloud Platform console on
your browser. Search for "Dataproc" and click on the "Dataproc" icon. This will
navigate you to the Dataproc clusters page. “Dataproc” page lists all Dataproc
clusters created under your project directory. You can see `CLUSTER_NAME` with
status "Running". This cluster is now ready to host RAPIDS Spark and XGBoost
applications.

* Advanced feature:

  We also support [Multi-Instance GPU](https://www.nvidia.com/en-gb/technologies/multi-instance-gpu/) (MIG) feature of the NVIDIA Ampere architecture. 

  After cluster creation each MIG instance will show up like a regular GPU to YARN. For instance, if you requested
2 workers each with 1 A100 and used the default 2 MIG instances per A100, the cluster would have a total of 4 GPUs
that can be allocated.

  It is important to note that CUDA 11 only supports enumeration of a single MIG instance. It is recommended that you
only request a single MIG instance per container. For instance, if running Spark only request
1 GPU per executor (spark.executor.resource.gpu.amount=1). Please see the
[MIG user guide](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/) for more information.

  First decide which Ampere based GPU you are using. In the example we use the A100.
Decide the number of MIG instances and [instance profiles to use](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/#lgi).
By default if the MIG profiles are not specified it will configure 2 MIG instances with profile id 9. If
a different instance profile is required, you can specify it in the MIG_CGI metadata parameter. Either a
profile id or the name (ie 3g.20gb) can be specified. For example:

    ```bash
        --metadata=^:^MIG_CGI='3g.20gb,9'
    ```
  Create cluster with MIG enabled:
    
    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --worker-machine-type a2-highgpu-1g
        --worker-accelerator type=nvidia-tesla-a100,count=1 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/spark-rapids/spark-rapids.sh \
        --metadata=startup-script-url=gs://goog-dataproc-initialization-actions-${REGION}/spark-rapids/mig.sh
    ```

### Step 2. Run a sample query and exam GPU usage

Once you have started your Spark shell or Zeppelin notebook you can run the
following commands to do a basic join and look at the UI to see that it runs on
the GPU.

```scala
val df = sc.makeRDD(1 to 10000000, 6).toDF
val df2 = sc.makeRDD(1 to 10000000, 6).toDF
val out = df.select( $"value" as "a").join(df2.select($"value" as "b"), $"a" === $"b")
out.count()
out.explain()
```

From `out.explain()`, you should see `GpuRowToColumn`, `GpuFilter`,
`GpuColumnarExchange`, those all indicate things that would run on the GPU.
In some releases, you might not see that due to AQE has not finalized the plan. Please see
[RAPIDS Spark Q&A for more details](https://nvidia.github.io/spark-rapids/docs/FAQ.html#is-adaptive-query-execution-aqe-supported)

Or go to the Spark UI and click on the application you ran and on the "SQL" tab.
If you click the operation "count at ...", you should see the graph of Spark
Executors and some of those should have the "GPU" label as well.

If you want to monitor GPU metrics on Dataproc, you can create the cluster with additional
[metadata](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/metadata) and
[scopes](https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create#--scopes):
```
--metadata install-gpu-agent="true"
--scopes monitoring
```
You can then monitor the following metrics on [Web UI](https://console.cloud.google.com/monitoring/metrics-explorer),
we should be able to see "Resource & Metric" -> "VM Instance" -> "Custom":
* **custom.googleapis.com/instance/gpu/utilization** - The GPU cores utilization in %.
* **custom.googleapis.com/instance/gpu/memory_utilization** - The GPU memory bandwidth utilization in %.
* **custom.googleapis.com/instance/gpu/memory_total** - Total memory of the GPU card in MB.
* **custom.googleapis.com/instance/gpu/memory_used** - Used memory of the GPU card.
* **custom.googleapis.com/instance/gpu/memory_free** - Available memory of the GPU card.
* **custom.googleapis.com/instance/gpu/temperature** - Temperature of the GPU.
The metrics are sent with attached label, marking them by the gpu_type and gpu_bus_id.
This way, instances with multiple GPUs attached can report the metrics of their cards separately.
You can later aggregate or filter those metrics in the Cloud Monitoring systems.
