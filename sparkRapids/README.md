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

To use RAPIDS Accelerator For Apache Spark, XGBoost4j with Spark 3

*   Apache Spark 3.0+
*   Hardware Requirements
    *   NVIDIA Pascal™ GPU architecture or better (V100, P100, T4 and later)
    *   Multi-node clusters with homogenous GPU configuration
*   Software Requirements
    *   NVIDIA GPU driver 440.33+
    *   CUDA v11.5/v11.0/v10.2/v10.1
    *   NCCL 2.11.4+
    *   Ubuntu 18.04, Ubuntu 20.04 or Rocky Linux 7, Rocky Linux8, Debian 10

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

```bash
export CLUSTER_NAME=<cluster_name>
export GCS_BUCKET=<your bucket for the logs and notebooks>
export REGION=<region>
export NUM_GPUS=1
export NUM_WORKERS=2
export CUDA_VER=11.5

gcloud dataproc clusters create $CLUSTER_NAME  \
    --region $REGION \
    --image-version=2.0-ubuntu18 \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 200 \
    --num-workers $NUM_WORKERS \
    --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
    --worker-machine-type n1-standard-8 \
    --num-worker-local-ssds 1 \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/sparkRapids/spark-rapids.sh \
    --optional-components=JUPYTER,ZEPPELIN \
    --metadata gpu-driver-provider="NVIDIA",rapids-runtime="SPARK",cuda-version="$CUDA_VER" \
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