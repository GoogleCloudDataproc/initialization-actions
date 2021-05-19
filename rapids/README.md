# RAPIDS

[RAPIDS](https://rapids.ai/) suite of open source software libraries and APIs
gives you the ability to execute end-to-end data science and analytics pipelines
entirely on GPUs. Licensed under Apache 2.0, RAPIDS is incubated by NVIDIA®
based on extensive hardware and data science experience. Its core libraries
includes cuDF, cuML, XGBoost, etc. To scale out RAPIDS, this initialization
action supports Dask and Spark runtimes for RAPIDS on
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

### Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

This initialization action will install RAPIDS on Dataproc for either Spark or
Dask. RAPIDS Accelerator For Apache Spark is supported on Dataproc 2.0+ (Spark 3.0)+. 
Spark with RAPIDS XGBoost is support on Dataproc 1.5+ with Spark 2.4.
RAPIDS Accelerator For Dask is only supported on Dataproc 2.0+.

## RAPIDS Accelerator For Apache Spark

### Prerequisites

To use RAPIDS Accelerator For Apache Spark, XGBoost4j with Spark 3

*   Apache Spark 3.0+
*   Hardware Requirements
    *   NVIDIA Pascal™ GPU architecture or better (V100, P100, T4 and later)
    *   Multi-node clusters with homogenous GPU configuration
*   Software Requirements
    *   NVIDIA GPU driver 440.33+
    *   CUDA v11.0/v10.2/v10.1
    *   NCCL 2.4.7+
    *   Ubuntu 18.04, Ubuntu 20.04 or CentOS 7, CentOS8

This section describes how to create
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster with
[RAPIDS Accelerator For Apache Spark](https://github.com/NVIDIA/spark-rapids) and
[XGBoost4j](https://github.com/NVIDIA/spark-xgboost-examples).

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

gcloud dataproc clusters create $CLUSTER_NAME  \
    --region $REGION \
    --image-version=2.0-ubuntu18 \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 200 \
    --num-workers $NUM_WORKERS \
    --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
    --worker-machine-type n1-standard-8 \
    --num-worker-local-ssds 1 \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
    --optional-components=JUPYTER,ZEPPELIN \
    --metadata gpu-driver-provider="NVIDIA",rapids-runtime="SPARK" \
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

## Spark 2.x RAPIDS XGBoost

This section describes how to create
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster with
[XGBoost4j](https://github.com/NVIDIA/spark-xgboost-examples/tree/spark-2).

### Prerequisites

To use Spark RAPIDS XGBoost4j with Spark 2

*   Apache Spark 2.3+
*   Hardware Requirements
    *   NVIDIA Pascal™ GPU architecture or better (V100, P100, T4 and later)
    *   Multi-node clusters with homogenous GPU configuration
*   Software Requirements
    *   NVIDIA driver 410.48+
    *   CUDA v10.1/v10.0/v9.2
    *   NCCL 2.4.7+
*   `EXCLUSIVE_PROCESS` must be set for all GPUs in each NodeManager (set by
    default in this initialization action)
*   `spark.dynamicAllocation.enabled` property must be set to `false` for Spark
    (set by default in this initialization action)

### Step 1. Download dataset for Spark RAPIDS XGBoost application

From
[Spark examples](https://github.com/NVIDIA/spark-xgboost-examples/tree/spark-2)
repository download to your own bucket:

1.  PySpark application files
1.  A sample dataset for a XGBoost PySpark application

```bash
GCS_BUCKET=<bucket_name>
git clone --depth 1 https://github.com/NVIDIA/spark-xgboost-examples.git /tmp/rapidsai-spark-examples
git checkout spark-2

# Upload PySpark application files
pushd /tmp/rapidsai-spark-examples/examples/apps/python
zip -r samples.zip ai
gsutil cp samples.zip main.py gs://${GCS_BUCKET}/pyspark/
popd

# Upload a sample dataset for a XGBoost applications
pushd /tmp/rapidsai-spark-examples/datasets
tar -xzf mortgage-small.tar.gz
gsutil cp -r mortgage-small/ gs://${GCS_BUCKET}/
popd
```

### Step 2. Create Dataproc cluster with Spark RAPIDS XGBoost

The following command will create a new Dataproc cluster named `CLUSTER_NAME`
with installed GPU drivers, Spark RAPIDS XGBoost libraries and Jupyter Notebook.

```bash
export CLUSTER_NAME=<cluster_name>
export GCS_BUCKET=<your bucket for the logs and notebooks>
export REGION=<region>
gcloud dataproc clusters create $CLUSTER_NAME \
    --region $REGION \
    --image-version 1.5-ubuntu18 \
    --master-machine-type n1-standard-4 \
    --worker-machine-type n1-highmem-16 \
    --worker-accelerator type=nvidia-tesla-t4,count=1 \
    --optional-components=ANACONDA,JUPYTER,ZEPPELIN \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
    --metadata gpu-driver-provider=NVIDIA \
    --metadata rapids-runtime=SPARK \
    --bucket $GCS_BUCKET \
    --enable-component-gateway
```

After submitting this command, please go to the Google Cloud Platform console on
your browser. Search for "Dataproc" and click on the "Dataproc" icon. This will
navigate you to the Dataproc clusters page. “Dataproc” page lists all Dataproc
clusters created under your project directory. You can see `CLUSTER_NAME` with
status "Running". This cluster is now ready to host RAPIDS Spark XGBoost
applications.

### Step 3. Upload and run a sample XGBoost PySpark application to the Jupyter notebook on your cluster.

Once the cluster has been created, YARN Resource Manager could be accessed using
[Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)
from your browser.

To open the Jupyter notebook, click on the `CLUSTER_NAME` under Dataproc
clusters page and navigate to the "Web Interfaces" tab. Under the "Web
Interfaces", click on the “Jupyter” link. This will open the Jupyter Notebook.
This notebook is running on the just created `CLUSTER_NAME` cluster.

Next, to upload the Sample PySpark App into the Jupyter notebook, use the
“Upload” button on the Jupyter notebook. Sample PySpark notebook is inside the
[`spark-examples/examples/notebooks/python/`](https://github.com/NVIDIA/spark-xgboost-examples/tree/spark-2/examples/notebooks/python)
directory. Once you upload the sample `mortgage-gpu.ipynb`, make sure to change
the kernel to “PySpark” under the "Kernel" tab using "Change Kernel" selection.
The Spark XGBoost Sample Jupyter notebook is now ready to run on the cluster. To
run the Sample PySpark app on Jupyter notebook, please follow the instructions
in the notebook and also update the data path for sample datasets.

```python
train_data = GpuDataReader(spark).schema(schema).option('header', True).csv('gs://$GCS_BUCKET/mortgage-small/train')
eval_data = GpuDataReader(spark).schema(schema).option('header', True).csv('gs://$GCS_BUCKET/mortgage-small/eval')
```

### Step 4. Execute a sample application as Dataproc Spark job

#### 4a) Submit Spark Scala application on GPUs

Please build the `sample_xgboost_apps.jar` with dependencies as specified in the
[guide](https://github.com/NVIDIA/spark-xgboost-examples/blob/spark-2/getting-started-guides/building-sample-apps/scala.md)
and upload the jar file (`sample_xgboost_apps-0.1.4-jar-with-dependencies.jar`)
into the `gs://${GCS_BUCKET}/scala/` folder. To do this you can either drag and
drop files from your local machine into the
[Cloud Storage browser](https://console.cloud.google.com/storage/browser), or
use the
[gsutil cp command](https://cloud.google.com/storage/docs/gsutil/commands/cp) as
was shown previously to do this from a command line.

Use the following commands to submit sample Scala application on this cluster.
Note that `spark.task.cpus` need to match `spark.executor.cores`.

To submit such a job run:

```bash
GCS_BUCKET=<bucket_name>
CLUSTER_NAME=<cluster_name>
REGION=<region>
SPARK_NUM_EXECUTORS=4
SPARK_NUM_CORES_PER_EXECUTOR=12
SPARK_EXECUTOR_MEMORY=22G
SPARK_DRIVER_MEMORY=10g
SPARK_EXECUTOR_MEMORYOVERHEAD=22G
gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --class=ai.rapids.spark.examples.mortgage.GPUMain \
    --jars=gs://${GCS_BUCKET}/scala/sample_xgboost_apps-0.1.4-jar-with-dependencies.jar \
    --properties=spark.executor.cores=${SPARK_NUM_CORES_PER_EXECUTOR},spark.task.cpus=${SPARK_NUM_CORES_PER_EXECUTOR},spark.executor.instances=${SPARK_NUM_EXECUTORS},spark.driver.memory=${SPARK_DRIVER_MEMORY},spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORYOVERHEAD},spark.executor.memory=${SPARK_EXECUTOR_MEMORY},spark.executorEnv.LD_LIBRARY_PATH=/usr/local/lib/x86_64-linux-gnu:/usr/local/cuda-10.0/lib64:\${LD_LIBRARY_PATH} \
    -- \
    -format=csv \
    -numRound=100 \
    -numWorkers=$SPARK_NUM_EXECUTORS \
    -treeMethod=gpu_hist \
    -trainDataPath=gs://${GCS_BUCKET}/mortgage-small/train \
    -evalDataPath=gs://${GCS_BUCKET}/mortgage-small/eval \
    -maxDepth=8
```

#### 4b) Submit PySpark application on GPUs

Please build the XGBoost PySpark application as specified in the
[guide](https://github.com/nvidia/spark-xgboost-examples/blob/spark-2/getting-started-guides/building-sample-apps/python.md)
and upload `main.py` and `samples.zip` files into the
`gs://${GCS_BUCKET}/pyspark/` folder.

Use the following commands to submit sample PySpark application on this GPU
cluster.

```bash
GCS_BUCKET=<bucket_name>
CLUSTER_NAME=<cluster_name>
REGION=<region>
SPARK_NUM_EXECUTORS=4
SPARK_NUM_CORES_PER_EXECUTOR=12
SPARK_EXECUTOR_MEMORY=22G
SPARK_DRIVER_MEMORY=10g
SPARK_EXECUTOR_MEMORYOVERHEAD=22G
RAPIDS_SPARK_VERSION=2.x
RAPIDS_VERSION=1.0.0-Beta4

wget "https://repo1.maven.org/maven2/ai/rapids/xgboost4j-spark_${RAPIDS_SPARK_VERSION}/${RAPIDS_VERSION}/xgboost4j-spark_${RAPIDS_SPARK_VERSION}-${RAPIDS_VERSION}.jar"

gcloud dataproc jobs submit pyspark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --py-files=xgboost4j-spark_${RAPIDS_SPARK_VERSION}-${RAPIDS_VERSION}.jar,gs://${GCS_BUCKET}/pyspark/samples.zip \
    gs://${GCS_BUCKET}/pyspark/main.py \
    --properties=spark.executor.cores=${SPARK_NUM_CORES_PER_EXECUTOR},spark.task.cpus=${SPARK_NUM_CORES_PER_EXECUTOR},spark.executor.instances=${SPARK_NUM_EXECUTORS},spark.driver.memory=${SPARK_DRIVER_MEMORY},spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORYOVERHEAD},spark.executor.memory=${SPARK_EXECUTOR_MEMORY},spark.executorEnv.LD_LIBRARY_PATH=/usr/local/lib/x86_64-linux-gnu:/usr/local/cuda-10.0/lib64:\${LD_LIBRARY_PATH} \
    -- \
    --mainClass=ai.rapids.spark.examples.mortgage.gpu_main \
    --format=csv \
    --numRound=100 \
    --numWorkers=$SPARK_NUM_EXECUTORS \
    --treeMethod=gpu_hist \
    --trainDataPath=gs://${GCS_BUCKET}/mortgage-small/train \
    --evalDataPath=gs://${GCS_BUCKET}/mortgage-small/eval \
    --maxDepth=8
```

### Important notes

*   RAPIDS Spark GPU supported on Pascal or newer GPU architectures (Tesla K80s
    will _not_ work with RAPIDS). See
    [list](https://cloud.google.com/compute/docs/gpus/) of available GPU types
    by GCP region.
*   You must set a GPU accelerator type for worker nodes, else the GPU driver
    install will fail and the cluster will report an error state.
*   When running RAPIDS Spark GPU with multiple attached GPUs, We recommend an
    `n1-standard-32` worker machine type or better to ensure sufficient
    host-memory for buffering data to and from GPUs. When running with a single
    attached GPU, GCP only permits machine types up to 24 vCPUs.

## RAPIDS Accelerator For Dask

This section automates the process of setting up a Dataproc cluster with
DASK and RAPIDS installed. This requires additionally using the
[Dask initialization action](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/dask).

With the Dask initialization action, you can set up Dask to leverage `yarn`
for orchestration or `standalone` to leverage the `dask-scheduler`. Learn
more in the [Dask initialization action](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/dask).

### Create Dataproc cluster with Dask and RAPIDS

Using the `gcloud` command to create a new cluster with this initialization
action. RAPIDS with Dask is only supported on Dataproc 2.0+.

```bash
CLUSTER_NAME=<cluster_name>
REGION=<region>
DASK_RUNTIME=<yarn|standalone>
gcloud dataproc clusters create $CLUSTER_NAME \
    --region $REGION \
    --image-version 2.0-ubuntu18 \
    --master-machine-type n1-standard-32 \
    --master-accelerator type=nvidia-tesla-t4,count=2 \
    --worker-machine-type n1-standard-32 \
    --worker-accelerator type=nvidia-tesla-t4,count=2 \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/dask/dask.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
    --initialization-action-timeout=60m \
    --metadata gpu-driver-provider=NVIDIA,dask-runtime=${DASK_RUNTIME},rapids-runtime=DASK \
    --enable-component-gateway
```

### Run RAPIDS Accelerator For Dask workload

Once the cluster has been created, if using `standalone` mode, the Dask
scheduler listens for workers on port `8786`, and its status dashboard is on
port `8787` on the Dataproc master node. These ports can be changed by
modifying the `install_systemd_dask_service` function in the initialization action script.

To connect to the Dask web interface, you will need to create an SSH tunnel as
described in the
[Dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces)
documentation. You can also connect using the
[Dask Client Python API](http://distributed.dask.org/en/latest/client.html) from
a
[Jupyter notebook](https://cloud.google.com/dataproc/docs/concepts/components/jupyter),
or a from a plain Python script or interpreter session.

See
[the example notebook](https://github.com/rapidsai/notebooks-extended/blob/master/intermediate_notebooks/E2E/taxi/NYCTaxi-E2E.ipynb)
that demonstrates end to end data pre-processing (cuDF & Dask) and model
training (XGBoost) with RAPIDS APIs. Additional example notebooks
[are available](https://github.com/rapidsai/notebooks). See the
[RAPIDS documentation](https://docs.rapids.ai/) for API details.

RAPIDS is a relatively young project with APIs evolving quickly. If you
encounter unexpected errors or have feature requests, please file them at the
relevant [RAPIDS repo](https://github.com/rapidsai).

### Options

#### GPU Types & Driver Configuration

By default, this initialization action uses NVIDIA-provided GPU driver and CUDA
installed by [GPU initialization action](/gpu/install_gpu_driver.sh). If you
wish to install a different GPU driver and CUDA version see
[GPU initialization action README](/gpu/README.md) file for instructions.

RAPIDS works with
[all "compute" GPU models](https://cloud.google.com/compute/docs/gpus/) except
for the Tesla K80. Currently, only CUDA 10.0 is supported for RAPIDS on
Dataproc.

#### Master as Worker Configuration

By default, the master node also runs `dask-cuda-worker`. This is useful for
smaller scale jobs - processes run on 4 GPUs in a single node will usually be
more performant than when run on the same number of GPUs in separate server
nodes (due to higher communication costs).

If you want to save the master's GPU(s) for other purposes, this behavior is
configurable via a metadata key using `--metadata`.

*   `dask-cuda-worker-on-master=false` - whether to run `dask-cuda-worker` on
    the master node

For example:

```bash
CLUSTER_NAME=<cluster_name>
REGION=<region>
DASK_RUNTIME=<yarn|standalone>
gcloud dataproc clusters create $CLUSTER_NAME \
    --region $REGION \
    --image-version 2.0-ubuntu18 \
    --master-machine-type n1-standard-32 \
    --master-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
    --worker-machine-type n1-standard-32 \
    --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/dask/dask.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
    --initialization-action-timeout=60m \
    --metadata gpu-driver-provider=NVIDIA,dask-runtime=${DASK_RUNTIME},rapids-runtime=DASK,dask-cuda-worker-on-master=false \
    --enable-component-gateway
```

### Important notes

*   RAPIDS initialization action depends on the
    [Anaconda](https://cloud.google.com/dataproc/docs/concepts/components/anaconda)
    component, which should be included at cluster creation time via the
    `--optional-components=ANACONDA` argument. - this is no longer needed for Dataproc 2.0
*   RAPIDS initialization action depends on the [GPU](/gpu/README.md)
    initialization action, which should be included at cluster creation time via
    the `--initialization-actions
    gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh`
    argument and configured to install NVIDIA-provided GPU driver via
    `--metadata gpu-driver-provider=NVIDIA`.
*   In addition to [GPU](/gpu/README.md), RAPIDS for Dask also relies on the
    [Dask](/dask/README.md) initialization action. You can configure the Dask
    runtime via `--metadata dask-runtime=yarn|standalone`. `yarn` is the 
    default.
*   RAPIDS is supported on Pascal or newer GPU architectures (Tesla K80s will
    _not_ work with RAPIDS). See
    [list](https://cloud.google.com/compute/docs/gpus/) of available GPU types
    by GCP region.
*   You must set a GPU accelerator type for both master and worker nodes, else
    the GPU driver install will fail and the cluster will report an error state.
*   When running RAPIDS with multiple attached GPUs, We recommend an
    `n1-standard-32` worker machine type or better to ensure sufficient
    host-memory for buffering data to and from GPUs. When running with a single
    attached GPU, GCP only permits machine types up to 24 vCPUs.
*   `conda-environment.yaml` embedded in the initialization action can be
    updated based on which RAPIDS versions you wish to install
*   Installing the GPU driver and conda packages takes about 10 minutes
*   When deploying RAPIDS on few GPUs, ETL style processing with cuDF and Dask
    can run sequentially. When training ML models, you _must_ have enough total
    GPU memory in your cluster to hold the training set in memory.
*   Dask's status dashboard is set to use HTTP port `8787` and is accessible
    from the master node
*   High-Availability configuration is discouraged as
    [the dask-scheduler doesn't support it](https://github.com/dask/distributed/issues/1072).
*   Dask scheduler and worker logs are written to `/var/log/dask-scheduler.log`
    and `/var/log/dask-cuda-workers.log` on the master and host respectively.
