# RAPIDS

[RAPIDS](https://rapids.ai/) suite of open source software libraries and APIs 
gives you the ability to execute end-to-end data science and analytics pipelines 
entirely on GPUs. Licensed under Apache 2.0, RAPIDS is incubated by NVIDIA® based 
on extensive hardware and data science science experience. Its core libraries includes 
cuDF, cuML and XGBoost ... etc. To scale out RAPIDS, this initialization action deploy 
Dask-based RAPIDS and Spark-base RAPIDS on 
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

# Spark-based RAPIDS

This section deploy the dependency of RAPIDS spark
GPU(https://github.com/rapidsai/spark-examples) on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.
It is required to set `gpu-driver-provider="NVIDIA", rapids-runtime="SPARK"` in `metadata` session. 


Prerequisites
-------------
* Apache Spark 2.3+
* Hardware Requirements
  * NVIDIA Pascal™ GPU architecture or better (V100, P100, T4 and later)
  * Multi-node clusters with homogenous GPU configuration
* Software Requirements
  * NVIDIA driver 410.48+
  * CUDA V10.1/10.0/9.2
  * NCCL 2.4.7 and later
* `EXCLUSIVE_PROCESS` must be set for all GPUs in each NodeManager.(Initialization script provided in this guide will set this mode by default)
* `spark.dynamicAllocation.enabled` must be set to False for spark


Before you begin, please make sure you have installed [Google Cloud SDK](https://cloud.google.com/sdk/) and selected your project directory on your local machine. The following steps require a GCP project directory and Google Storage bucket associated with the project directory.

### Step 1, Download dataset and Apps for your spark GPU cluster 

[Spark examples](https://github.com/rapidsai/spark-examples/) provides instructions on:
1.  Compiled scala jar files
2.  PySpark app files
3.  Sample datasets for XGBoost apps

### Step 2, Create a GPU Cluster with pre-installed GPU drivers, Spark RAPIDS libraries, Spark XGBoost libraries and Jupyter Notebook  

The following command will create a new spark GPU cluster named `CLUSTER_NAME`. You might to modify `GCS_BUCKET` to 
storage dataproc logs and `CLUSTER_NAME` for your cluster. Also you will need `gcloud` command interface set up. Also 
modify `--properties` to include update-to-date jar file released by NVIDIA Spark XGBoost team.  

```bash
export GCS_BUCKET=your-gcs-bucket
export CLUSTER_NAME=my-gpu-cluster
export ZONE=europe-west4-c
export REGION=europe-west4
export NUM_GPUS=2
export NUM_WORKERS=2
# please check rapid.sh for related default version information
export RAPIDS_SPARK_VERSION='2.x'
export RAPIDS_VERSION='1.0.0-Beta4'
export RAPIDS_CUDF_VERSION='0.9.2-cuda10'

gcloud beta dataproc clusters create $CLUSTER_NAME  \
    --zone $ZONE \
    --region $REGION \
    --master-machine-type n1-standard-16 \
    --master-boot-disk-size 200 \
    --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
    --worker-machine-type n1-standard-32 \
    --worker-boot-disk-size 200 \
    --num-worker-local-ssds 1 \
    --num-workers $NUM_WORKERS \
    --image-version 1.4-ubuntu18 \
    --bucket $GCS_BUCKET \
    --metadata gpu-driver-provider="NVIDIA", rapids-runtime="SPARK" \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
    --optional-components=ANACONDA,JUPYTER,ZEPPELIN \
    --subnet=default \
    --properties "^#^spark:spark.dynamicAllocation.enabled=false#spark:spark.shuffle.service.enabled=false#spark:spark.submit.pyFiles=/usr/lib/spark/python/lib/xgboost4j-spark_${RAPIDS_SPARK_VERSION}-${RAPIDS_VERSION}.jar#spark:spark.jars=/usr/lib/spark/jars/xgboost4j-spark_${RAPIDS_SPARK_VERSION}-${RAPIDS_VERSION}.jar,/usr/lib/spark/jars/xgboost4j_${RAPIDS_SPARK_VERSION}-${RAPIDS_VERSION}.jar,/usr/lib/spark/jars/cudf-${RAPIDS_CUDF_VERSION}.jar" \
    --enable-component-gateway
```

After submitting the commands, please go to the Google Cloud Platform console on your browser. Search for "Dataproc" and click on the "Dataproc" icon. This will navigate you to the Dataproc clusters page. “Dataproc” page lists all Dataproc clusters created under your project directory. You can see “my-gpu-cluster” with Status "Running". This cluster is now ready to host RAPIDS Spark XGBoost applications.

### Step 3. Upload and run a sample XGBoost PySpark app to the Jupyter notebook on your GCP cluster.

Once the cluster has been created, yarn resource manager could be accessed on
port `8088` on the Dataproc master node.

To connect to the dataproc web interface, you will need to create an SSH tunnel
as described in the
[dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces)
documentation. Or go to the dataproc cluster web interface.

To open the Jupyter notebook, click on the “my-gpu-cluster” under Dataproc page and navigate to the "Web Interfaces" Tab. Under the "Web Interfaces", click on the “Jupyter” link.
This will open the Jupyter Notebook. This notebook is running on the “my-gpu-cluster” we just created. 

Next, to upload the Sample PySpark App into the Jupyter notebook, use the “Upload” button on the Jupyter notebook. Sample Pyspark notebook is inside the `spark-examples/examples/notebooks/python/’ directory. Once you upload the sample mortgage-gpu.ipynb, make sure to change the kernel to “PySpark” under the "Kernel" tab using "Change Kernel" selection.The Spark XGBoost Sample Jupyter notebook is now ready to run on a “my-gpu-cluster”.
To run the Sample PySpark app on Jupyter notebook, please follow the instructions on the notebook and also update the data path for sample datasets.
```
train_data = GpuDataReader(spark).schema(schema).option('header', True).csv('gs://$GCS_BUCKET/mortgage-small/train')
eval_data = GpuDataReader(spark).schema(schema).option('header', True).csv('gs://$GCS_BUCKET/mortgage-small/eval')
```

### Step 4, Execute the sample app.
#### 4a) Submit Scala Spark App on GPUs

Please build the `sample_xgboost_apps jar` with dependencies as specified in the [guide](/getting-started-guides/building-sample-apps/scala.md) and place the jar file (`sample_xgboost_apps-0.1.4-jar-with-dependencies.jar`) under the `gs://$GCS_BUCKET/spark-gpu` folder. To do this you can either drag and drop files from your local machine into the GCP [storage browser](https://console.cloud.google.com/storage/browser/rapidsai-test-1/?project=nv-ai-infra&organizationId=210881545417), or use the [gsutil cp](https://cloud.google.com/storage/docs/gsutil/commands/cp) as shown before to do this from a command line.

Use the following commands to submit sample Scala app on this GPU cluster. Note that `spark.task.cpus` need to match `spark.executor.cores`.

To submit such a job run:

```bash
export MAIN_CLASS=ai.rapids.spark.examples.mortgage.GPUMain
export RAPIDS_JARS=gs://$GCS_BUCKET/sample_xgboost_apps-0.1.4-jar-with-dependencies.jar
export DATA_PATH=gs://$GCS_BUCKET
export TREE_METHOD=gpu_hist
export SPARK_NUM_EXECUTORS=4
export SPARK_NUM_CORES_PER_EXECUTOR=12
export SPARK_EXECUTOR_MEMORY=22G
export SPARK_DRIVER_MEMORY=10g
export SPARK_EXECUTOR_MEMORYOVERHEAD=22G

gcloud beta dataproc jobs submit spark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --class=$MAIN_CLASS \
    --jars=$RAPIDS_JARS \
    --properties=spark.executor.cores=${SPARK_NUM_CORES_PER_EXECUTOR},spark.task.cpus=${SPARK_NUM_CORES_PER_EXECUTOR},spark.executor.instances=${SPARK_NUM_EXECUTORS},spark.driver.memory=${SPARK_DRIVER_MEMORY},spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORYOVERHEAD},spark.executor.memory=${SPARK_EXECUTOR_MEMORY},spark.executorEnv.LD_LIBRARY_PATH=/usr/local/lib/x86_64-linux-gnu:/usr/local/cuda-10.0/lib64:${LD_LIBRARY_PATH} \
    -- \
    -format=csv \
    -numRound=100 \
    -numWorkers=${SPARK_NUM_EXECUTORS} \
    -treeMethod=${TREE_METHOD} \
    -trainDataPath=${DATA_PATH}/mortgage/csv/train/mortgage_train_merged.csv  \
    -evalDataPath=${DATA_PATH}/mortgage/csv/test/mortgage_eval_merged.csv \
    -maxDepth=8  
```
#### 4b) Submit  PySpark App on GPUs

Please build the sample_xgboost pyspark app as specified in the [guide](/getting-started-guides/building-sample-apps/python.md) and place the sample.zip file into GCP storage bucket.

Use the following commands to submit sample PySpark app on this GPU cluster.

```bash
    export DATA_PATH=gs://$GCS_BUCKET
    export LIBS_PATH=/usr/lib/spark/jars/
    export SPARK_DEPLOY_MODE=cluster
    export SPARK_PYTHON_ENTRYPOINT=${LIBS_PATH}/main.py
    export MAIN_CLASS=ai.rapids.spark.examples.mortgage.gpu_main
    export RAPIDS_JARS=${LIBS_PATH}/cudf-${RAPIDS_CUDF_VERSION}.jar,${LIBS_PATH}/xgboost4j_${RAPIDS_SPARK_VERSION}.jar,${LIBS_PATH}/xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar
    export SPARK_PY_FILES=${LIBS_PATH}/xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar,${LIBS_PATH}/sample.zip
    export TREE_METHOD=gpu_hist
    export SPARK_NUM_EXECUTORS=4
    export SPARK_NUM_CORES_PER_EXECUTOR=12
    export SPARK_EXECUTOR_MEMORY=22G
    export SPARK_DRIVER_MEMORY=10g
    export SPARK_EXECUTOR_MEMORYOVERHEAD=22G

    gcloud beta dataproc jobs submit pyspark \
        --cluster=$CLUSTER_NAME \
        --region=$REGION \
        --properties=spark.executor.cores=${SPARK_NUM_CORES_PER_EXECUTOR},spark.task.cpus=${SPARK_NUM_CORES_PER_EXECUTOR},spark.executor.instances=${SPARK_NUM_EXECUTORS},spark.driver.memory=${SPARK_DRIVER_MEMORY},spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORYOVERHEAD},spark.executor.memory=${SPARK_EXECUTOR_MEMORY},spark.executorEnv.LD_LIBRARY_PATH=/usr/local/lib/x86_64-linux-gnu:/usr/local/cuda-10.0/lib64:${LD_LIBRARY_PATH} \        
        --jars=$RAPIDS_JARS \
        --py-files=${SPARK_PY_FILES} \
        ${SPARK_PYTHON_ENTRYPOINT} \
        --mainClass=${MAIN_CLASS} \                                                  \
        -- \
        -format=csv \
        -numRound=100 \
        -numWorkers=${SPARK_NUM_EXECUTORS} \
        -treeMethod=${TREE_METHOD} \
        -trainDataPath=${DATA_PATH}/mortgage/csv/train/mortgage_train_merged.csv  \
        -evalDataPath=${DATA_PATH}/mortgage/csv/test/mortgage_eval_merged.csv \
        -maxDepth=8 
```

### Important notes

*   RAPIDS Spark GPU is supported on Pascal or newer GPU architectures (Tesla
    K80s will _not_ work with RAPIDS). See
    [list](https://cloud.google.com/compute/docs/gpus/) of available GPU types
    by GCP region.
*   You must set a GPU accelerator type for worker nodes, else the GPU driver
    install will fail and the cluster will report an error state.
*   When running RAPIDS Spark GPU with multiple attached GPUs, We recommend an
    n1-standard-32 worker machine type or better to ensure sufficient
    host-memory for buffering data to and from GPUs. When running with a single
    attached GPU, GCP only permits machine types up to 24 vCPUs.


# Dask-based RAPIDS

This section automates the process of setting up a Dask-cuDF
cluster. It is required to set `gpu-driver-provider="NVIDIA", rapids-runtime="DASK"` in `metadata` session.

-   creates `RAPIDS` conda environment and installs RAPIDS conda packages.
-   starts systemd services of Dask CUDA cluster:
    -   `dask-scheduler` and optionally `dask-cuda-worker` on the Dataproc
        master node.
    -   `dask-cuda-worker` on the Dataproc worker nodes.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with
RAPIDS installed:

1.  Using the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    export GCS_BUCKET=<gcs_bucket_name>
    export CLUSTER_NAME=<cluster_name>
    export ZONE=<zone>
    export REGION=<region>
    export NUM_GPUS=4
    export NUM_WORKERS=2
    
    gcloud beta dataproc clusters create $CLUSTER_NAME  \
        --zone $ZONE \
        --region $REGION \
        --master-machine-type n1-standard-32 \
        --master-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
        --master-boot-disk-size 200 \
        --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
        --worker-machine-type n1-standard-32 \
        --worker-boot-disk-size 200 \
        --num-workers $NUM_WORKERS \
        --image-version 1.4-ubuntu18 \
        --bucket $GCS_BUCKET \
        --metadata gpu-driver-provider="NVIDIA",rapids-runtime="DASK" \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
        --optional-components=ANACONDA \
        --subnet=default \
        --initialization-action-timeout=60m \
        --enable-component-gateway
    ```

2.  Once the cluster has been created, the Dask scheduler listens for workers on
    port `8786`, and its status dashboard is on port `8787` on the Dataproc
    master node. These ports can be changed by modifying the
    `install_systemd_dask_service` function in the initialization action script.

To connect to the Dask web interface, you will need to create an SSH tunnel as
described in the
[dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces)
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

#### Master As Worker Configuration

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
    export GCS_BUCKET=<gcs_bucket_name>
    export CLUSTER_NAME=<cluster_name>
    export ZONE=<zone>
    export REGION=<region>
    export NUM_GPUS=4
    export NUM_WORKERS=2
    
    gcloud beta dataproc clusters create $CLUSTER_NAME  \
        --zone $ZONE \
        --region $REGION \
        --master-machine-type n1-standard-32 \
        --master-boot-disk-size 200 \
        --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
        --worker-machine-type n1-standard-32 \
        --worker-boot-disk-size 200 \
        --num-workers $NUM_WORKERS \
        --image-version 1.4-ubuntu18 \
        --bucket $GCS_BUCKET \
        --metadata gpu-driver-provider="NVIDIA",rapids-runtime="DASK",dask-cuda-worker-on-master=false \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
        --optional-components=ANACONDA \
        --subnet=default \
        --initialization-action-timeout=60m \
        --enable-component-gateway
```

## Important notes

*   RAPIDS initialization action depends on the
    [Anaconda](https://cloud.google.com/dataproc/docs/concepts/components/anaconda)
    component, which should be included at cluster creation time via the
    `--optional-components-ANACONDA` argument.
*   RAPIDS initialization action depends on the [GPU](/gpu/README.md)
    initialization action, which should be included at cluster creation time via
    the `--initialization-actions
    gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh`
    argument and configured to install NVIDIA-provided GPU driver via
    `--metadata gpu-driver-provider=NVIDIA`.
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
*   `conda-environment.yaml` embedded in the initalization action can be updated
    based on which RAPIDS versions you wish to install
*   Installing the GPU driver and conda packages takes about 10 minutes
*   When deploying RAPIDS on few GPUs, ETL style processing with cuDF and Dask
    can run sequentially. When training ML models, you _must_ have enough total
    GPU memory in your cluster to hold the training set in memory.
*   Dask's status dashboard is set to use HTTP port `8787` and is accessible
    from the master node
*   High-Availability configuration is discouraged as
    [the dask-scheduler doesn't support it](https://github.com/dask/distributed/issues/1072).
*   Dask scheduler and worker logs are written to /var/log/dask-scheduler.log
    and /var/log/dask-cuda-workers.log on the master and host respectively.
*   If using the
    [Jupyter optional component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter),
    note that RAPIDS init-actions will install
    [nb_conda_kernels](https://github.com/Anaconda-Platform/nb_conda_kernels)
    and restart Jupyter so that the RAPIDS conda environment appears in Jupyter.
