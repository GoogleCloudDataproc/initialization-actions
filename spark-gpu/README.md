# RAPIDS Spark GPU

This initialization action deploy the dependency of RAPIDS spark GPU(https://github.com/rapidsai/spark-examples) on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

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

Our initialization action does the following:

### Step 1.  Initialization steps to download required files for Spark RAPIDS XGBoost app

Before you create a cluster, please git clone the [spark-examples directory](https://github.com/rapidsai/spark-examples) to your local machine. `cd` into the spark-examples/getting-started-guides/csp/gcp/spark-gpu directory. Open the rapids.sh script using a text editor.  Modify the `GCS_BUCKET=my-bucket` line and `INIT_ACTIONS_BUCKET=my-bucket`(this parameter will not be required after init action merged into dataproc official repo) to specify your google GCP bucket name.  

Execute the commands below while in the spark-examples folder.  These commands will copy the following files into your GCP bucket: 

1. Initialization scripts for GPU and RAPIDS Spark, 
2. PySpark app files  
3. A sample dataset for a XGBoost PySpark app 
4. The latest Spark RAPIDS XGBoost jar files from the public maven repository

```bash
export GCS_BUCKET=my-bucket
export RAPIDS_SPARK_VERSION='2.x-1.0.0-Beta3'
export RAPIDS_CUDF_VERSION='0.9.2-cuda10'
pushd getting-started-guides/csp/gcp/spark-gpu
gsutil cp -r internal gs://$GCS_BUCKET/spark-gpu/
gsutil cp rapids.sh gs://$GCS_BUCKET/spark-gpu/rapids.sh
popd
pushd datasets/
tar -xvf mortgage-small.tar.gz
gsutil cp -r mortgage-small/ gs://$GCS_BUCKET/
popd
wget -O cudf-${RAPIDS_CUDF_VERSION}.jar https://repo1.maven.org/maven2/ai/rapids/cudf/${RAPIDS_CUDF_VERSION%-*}/cudf-${RAPIDS_CUDF_VERSION}.jar
wget -O xgboost4j_${RAPIDS_SPARK_VERSION}.jar https://repo1.maven.org/maven2/ai/rapids/xgboost4j_${RAPIDS_SPARK_VERSION/-/\/}/xgboost4j_${RAPIDS_SPARK_VERSION}.jar
wget -O xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar https://repo1.maven.org/maven2/ai/rapids/xgboost4j-spark_${RAPIDS_SPARK_VERSION/-/\/}/xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar
gsutil cp cudf-${RAPIDS_CUDF_VERSION}.jar xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar xgboost4j_${RAPIDS_SPARK_VERSION}.jar gs://$GCS_BUCKET/
````

After executing these commands, use your web browser to navigate to Google Cloud Platform console and make sure your Google storage bucket “my-bucket” directory structure has the following files:
* gs://my-bucket/spark-gpu/rapids.sh
* gs://my-bucket/spark-gpu/internal/install-gpu-driver-debian.sh
* gs://my-bucket/spark-gpu/internal/install-gpu-driver-ubuntu.sh
* gs://my-bucket/cudf-${RAPIDS_CUDF_VERSION}.jar
* gs://my-bucket/xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar
* gs://my-bucket/xgboost4j_${RAPIDS_SPARK_VERSION}.jar
* gs://my-bucket/mortgage-small/eval/mortgage-small.csv
* gs://my-bucket/mortgage-small/eval/mortgage-small.csv
* gs://my-bucket/mortgage-small/trainWithEval/test.csv


### Step 2 Using the `gcloud` command to create a new cluster with this initialization action. 

The following command will create a new cluster named `<CLUSTER_NAME>`. Before the init script fully merged into 
`<dataproc-initialization-actions>` bucket, user need to copy the sparkgpu initialization script into a accessible GCS and into following structure. Ubuntu is recommended as CUDA support ubuntu, debian could be used by modifying `image-version` and `linux-dist` accordingly.

    ```bash
    /$GCS_BUCKET/spark-gpu/rapids.sh
    /$GCS_BUCKET/spark-gpu/internal/install-gpu-driver*.sh
    ```
    
    ```bash
    export CLUSTER_NAME=my-gpu-cluster
    export ZONE=us-central1-b
    export REGION=us-central1
    export GCS_BUCKET=my-bucket
    export INIT_ACTIONS_BUCKET=my-bucket
    export NUM_GPUS=2
    export NUM_WORKERS=2
    export RAPIDS_SPARK_VERSION='2.x-1.0.0-Beta3'
    export RAPIDS_CUDF_VERSION='0.9.2-cuda10'
     
    gcloud beta dataproc clusters create $CLUSTER_NAME  \
        --zone $ZONE \
        --region $REGION \
        --master-machine-type n1-standard-32 \
        --master-boot-disk-size 50 \
        --worker-accelerator type=nvidia-tesla-t4,count=$NUM_GPUS \
        --worker-machine-type n1-standard-32 \
        --worker-boot-disk-size 50 \
        --num-worker-local-ssds 1 \
        --num-workers $NUM_WORKERS \
        --image-version 1.4-ubuntu18 \
        --bucket $GCS_BUCKET \
        --metadata JUPYTER_PORT=8123,INIT_ACTIONS_REPO="gs://$INIT_ACTIONS_BUCKET",linux-dist="ubuntu",GCS_BUCKET="gs://$GCS_BUCKET" \
        --initialization-actions gs://$INIT_ACTIONS_BUCKET/spark-gpu/rapids.sh \
        --optional-components=ANACONDA,JUPYTER \
        --subnet=default \
        --properties "^#^spark:spark.dynamicAllocation.enabled=false#spark:spark.shuffle.service.enabled=false#spark:spark.submit.pyFiles=/usr/lib/spark/python/lib/xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar#spark:spark.jars=/usr/lib/spark/jars/xgboost4j-spark_${RAPIDS_SPARK_VERSION}.jar,/usr/lib/spark/jars/xgboost4j_${RAPIDS_SPARK_VERSION}.jar,/usr/lib/spark/jars/cudf-${RAPIDS_CUDF_VERSION}.jar" \
        --enable-component-gateway   
    ```

### Step 3, execute the sample app 

Once the cluster has been created, yarn resource manager could be accessed on port `8088` on the Dataproc master 
node. 

To connect to the dataproc web interface, you will need to create an SSH tunnel as
described in the
[dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces)
documentation. 

See
[the Mortgage example](https://github.com/rapidsai/spark-examples/tree/master/examples/apps/scala/src/main/scala/ai/rapids/spark/examples/mortgage)
that demonstrates end to end XGBoost4j in spark including data pre-processing and model
training with RAPIDS Spark GPU APIs. Additional examples
[are available](https://github.com/rapidsai/spark-examples/tree/master/examples). See the
[RAPIDS Spark GPU API documentation](https://github.com/rapidsai/spark-examples/tree/master/api-docs) for API details.

To submit such a job run:

 ```bash
    export MAIN_CLASS=ai.rapids.spark.examples.mortgage.GPUMain
    export RAPIDS_JARS=gs://$GCS_BUCKET/spark-gpu/sample_xgboost_apps-0.1.4-jar-with-dependencies.jar
    export DATA_PATH=$GCS_BUCKET
    export TREE_METHOD=gpu_hist
    export SPARK_NUM_EXECUTORS=4
    export CLUSTER_NAME=my-gpu-cluster
    export REGION=us-central1

    gcloud beta dataproc jobs submit spark \
        --cluster=$CLUSTER_NAME \
        --region=$REGION \
        --class=$MAIN_CLASS \
        --jars=$RAPIDS_JARS \
        --properties=spark.executor.cores=1,spark.executor.instances=${SPARK_NUM_EXECUTORS},spark.executor.memory=8G,spark.executorEnv.LD_LIBRARY_PATH=/usr/local/lib/x86_64-linux-gnu:/usr/local/cuda-10.0/lib64:${LD_LIBRARY_PATH} \
        -- \
        -format=csv \
        -numRound=100 \
        -numWorkers=${SPARK_NUM_EXECUTORS} \
        -treeMethod=${TREE_METHOD} \
        -trainDataPath=${DATA_PATH}/mortgage-small/train/mortgage_small.csv \
        -evalDataPath=${DATA_PATH}/mortgage-small/eval/mortgage_small.csv \
        -maxDepth=8  
 ```


RAPIDS Spark GPU is a relatively young project with APIs evolving quickly. If you
encounter unexpected errors or have feature requests, please file them at the
relevant [RAPIDS Spark example repo](https://github.com/rapidsai/spark-examples).

### Options

#### GPU Types & Driver Configuration

By default, these initialization actions install a CUDA 10.0 with NVIDIA 418 driver. If you wish
to install a different driver version, `metadata` need to be passed into initial action. Available options below:

```
    cuda-version='10-0'
    nccl-url='https://developer.nvidia.com/compute/machine-learning/nccl/secure/v2.4/prod/nccl-repo-ubuntu1804-2.4.8-ga-cuda10.0_1-1_amd64.deb'
    nccl-version='2.4.8'
```

#### Initialization Action Source

The RAPIDS Spark GPU initialization action steps are performed by [rapids.sh](rapids.sh)
which runs additional scripts downloaded from `rapids` directory in
[Dataproc Initialization Actions repo](https://pantheon.corp.google.com/storage/browser/dataproc-initialization-actions)
GCS bucket by default:

## Important notes

*   RAPIDS Spark GPU is supported on Pascal or newer GPU architectures (Tesla K80s will
    _not_ work with RAPIDS). See
    [list](https://cloud.google.com/compute/docs/gpus/) of available GPU types
    by GCP region.
*   You must set a GPU accelerator type for worker nodes, else
    the GPU driver install will fail and the cluster will report an error state.
*   When running RAPIDS Spark GPU with multiple attached GPUs, We recommend an
    n1-standard-32 worker machine type or better to ensure sufficient
    host-memory for buffering data to and from GPUs. When running with a single
    attached GPU, GCP only permits machine types up to 24 vCPUs.

