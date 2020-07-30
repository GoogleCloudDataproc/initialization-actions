# Machine Learning VM

This initialization action installs a set of packages designed to get you up and running with many commonly-used machine learning packages on a
[Dataproc](https://cloud.google.com/dataproc) cluster. Currently, this initialization action is supported on Dataproc [Ubuntu images] (https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-versions) 1.5 or higher. It is recommended to use our 2.0 image track for the best performance and greatest access to features. 

Features of this configuration include:

* Python packages such as  TensorFlow, PyTorch, MxNet, Scikit-learn and Keras
* R packages including XGBoost, Caret, randomForest, sparklyr
* Full list of Python and R packages below.
* Spark-BigQuery Connector
* RAPIDS on Spark and GPU support
* Jupyter and Anaconda via [Optional Components](https://cloud.google.com/dataproc/docs/concepts/components/overview)
* [Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with a set of preconfigured machine learning packages. You'll need to supply the following metadata flags:

```
--metadata spark-bigquery-connector-version=0.17.0
```
Feel free to use a different connector version. 

If you wish to have GPU and/or RAPIDS support, include the following.
```
--metadata gpu-driver-provider=NVIDIA
--metadata include-gpus=true
--metadata rapids-runtime=<SPARK|DASK>

# For RAPIDS on Dataproc 1.5
--metadata cuda-version=10.1
```

You can also include your own Python libraries with the metadata flag `PYTHON_PACKAGES`.
```
--metadata PYTHON_PACKAGES=package1==ver1,package2==ver2
```


1.  Use the `gcloud` command to create a new cluster with this initialization action. The command shown below includes a curated list of packages that will be installed on the cluster:

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-machine-type n1-standard-16 \
        --worker-machine-type n1-highmem-32 \
        --worker-accelerator type=nvidia-tesla-t4,count=2 \
        --image-version preview-ubuntu \
        --metadata spark-bigquery-connector-version=0.17.0 \
        --metadata gpu-driver-provider=NVIDIA \
        --metadata rapids-runtime=SPARK \
        --metadata include-gpus=true \
        --optional-components ANACONDA,JUPYTER \
        --initialization-actions gs://dataproc-initialization-actions/mlvm/mlvm.sh \
        --initialization-action-timeout=30m
        --enable-component-gateway  
    ```

You can use this initialization action with [Dataproc Hub](https://cloud.google.com/dataproc/docs/tutorials/dataproc-hub-admins) with the following YAML configuration:

```
config:
  endpointConfig:
    enableHttpPortAccess: true
  gceClusterConfig:
    metadata:
      gpu-driver-provider: NVIDIA
      include-gpus: 'true'
      rapids-runtime: SPARK
      spark-bigquery-connector-version: 0.17.1
  initializationActions:
  - executableFile: gs://bmiro-test/dataproc-initialization-actions/mlvm/mlvm.sh
    executionTimeout: 1800s
  masterConfig:
    machineTypeUri: n1-standard-16
  softwareConfig:
    imageVersion: 2.0.0-RC6-ubuntu18
    optionalComponents:
    - JUPYTER
    - ANACONDA
  workerConfig:
    accelerators:
    - acceleratorCount: 2
      acceleratorTypeUri: nvidia-tesla-t4
    machineTypeUri: n1-highmem-32
    numInstances: 2
```

## Full list of installed libraries:

NVIDIA GPU Drivers: 
*CUDA 10.2 (10.1 for Dataproc 1.x)
*NCCL 2.7.6
*RAPIDS 0.14.0
*Latest NVIDIA drivers for Ubuntu / Debian

### Python Libraries
```
google-cloud-bigquery==1.26.1
google-cloud-datalabeling==0.4.0
google-cloud-storage==1.30.0
google-cloud-bigtable==1.4.0
google-cloud-dataproc==1.0.1
google-api-python-client==1.10.0
matplotlib==3.3.0
mxnet==1.6.0 
nltk==3.5
numpy==1.18.4 
rpy2==3.3.3
scikit-learn==0.23.1 
spark-nlp==2.5.1
sparksql-magic==0.0.3
tensorflow==2.2.0
tensorflow-datasets==3.2.1
tensorflow-estimator==2.3.0
tensorflow-hub==0.8.0
tensorflow-io==0.14.0
tensorflow-probability==0.10.1
torch==1.5.1
torchvision: 0.6.1
xgboost: 1.1.0
```

Note: `tensorflow-gpu` is installed in place of `tensorflow` if `--metadata include-gpus=true` is provided.  

Additionally, the following is also included in Dataproc 2.0+
```
spark-tensorflow-distributor==0.1.0
```

### R Libraries
Conda: 
```
r-essentials=3.6.0
r-xgboost=0.90.0.2
r-sparklyr=1.0.0
```

### Java Libraries
```
spark-nlp - 2.5.4
spark-bigquery-connector - (version supplied at cluster creation time)
RAPIDS XGBOOST libraries - 0.14.0
```


You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).