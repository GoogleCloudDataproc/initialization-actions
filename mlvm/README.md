# Machine Learning VM

This initialization action installs a set of packages designed to get you up and
running with many commonly-used machine learning packages on a
[Dataproc](https://cloud.google.com/dataproc) cluster. Currently, this
initialization action is supported on Dataproc images 1.5 or higher. It is
recommended to use Dataproc 2.0 image track for the best performance and
greatest access to features.

Features of this configuration include:

*   Python packages such as TensorFlow, PyTorch, MxNet, Scikit-learn and Keras
*   R packages including XGBoost, Caret, randomForest, sparklyr
*   Full list of Python and R packages below.
*   Spark-BigQuery Connector
*   RAPIDS on Spark and GPU support
*   Jupyter and Anaconda via
    [Optional Components](https://cloud.google.com/dataproc/docs/concepts/components/overview)
*   [Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production. As this initialization action relies
on using other initialization actions, it is encouraged that you copy this
repo to a GCS bucket. This initialization action allows you to provide the
location of your initialization actions repo via the metadata flag
`--metadata init-actions-repo=${YOUR_REPO}`.

You can use this initialization action to create a new Dataproc cluster with a
set of preconfigured machine learning packages.

If you wish to have GPU support, please include the following.

```
--metadata include-gpus=true
--metadata gpu-driver-provider=NVIDIA
```

If you wish to have RAPIDS support, you must include the following in addition
to the above GPU metadata:

```
--metadata rapids-runtime=<SPARK|DASK>
```

Use the `gcloud` command to create a new cluster with this initialization
action. The command shown below includes a curated list of packages that will be
installed on the cluster:

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
INIT_ACTIONS_REPO=gs://<your_bucket_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --master-machine-type n1-standard-16 \
    --worker-machine-type n1-highmem-32 \
    --master-accelerator type=nvidia-tesla-t4,count=2 \
    --worker-accelerator type=nvidia-tesla-t4,count=2 \
    --image-version 2.0-ubuntu18 \
    --metadata gpu-driver-provider=NVIDIA \
    --metadata rapids-runtime=SPARK \
    --metadata include-gpus=true \
    --metadata init-actions-repo=${INIT_ACTIONS_REPO} \
    --metadata cuda-version=11.0 \
    --metadata cudnn-version=8.0.5.39 \
    --optional-components JUPYTER \
    --initialization-actions ${INIT_ACTIONS_REPO}/mlvm/mlvm.sh \
    --initialization-action-timeout=45m \
    --enable-component-gateway
```

You can make a Dataproc 1.5 cluster with the following command:
```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
INIT_ACTIONS_REPO=gs://<your_bucket_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --master-machine-type n1-standard-16 \
    --worker-machine-type n1-highmem-32 \
    --master-accelerator type=nvidia-tesla-t4,count=2 \
    --worker-accelerator type=nvidia-tesla-t4,count=2 \
    --image-version 1.5-ubuntu18 \
    --metadata gpu-driver-provider=NVIDIA \
    --metadata rapids-runtime=SPARK \
    --metadata include-gpus=true \
    --metadata init-actions-repo=${INIT_ACTIONS_REPO} \
    --metadata cuda-version=10.1 \
    --metadata cudnn-version=7.6.5.32 \
    --optional-components ANACONDA,JUPYTER \
    --initialization-actions ${INIT_ACTIONS_REPO}/mlvm/mlvm.sh \
    --initialization-action-timeout=45m \
    --enable-component-gateway
```

You can use this initialization action with
[Dataproc Hub](https://cloud.google.com/dataproc/docs/tutorials/dataproc-hub-admins)
with the following YAML configuration, replacing `<INIT_ACTIONS_REPO>` with
your ${INIT_ACTIONS_REPO} bucket:

```yaml
config:
  endpointConfig:
    enableHttpPortAccess: true
  gceClusterConfig:
    metadata:
      gpu-driver-provider: NVIDIA
      rapids-runtime: SPARK
      include-gpus: 'true'
      init-actions-repo: <INIT_ACTIONS_REPO>
      cuda-version: 11.0
      cudnn-version: 8.0.5.39
  initializationActions:
  - executableFile: gs://<INIT_ACTIONS_REPO>/mlvm/mlvm.sh
    executionTimeout: 2700s
  masterConfig:
    accelerators:
    - acceleratorCount: 2
      acceleratorTypeUri: nvidia-tesla-t4
    machineTypeUri: n1-standard-16
  softwareConfig:
    imageVersion: 2.0-ubuntu18
    optionalComponents:
    - JUPYTER
  workerConfig:
    accelerators:
    - acceleratorCount: 2
      acceleratorTypeUri: nvidia-tesla-t4
    machineTypeUri: n1-highmem-32
    numInstances: 2
```

## Full list of installed libraries:

NVIDIA GPU Drivers:

*   CUDA 11.0 # 10.1 on Dataproc 1.5
*   cuDNN 8.0.5.39 # 7.6.5.32 on Dataproc 1.5
*   NCCL 2.7.8
*   RAPIDS 0.17.0
*   Latest NVIDIA drivers for Ubuntu / Debian
*   spark-bigquery-connector 0.18.1

### Python Libraries

All libraries are installed with their latest versions unless noted otherwise.

#### Google Cloud Client Libraries (pip)

#### Tensorflow (pip)

```
tensorflow==2.4 # 2.3 on Dataproc 1.5
tensorflow-datasets==4.2
tensorflow-estimator==2.4 # 2.3 on Dataproc 1.5
tensorflow-hub==0.11
tensorflow-io==0.17 # 0.16 on Dataproc 1.5
tensorflow-probability==0.12
```

#### Other pip libraries

```
mxnet=1.6
rpy2=3.4
sparksql-magic==0.0.3
spark-tensorflow-distributor==0.1.0  # Not installed on Dataproc 1.5
```

#### Conda libraries

```
dask-yarn=0.8
scikit-learn=0.24
spark-nlp=2.7.2
pytorch=1.7
torchvision=0.8
xgboost==1.3
```

For additional Python libraries you can use either of the following:

```
--metadata PIP_PACKAGES="package1==version1 package2"
--metadata CONDA_PACKAGES="package1=version1 package2"
```

### R libraries

Conda is used to install R libraries. R version is 3.6 for Dataproc 1.5, and
4.0 for Dataproc 2.0+

```
r-essentials=${R_VERSION}
r-xgboost=1.3
r-sparklyr=1.5
```
Note: due to a version conflict, `r-xgboost` is not installed if
`--metadata rapids-runtime` is provided.

For additional R libraries you can use the following:

```
--metadata CONDA_PACKAGES="package1=version1 version2"
```

### Java Libraries

```
spark-nlp - 2.7.2
spark-bigquery-connector - 0.18.1
RAPIDS XGBOOST libraries - 0.17.0
```

You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).
