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
initialization actions in production.

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

# For RAPIDS on Dataproc 1.5, include this as well.
--metadata cuda-version=10.1
```

Use the `gcloud` command to create a new cluster with this initialization
action. The command shown below includes a curated list of packages that will be
installed on the cluster:

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --master-machine-type n1-standard-16 \
    --worker-machine-type n1-highmem-32 \
    --worker-accelerator type=nvidia-tesla-t4,count=2 \
    --image-version preview-ubuntu \
    --metadata gpu-driver-provider=NVIDIA \
    --metadata rapids-runtime=SPARK \
    --metadata include-gpus=true \
    --optional-components ANACONDA,JUPYTER \
    --initialization-actions gs://dataproc-initialization-actions/mlvm/mlvm.sh \
    --initialization-action-timeout=45m
    --enable-component-gateway
```

You can use this initialization action with
[Dataproc Hub](https://cloud.google.com/dataproc/docs/tutorials/dataproc-hub-admins)
with the following YAML configuration:

```yaml
config:
  endpointConfig:
    enableHttpPortAccess: true
  gceClusterConfig:
    metadata:
      gpu-driver-provider: NVIDIA
      include-gpus: 'true'
      rapids-runtime: SPARK
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

*   CUDA 10.2 (10.1 for Dataproc 1.x)
*   NCCL 2.7.6
*   RAPIDS 0.14.0
*   Latest NVIDIA drivers for Ubuntu / Debian
*   spark-bigquery-connector 0.17.0

### Python Libraries

All libraries are installed with their latest versions unless noted otherwise.

#### Google Cloud Client Libraries (pip)

#### Tensorflow (pip)

```
tensorflow==2.3.0 (if no GPUs available)
tensorflow-gpu==2.3.0 (if GPUs available)
tensorflow-datasets==3.2.1
tensorflow-estimator==2.3.0
tensorflow-hub==0.8.0
tensorflow-io==0.15.0
tensorflow-probability==0.11.0
```

#### Other pip libraries

```
sparksql-magic==0.0.3
spark-tensorflow-distributor==0.1.0  # Dataproc 2.0+
xgboost==1.1.1
```

#### Conda libraries

```
matplotlib=3.2.2
mxnet=1.5.0
nltk=3.5.0
rpy2=2.9.4
scikit-learn=0.23.1
spark-nlp=2.5.5
pytorch=1.6.0
torchvision=0.7.0
```

For additional Python libraries you can use either of the following:

```
--metadata PIP_PACKAGES="package1==version1 package2"
--metadata CONDA_PACKAGES="package1=version1 package2"
```

### R libraries

Conda is used to install R libraries.

```
r-essentials=${R_VERSION}
r-xgboost=0.90.0.2
r-sparklyr=1.0.0
```

For additional R libraries you can use the following:

```
--metadata CONDA_PACKAGES="package1=version1 version2"
```

### Java Libraries

```
spark-nlp - 2.5.5
spark-bigquery-connector - 0.17.0
RAPIDS XGBOOST libraries - 0.14.0
```

You can find more information about using initialization actions with Dataproc
in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).
