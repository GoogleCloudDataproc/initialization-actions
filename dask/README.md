# Dask

[Dask](https://dask.org/) is an open source set of tools for parallelizing Python analytics tasks. It is developed in coordination with popular open source Python libraries including Numpy, Pandas and Scikit-Learn. 

This initialization action will create a Conda environment bundled with Dask and [Dask-Yarn](https://yarn.dask.org/), a tool for running Dask jobs on a YARN cluster such as [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. You can then take advantage of Dataproc's features such as scaling out jobs with [Autoscaling](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/autoscaling).

This initialization action can optionally install NVIDIA® [RAPIDS](https://rapids.ai/) to pair the CPU parallelism offerings of Dask with the GPU parallelism offerings of RAPIDS including cuDF, cuML and XGBoost. 

### Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

## Creating Dataproc Cluster with Dask and Dask-Yarn

*Note:* Using the [Anaconda](https://cloud.google.com/dataproc/docs/concepts/components/anaconda) component does not add these new libraries into the Dask environment. You must manually include the libraries you want via the metadata flag `CONDA_PACKAGES` flag. 

The following command will create a create a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster with [Dask](https://dask.org/) and [Dask-Yarn](https://yarn.dask.org/) installed.

```bash
CLUSTER_NAME=<cluster-name>
REGION=<region>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region ${REGION} \
  --master-machine-type n1-standard-16 \
  --worker-machine-type n1-highmem-32 \
  --image-version preview-ubuntu \
  --initialization-actions gs://bmiro-test/dataproc-initialization-actions/dask/dask.sh \
  --initialization-action-timeout=45m
```

You can create a cluster with the [Jupyter](https://cloud.google.com/dataproc/docs/concepts/components/jupyter) optional component and [component gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways) to use Dask and Dask-Yarn from a notebook environment:

```bash
CLUSTER_NAME=<cluster-name>
REGION=<region>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region ${REGION} \
  --master-machine-type n1-standard-16 \
  --worker-machine-type n1-highmem-32 \
  --image-version preview-ubuntu \
  --optional-components ANACONDA,JUPYTER \
  --initialization-actions gs://bmiro-test/dataproc-initialization-actions/dask/dask.sh \
  --initialization-action-timeout=45m \
  --enable-component-gateway
```

You can also configure your Dask environment to include NVIDIA® [RAPIDS](https://rapids.ai/), GPUs and GPU drivers configured for you:

```bash
CLUSTER_NAME=<cluster-name>
REGION=<region>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region us-central1 \
  --master-machine-type n1-standard-16 \
  --worker-machine-type n1-highmem-32 \
  --worker-accelerator type=nvidia-tesla-t4,count=2 \
  --image-version preview-ubuntu \
  --optional-components ANACONDA,JUPYTER \
  --initialization-actions gs://dataproc-initialization-actions/gpu/install_gpu_driver.sh,gs://bmiro-test/dataproc-initialization-actions/dask/dask.sh \
  --initialization-action-timeout=45m \
  --metadata include-rapids=true \
  --metadata gpu-driver-provider=NVIDIA \
  --enable-component-gateway 
```

## Dask example
You can interact with your Dask-Yarn cluster by creating a `YarnCluster` object. 

```python
from dask_yarn import YarnCluster
from dask.distributed import Client
import dask.array as da

import numpy as np

cluster = YarnCluster()
client = Client(cluster)

client.adapt() # Dynamically scale Dask resources

x = da.sum(np.ones(5))
x.compute()
```

## Interacting with the cluster

With the [Jupyter](https://cloud.google.com/dataproc/docs/concepts/components/jupyter) optional component, you can select the `dask` environment as your kernel.

You can also `ssh` into the cluster and execute Dask jobs from Python files. There's an alias on your cluster `dask-python` that points to the Python installation in your Dask environment. To run jobs, you can either `scp` a file onto your cluster or use `gsutil` on the cluster to download the Python file.

`gcloud compute ssh <cluster-name> --command="gsutil cp gs://path/to/file.py .; dask-python file.py`

#### Supported metadata parameters:

This initialization action supports the following `metadata` fields:

- `CONDA_PACKAGES=package1=version1 package2`: packages to install into the Conda Dask environment. 
- `CONDA_CHANNELS=channel1 channel2`: channels to install packages from. Ex.
- `dask-yarn-version=<VERSION>`: Dask-Yarn version to include in the environment. Default `0.8.1`.
- `include-rapids=true`: install NVIDIA® [RAPIDS](https://rapids.ai/) into the environment. Default `false`.
- `rapids-version=<VERSION>`: RAPIDS version to install into the environment. Default `0.14`.
- `cuda-version=<VERSION>`: CUDA version to install on the cluster. Default `10.2`.
- Any parameters provided by the [GPU initialization action](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/gpu).



*   If using the
    [Jupyter optional component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter),
    note that RAPIDS init-actions will install
    [nb_conda_kernels](https://github.com/Anaconda-Platform/nb_conda_kernels)
    and restart Jupyter so that the RAPIDS conda environment appears in Jupyter.
