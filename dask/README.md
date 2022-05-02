# Dask

[Dask](https://dask.org/) is an open source set of tools for parallelizing
Python analytics tasks. It is developed in coordination with popular open source
Python libraries including Numpy, Pandas and Scikit-Learn.

This initialization action will set up Dask on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster to run with
either `yarn` or `standalone`, both taking advantage of
[Dask Distributed](https://distributed.dask.org/en/latest/). This
initialization action is supported on **Dataproc image versions 2.0 and newer**.

In `yarn` mode, the cluster is configured with
[Dask-Yarn](https://yarn.dask.org). You can then take advantage of Dataproc's
features such as scaling out workloads with
[Autoscaling](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/autoscaling).

In `standalone` mode, Dataproc workers are treated as their own distributed
machines separate from YARN.

You can also add [RAPIDS](https://rapids.ai) and GPUs to your environment by
following the instructions in the
[RAPIDS initialization action](https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/rapids).
Note: RAPIDS with Dask is only supported on Dataproc image version 2.0+.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

### Creating Dataproc Cluster with Dask and Dask-Yarn

The following command will create a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster with
[Dask](https://dask.org/) and [Dask-Yarn](https://yarn.dask.org/) installed.

```bash
CLUSTER_NAME=<cluster-name>
REGION=<region>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region ${REGION} \
  --master-machine-type e2-standard-16 \
  --worker-machine-type e2-highmem-32 \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/dask/dask.sh \
  --initialization-action-timeout 20m
```

You can create a cluster with the
[Jupyter](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)
optional component and
[component gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)
to use Dask and Dask-Yarn from a notebook environment:

```bash
CLUSTER_NAME=<cluster-name>
REGION=<region>
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region ${REGION} \
  --master-machine-type e2-standard-16 \
  --worker-machine-type e2-highmem-32 \
  --optional-components JUPYTER \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/dask/dask.sh \
  --initialization-action-timeout 45m \
  --enable-component-gateway
```

### Dask examples

#### Dask standalone

```python
from dask.distributed import Client
import dask.array as da

import numpy as np

client = Client("localhost:8786")

x = da.sum(np.ones(5))
x.compute()
```

#### Dask-Yarn

```python
from dask_yarn import YarnCluster
from dask.distributed import Client
import dask.array as da

import numpy as np

cluster = YarnCluster()
client = Client(cluster)

cluster.adapt() # Dynamically scale Dask resources

x = da.sum(np.ones(5))
x.compute()
```

### Interacting with Data Services

Several libraries exist within the Dask ecosystem for interacting with various
data services. More information can be found
[here](https://docs.dask.org/en/latest/remote-data-services.html).

By default, Dataproc image version 2.0+ comes with `pyarrow`, `gcsfs`,
`fastparquet` and `fastavro` installed.

### Interacting with the cluster

With the
[Jupyter](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)
optional component, you can select the `dask` environment as your kernel.

You can also `ssh` into the cluster and execute Dask jobs from Python files. To
run jobs, you can either `scp` a file onto your cluster or use `gsutil` on the
cluster to download the Python file.

`gcloud compute ssh <cluster-name> --command="gsutil cp gs://path/to/file.py .;
python file.py`

### Accessing Web UIs

You can monitor your Dask applications using Web UIs, depending on which
runtime you are using.

For `standalone` mode, you can access the native Dask UI. Create an [SSH tunnel](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces#connecting_to_web_interfaces)
to access the Dask UI on port 8787.

For `yarn` mode, you can access the Skein Web UI via the YARN ResourceManager.
To access the YARN ResourceManager, create your cluster with [component gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)
enabled or create an [SSH tunnel](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces#connecting_to_web_interfaces). You can then access the Skein Web
UI by following these [instructions](https://jcristharif.com/skein/web-ui.html).

### Supported metadata parameters

This initialization action supports the following `metadata` fields:

*   `dask-runtime=yarn|standalone`: Dask runtime. Default is `yarn`.
*   `dask-worker-on-master`: Treat Dask master node as an additional worker.
    Default is `true`.
