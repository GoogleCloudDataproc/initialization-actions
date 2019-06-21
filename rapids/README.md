# RAPIDS

This initialization action installs the latest release version of
[RAPIDS](https://rapids.ai/) on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

These initialization actions automate the process of setting up a Dask-cuDF
cluster by running the following:

On the Dataproc master node:

-   `dask-scheduler`, and `dask-cuda-worker`

On the Dataproc worker nodes:

-   `dask-cuda-worker`

Our initialization action does the following:

1.  [install nvidia GPU driver](internal/install-gpu-driver.sh)
1.  [install RAPIDS](rapids.sh) -
    [installs miniconda](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/conda),
    and [conda packages](internal/conda-environment.yml)
1.  [start dask-scheduler and dask-cuda-workers](internal/launch-dask.sh)

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with
RAPIDS installed:

1.  Using the `gcloud` command to create a new cluster with this initialization
    action. The following command will create a new cluster named
    `<CLUSTER_NAME>`.

    ```bash
    DATAPROC_BUCKET=dataproc-initialization-actions

    gcloud beta dataproc clusters create <CLUSTER_NAME> \
        --master-accelerator type=nvidia-tesla-t4,count=4 \
        --master-machine-type n1-standard-32 \
        --worker-accelerator type=nvidia-tesla-t4,count=4 \
        --worker-machine-type n1-standard-32 \
        --initialization-actions gs://$DATAPROC_BUCKET/rapids/rapids.sh \
        --optional-components=ANACONDA
    ```

1.  Once the cluster has been created, the Dask scheduler listens for workers on
    port `8786`, and its status dashboard is on port `8787` on the Dataproc
    master node. These ports can be changed by modifying the
    [internal/launch-dask.sh](launch-dask.sh) script.

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

By default, these initialization actions install a CUDA 10.0 driver. If you wish
to install a different driver version,
[find the appropriate driver download URL](https://www.nvidia.com/Download/index.aspx?lang=en-us)
for your driver's `.run` file.

*   `--metadata=gpu-driver-url=http://us.download.nvidia.com/tesla/410.104/NVIDIA-Linux-x86_64-410.104.run` -
    to specify alternate driver download URL.

For example:

```bash
DATAPROC_BUCKET=dataproc-initialization-actions

gcloud beta dataproc clusters create <CLUSTER_NAME> \
    --master-accelerator type=nvidia-tesla-t4,count=4 \
    --master-machine-type n1-standard-32 \
    --worker-accelerator type=nvidia-tesla-t4,count=4 \
    --worker-machine-type n1-standard-32 \
    --metadata "gpu-driver-url=http://us.download.nvidia.com/tesla/410.104/NVIDIA-Linux-x86_64-410.104.run" \
    --initialization-actions gs://$DATAPROC_BUCKET/rapids/rapids.sh \
    --optional-components=ANACONDA
```

RAPIDS works with
[all "compute" GPU models](https://cloud.google.com/compute/docs/gpus/) except
for the Tesla K80. Currently, only CUDA 10.0 is supported for RAPIDS on
Dataproc.

#### Master As Worker Configuration

By default, the master node also runs dask-cuda-workers. This is useful for
smaller scale jobs- processes run on 4 GPUs in a single node will usually be
more performant than when run on the same number of GPUs in separate server
nodes (due to higher communication costs).

If you want to save the master's GPU(s) for other purposes, this behavior is
configurable via a metadata key using `--metadata`.

*   `run-cuda-worker-on-master=false` - whether to run dask-cuda-workers on the
    master node

For example:

```bash
DATAPROC_BUCKET=dataproc-initialization-actions

gcloud beta dataproc clusters create <CLUSTER_NAME> \
    --master-accelerator type=nvidia-tesla-t4,count=4 \
    --master-machine-type n1-standard-32 \
    --worker-accelerator type=nvidia-tesla-t4,count=4 \
    --worker-machine-type n1-standard-32 \
    --metadata "run-cuda-worker-on-master=false" \
    --initialization-actions gs://$DATAPROC_BUCKET/rapids/rapids.sh \
    --optional-components=ANACONDA
```

#### Initialization Action Source

The RAPIDS initialization action steps are performed by [rapids.sh](rapids.sh)
which runs additional scripts downloaded from `rapids` directory in
[Dataproc Initialization Actions repo](https://pantheon.corp.google.com/storage/browser/dataproc-initialization-actions)
GCS bucket by default:

*   `--metadata
    "INIT_ACTIONS_REPO=gs://my-forked-dataproc-initialization-actions"`

## Important notes

*   RAPIDS init actions depend on the
    [Anaconda](https://cloud.google.com/dataproc/docs/concepts/components/anaconda)
    component, which should be included at cluster creation time via the
    `--optional-components=ANACONDA` argument.
*   RAPIDS is supported on Pascal or newer GPU architectures (Tesla K80s will
    _not_ work with RAPIDS). See
    [list](https://cloud.google.com/compute/docs/gpus/) of available GPU types
    by GCP region.
*   You must set a GPU accelerator type for both master and worker nodes, else
    the GPU driver install will fail and the cluster will report an error state.
*   When running RAPIDS with multiple attached GPUs, We recommend an
    n1-standard-32 worker machine type or better to ensure sufficient
    host-memory for buffering data to and from GPUs. When running with a single
    attached GPU, GCP only permits machine types up to 24 vCPUs.
*   [conda-environment.yml](internal/conda-environment.yml) can be updated based
    on which RAPIDS versions you wish to install
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
