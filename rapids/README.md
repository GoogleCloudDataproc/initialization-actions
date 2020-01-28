# RAPIDS

This initialization action installs the latest release version of
[RAPIDS](https://rapids.ai/) on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

This initialization action automates the process of setting up a Dask-cuDF
cluster:

-   creates `RAPIDS` conda environment and install RAPIDS conda packages.
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
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-accelerator type=nvidia-tesla-t4,count=4 \
        --master-machine-type n1-standard-32 \
        --worker-accelerator type=nvidia-tesla-t4,count=4 \
        --worker-machine-type n1-standard-32 \
        --optional-components ANACONDA \
        --initialization-actions \
            gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
        --metadata gpu-driver-provider=NVIDIA
    ```

1.  Once the cluster has been created, the Dask scheduler listens for workers on
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
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --master-accelerator type=nvidia-tesla-t4,count=4 \
    --master-machine-type n1-standard-32 \
    --worker-accelerator type=nvidia-tesla-t4,count=4 \
    --worker-machine-type n1-standard-32 \
    --optional-components ANACONDA \
    --initialization-actions \
        gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-${REGION}/rapids/rapids.sh \
    --metadata dask-cuda-worker-on-master=false \
    --metadata gpu-driver-provider=NVIDIA
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
*   `conda-environment.yml` in the initalization action can be updated based on
    which RAPIDS versions you wish to install
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
