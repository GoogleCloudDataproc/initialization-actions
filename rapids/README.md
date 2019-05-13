# RAPIDS

This initialization action installs the latest release version of [RAPIDS](https://rapids.ai/) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. 

These initialization actions automate the process of setting up a Dask-cuDF cluster by running the following:

On the Dataproc master node:
- `dask-scheduler`, and `dask-cuda-worker`

On the Dataproc worker nodes:
- `dask-cuda-worker`

The master Cloud Dataproc node will run the dask-scheduler _and_ dask-cuda-workers. All Cloud Dataproc workers will run dask-cuda-workers.

Our initialization actions do the following:
1. [install nvidia GPU driver](install-gpu-driver.sh)
2. [install RAPIDS](install-rapids.sh) - [installs miniconda](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/conda), and [conda packages](conda-environment.yml)
3. [start dask-scheduler and dask-cuda-workers](start-dask.sh)

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with RAPIDS installed:

1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    DATAPROC_BUCKET=dataproc-initialization-actions

    gcloud beta dataproc clusters create <CLUSTER_NAME> \
    --zone us-east1-c \
    --master-accelerator type=nvidia-tesla-t4,count=4 \
    --master-machine-type n1-standard-32 \
    --worker-accelerator type=nvidia-tesla-t4,count=4 \
    --worker-machine-type n1-standard-32 \
    --initialization-actions gs://$DATAPROC_BUCKET/rapids/install-gpu-driver.sh,gs://$DATAPROC_BUCKET/rapids/install-rapids.sh,gs://$DATAPROC_BUCKET/rapids/start-dask.sh
    ```

1. Once the cluster has been created, the Dask scheduler listens for workers on port `8786`, and its status dashboard is on port `8787` on the Dataproc master node. These ports can be changed by modifying the [start-dask.sh](start-dask.sh) script.

To connect to the Dask web interface, you will need to create an SSH tunnel as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. You can also connect using the [Dask Client Python API](http://distributed.dask.org/en/latest/client.html) from a notebook, or a from a plain Python script or interpreter session.

An [example notebook](notebooks/NYCTaxi-E2E.ipynb) is provided that demonstrates end to end data pre-processing (cuDF & Dask) and model training (XGBoost) with RAPIDS APIs. Additional example notebooks [are available](https://github.com/rapidsai/notebooks). See the [RAPIDS documentation](https://docs.rapids.ai/) for API details.

RAPIDS is a relatively young project with APIs evolving quickly. If you encounter unexpected errors or have feature requests, please file them at the relevant [RAPIDS repo](https://github.com/rapidsai).

### Options

By default, the master node also runs dask-cuda-workers. This is useful for smaller scale jobs- processes run on 4 GPUs in a single node will usually be more performant than when run on the same number of GPUs in separate server nodes (due to higher communication costs).

If you want to save the master's GPU(s) for other purposes, this behavior is configurable via a metadata key using `--metadata`.

* `master-worker=false` - whether to run dask-cuda-workers on the master node

For example:
```bash
DATAPROC_BUCKET=dataproc-initialization-actions

gcloud beta dataproc clusters create <CLUSTER_NAME> \
--zone us-east1-c \
--master-accelerator type=nvidia-tesla-t4,count=4 \
--master-machine-type n1-standard-32 \
--worker-accelerator type=nvidia-tesla-t4,count=4 \
--worker-machine-type n1-standard-32 \
--metadata "master-worker=false" \
--bucket $DATAPROC_BUCKET \
--initialization-actions gs://$DATAPROC_BUCKET/rapids/install-gpu-driver.sh,gs://$DATAPROC_BUCKET/rapids/install-rapids.sh,gs://$DATAPROC_BUCKET/rapids/start-dask.sh
```

By default, these initialization actions assume you are using a Tesla T4 GPU. If you are using other GPU types or want to run a different driver version, [find the appropriate driver download URL](https://www.nvidia.com/Download/index.aspx?lang=en-us) for your driver's `.run` file.

* `gpu-driver-url=http://us.download.nvidia.com/tesla/410.104/NVIDIA-Linux-x86_64-410.104.run` - specify an alternate driver download URL

For example:

```bash
DATAPROC_BUCKET=dataproc-initialization-actions

gcloud beta dataproc clusters create <CLUSTER_NAME> \
--zone us-east1-c \
--master-accelerator type=nvidia-tesla-t4,count=4 \
--master-machine-type n1-standard-32 \
--worker-accelerator type=nvidia-tesla-t4,count=4 \
--worker-machine-type n1-standard-32 \
--metadata "gpu-driver-url=http://us.download.nvidia.com/tesla/410.104/NVIDIA-Linux-x86_64-410.104.run" \
--initialization-actions gs://$DATAPROC_BUCKET/rapids/install-gpu-driver.sh,gs://$DATAPROC_BUCKET/rapids/install-rapids.sh,gs://$DATAPROC_BUCKET/rapids/start-dask.sh
```

## Important notes
* RAPIDS is only supported on Pascal or newer GPU architectures.
* You must set a GPU accelerator type for both master and worker nodes, else the GPU driver install will fail and the cluster will report an error state
* We recommend an n1-standard-32 worker machine type or better to ensure sufficient host-memory for buffering data to and from GPUs.
* [conda-environment.yml](conda-environment.yml) should be updated based on which RAPIDS versions you wish to install
* Installing the GPU driver and conda packages takes about 10 minutes
* When deploying RAPIDS on few GPUs, ETL style processing with cuDF and Dask can run sequentially. When training ML models, you _must_ have enough total GPU memory in your cluster to hold the training set in memory.
* Dask's status dashboard is set to use HTTP port `8787` and is accessible from the master node
* High-Availability configuration is discouraged as [the dask-scheduler doesn't support it](https://github.com/dask/distributed/issues/1072).
* Dask scheduler and worker logs are written to /var/log/dask-scheduler.log and /var/log/dask-cuda-workers.log on the master and host respectively.
