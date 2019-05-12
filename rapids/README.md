# RAPIDS

This initialization action installs the latest release version of [RAPIDS](https://rapids.ai/) on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The master Cloud Dataproc node will be the dask-scheduler and all Cloud Dataproc workers will be dask-cuda-workers.

Our initialization actions do the following:
1. [install nvidia GPU driver](install-gpu-driver.sh)
2. [install RAPIDS](install-rapids.sh) - [installs miniconda](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/conda), and [conda packages](conda-environment.yml)
3. [start dask-scheduler and dask-cuda-workers](start-dask.sh)

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with RAPIDS installed:

1. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud beta dataproc clusters create <CLUSTER_NAME> \
    --master-accelerator type=nvidia-tesla-t4 \
    --worker-accelerator type=nvidia-tesla-t4,count=4 \
    --worker-machine-type n1-standard-32 \
    --metadata "gpu-driver-url=http://us.download.nvidia.com/tesla/410.104/NVIDIA-Linux-x86_64-410.104.run,gpu-driver=NVIDIA-Linux-x86_64-410.104.run" \
    --initialization-actions gs://dataproc-initialization-actions/rapids/install-gpu-driver.sh,gs://dataproc-initialization-actions/rapids/install-rapids.sh,gs://dataproc-initialization-actions/rapids/start-dask.sh
    ```

1. Once the cluster has been created, the Dask scheduler listens for workers on port `8786`, and its status dashboard is on port `8787` (though you can change these in the script) on the master node in a Cloud Dataproc cluster.

To connect to the Dask web interface, you will need to create an SSH tunnel as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. You can also connect using the [Dask Client Python API](http://distributed.dask.org/en/latest/client.html) from a notebook, or a from a plain Python script or interpreter session.

An [example notebook](notebooks/NYCTaxi-E2E.ipynb) is provided that demonstrates end to end data pre-processing (ETL) and model training (XGBoost) with RAPIDS APIs. Additional example notebooks [are available](https://github.com/rapidsai/notebooks). See the [RAPIDS documentation](https://docs.rapids.ai/) for API details.

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes
* [conda-environment.yml](conda-environment.yml) must be updated based on which RAPIDS version you wish to install
* RAPIDS is only supported on Pascal or newer GPU architectures.
* Installing the recent GPU driver and conda packages takes about 10 minutes
* We recommend an n1-standard-32 worker machine type or better for enough host-memory to buffer data to and from GPUs
* When deploying RAPIDS on few GPUs, data processing will run sequentially. When training ML models, you _must_ have enough total GPU memory in your cluster to hold the training set in memory.
* Dask's status dashboard is set to use HTTP port `8787` by default
* High-Availability configuration is discouraged as [the dask-scheduler doesn't support it](https://github.com/dask/distributed/issues/1072).
* Dask scheduler and worker logs are written to /var/log/dask-scheduler.log and /var/log/dask-cuda-workers.log on the master and host respectively.
