# GPU driver installation

GPUs require special drivers and software which are not pre-installed on
[Dataproc](https://cloud.google.com/dataproc) clusters by default.
This initialization action installs GPU driver for NVIDIA GPUs on master and
worker nodes in a Dataproc cluster.

## Default versions

A reasonable default version will be selected for CUDA, the nvidia kernel
driver, cuDNN, and NCCL.  When using dataproc images 2.0 or later with the
default rapids-runtime, CUDA will default to `11.5`, and will otherwise default
to `11.2`.

CUDA | Full Version | Driver    | CuDNN     | NCCL    | Supported OSs
-----| ------------ | --------- | --------- | ------- | -------------------
10.1 | 10.1.243     | 418.88    | 7.6.4.38  | 2.4.8   | Ubuntu
10.2 | 10.2.89      | 440.64.00 | 7.6.5.32  | 2.5.6   | Ubuntu
11.0 | 11.0.3       | 450.51.06 | 8.0.4.30  | 2.7.8   | Ubuntu,Rocky
11.1 | 11.1.0       | 455.45.01 | 8.0.5.39  | 2.8.3   | Ubuntu,Debian
11.2 | 11.2.2       | 460.73.01 | 8.1.1.33  | 2.8.3   | Ubuntu,Debian,Rocky
11.5 | 11.5.2       | 495.29.05 | 8.3.0.98  | 2.11.4  | Ubuntu,Debian,Rocky
11.6 | 11.6.2       | 510.47.03 | 8.4.1.50  | 2.11.4  | Ubuntu,Debian,Rocky
11.7 | 11.7.1       | 515.65.01 | 8.5.0.96  | 2.12.12 | Ubuntu,Debian,Rocky
11.8 | 11.8.0       | 520.56.06 | 8.6.0.163 | 2.15.5  | Ubuntu,Debian,Rocky

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with GPU
support - it will install NVIDIA GPU drivers and CUDA on cluster nodes with
attached GPU adapters.

1.  Use the `gcloud` command to create a new cluster with NVIDIA-provided GPU
    drivers and CUDA installed by initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-accelerator type=nvidia-tesla-v100 \
        --worker-accelerator type=nvidia-tesla-v100,count=4 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh
    ```

1.  Use the `gcloud` command to create a new cluster with NVIDIA GPU drivers
    and CUDA installed by initialization action as well as the GPU
    monitoring service. The monitoring service is supported on Dataproc 2.0+ Debian
    and Ubuntu images. Please create a Github issue if support is needed for other
    Dataproc images.

    *Prerequisite:* Create GPU metrics in
    [Cloud Monitoring](https://cloud.google.com/monitoring/docs/) using Google
    Cloud Shell with the
    [create_gpu_metrics.py](https://github.com/GoogleCloudPlatform/ml-on-gcp/blob/master/dlvm/gcp-gpu-utilization-metrics/create_gpu_metrics.py)
    script.

    If you run this script locally you will need to set up a service account.

    ```bash
    export GOOGLE_CLOUD_PROJECT=<project-id>

    git clone https://github.com/GoogleCloudPlatform/ml-on-gcp.git
    cd ml-on-gcp/dlvm/gcp-gpu-utilization-metrics
    pip install -r ./requirements.txt
    python create_gpu_metrics.py
    ```

    Expected output:

    ```
    Created projects/project-sample/metricDescriptors/custom.googleapis.com/utilization_memory.
    Created projects/project-sample/metricDescriptors/custom.googleapis.com/utilization_gpu.
    Created projects/project-sample/metricDescriptors/custom.googleapis.com/memory_used
    ```

    Create cluster:

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-accelerator type=nvidia-tesla-v100 \
        --worker-accelerator type=nvidia-tesla-v100,count=4 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh \
        --metadata install-gpu-agent=true \
        --scopes https://www.googleapis.com/auth/monitoring.write
    ```

1.  Use the `gcloud` command to create a new cluster using Multi-Instance GPU (MIG) feature of the
    NVIDIA Ampere architecture. This creates a cluster with the NVIDIA GPU drivers
    and CUDA installed and the Ampere based GPU configured for MIG.

    After cluster creation each MIG instance will show up like a regular GPU to YARN. For instance, if you requested
    2 workers each with 1 A100 and used the default 2 MIG instances per A100, the cluster would have a total of 4 GPUs
    that can be allocated.

    It is important to note that CUDA 11 only supports enumeration of a single MIG instance. It is recommended that you
    only request a single MIG instance per container. For instance, if running Spark only request
    1 GPU per executor (spark.executor.resource.gpu.amount=1). Please see the
    [MIG user guide](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/) for more information.

    First decide which Amphere based GPU you are using. In the example we use the A100.
    Decide the number of MIG instances and [instance profiles to use](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/#lgi).
    By default if the MIG profiles are not specified it will configure 2 MIG instances with profile id 9. If
    a different instance profile is required, you can specify it in the MIG_CGI metadata parameter. Either a
    profile id or the name (ie 3g.20gb) can be specified. For example:

    ```bash
        --metadata=^:^MIG_CGI='3g.20gb,9'
    ```

    Create cluster with MIG enabled:

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --worker-machine-type a2-highgpu-1g
        --worker-accelerator type=nvidia-tesla-a100,count=1 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh \
        --metadata=startup-script-url=gs://goog-dataproc-initialization-actions-${REGION}/gpu/mig.sh
    ```

#### GPU Scheduling in YARN:

YARN is the default Resource Manager for Dataproc. To use GPU scheduling feature
in Spark, it requires YARN version >= 2.10 or >=3.1.1. If intended to use Spark
with Deep Learning use case, it recommended to use YARN >= 3.1.3 to get support
for [nvidia-docker version 2](https://github.com/NVIDIA/nvidia-docker).

In current Dataproc set up, we enable GPU resource isolation by initialization
script without NVIDIA Docker, you can find more information at
[NVIDIA Spark RAPIDS getting started guide](https://nvidia.github.io/spark-rapids/).

#### cuDNN

You can also install [cuDNN](https://developer.nvidia.com/CUDNN) on your
cluster. cuDNN is used as a backend for Deep Learning frameworks, such as
TensorFlow. A reasonable default will be selected.  To explicitly select a
version, include the metadata parameter `--metadata cudnn-version=x.x.x.x`. You
can find the list of archived versions
[here](https://developer.nvidia.com/rdp/cudnn-archive) which includes all
versions except the latest. To locate the version you need, click on Download
option for the correct cuDNN + CUDA version you desire, copy the link address
for the `cuDNN Runtime Library for Ubuntu18.04 x86_64 (Deb)` file of the
matching CUDA version and find the full version from the deb file. For instance,
for `libcudnn8_8.0.4.30-1+cuda11.0_amd64.deb`, the version is `8.0.4.30`. Below
is a table for mapping some recent major.minor cuDNN versions to full versions
and compatible CUDA versions:

Major.Minor | Full Version | CUDA Versions              | Release Date
----------- | ------------ | -------------------------- | ------------
8.6         | 8.6.0.163    | 10.2, 11.8                 | 2022-09-22
8.5         | 8.5.0.96     | 10.2, 11.7                 | 2022-08-04
8.4         | 8.4.1.50     | 10.2, 11.6                 | 2022-05-27
8.3         | 8.3.3.40     | 10.2, 11.5                 | 2022-03-18
8.2         | 8.2.4.15     | 10.2, 11.4                 | 2021-08-31
8.1         | 8.1.1.33     | 10.2, 11.2                 | 2021-02-25
8.0         | 8.0.5.39     | 10.1, 10.2, 11.0, 11.1     | 2020-11-01
7.6         | 7.6.5.32     | 9.0, 9.2, 10.0, 10.1, 10.2 | 2019-10-28
7.5         | 7.5.1.10     | 9.0, 9.2, 10.0, 10.1       | 2019-04-17

To figure out which version you need, refer to the framework's documentation,
sometimes found in the "building from source" sections.
[Here](https://www.tensorflow.org/install/source#gpu) is TensorFlow's.

#### NVIDIA Container Toolkit

If you have chosen to use the [DOCKER optional
component](https://cloud.google.com/dataproc/docs/concepts/components/docker),
`--optional-components=DOCKER`, and you have selected a sufficient CUDA version,
the gpu driver installation script will also install the [NVIDIA Container
Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/overview.html)
and configure your cluster to use nvidia-docker for [Launching Applications Using Docker Containers](https://hadoop.apache.org/docs/r3.0.2/hadoop-yarn/hadoop-yarn-site/DockerContainers.html).
You may specify the docker image on which to run your workload using the
`yarn-docker-image` metadata parameter.

#### Metadata parameters:

-   `install-gpu-agent: true|false` - this is an optional parameter with
    case-sensitive value. Default is `false`.

    **Note:** This parameter will collect GPU utilization and send statistics to
    Stackdriver. Make sure you add the correct scope to access Stackdriver.

-   `gpu-driver-url: <URL>` - this is an optional parameter for customizing
    NVIDIA-provided GPU driver on Debian.  Default is
    `https://download.nvidia.com/XFree86/Linux-x86_64/495.29.05/NVIDIA-Linux-x86_64-495.29.05.run`

-   `cuda-url: <URL>` - this is an optional parameter for customizing
    NVIDIA-provided CUDA on Debian. This is required if not using CUDA `10.1` or
    `10.2` with a Debian image. Please find the appropriate linux-based
    runtime-file URL [here](https://developer.nvidia.com/cuda-toolkit-archive).
    Default is
    `https://developer.download.nvidia.com/compute/cuda/11.5.2/local_installers/cuda_11.5.2_495.29.05_linux.run`

-   `rapids-runtime: SPARK|DASK|<RUNTIME>` - this is an optional parameter for
    customizing the rapids runtime. Default is `SPARK`.

-   `cuda-version: 10.1|10.2|<VERSION>` - this is an optional parameter for
    customizing NVIDIA-provided CUDA version. Default is `11.5`.

-   `nccl-version: 2.8.3|2.11.4|<VERSION>` - this is an optional parameter for
    customizing NVIDIA-provided NCCL version. Default is `2.11.4`.

-   `gpu-driver-version: 460.73.01|495.29.05|<VERSION>` - this is an optional
    parameter for customizing NVIDIA-provided kernel driver version. Default is
    `495.29.05`.

-   `cudnn-version: <VERSION>` - this is an optional parameter for installing
    [NVIDIA cuDNN](https://developer.nvidia.com/CUDNN) version `x.x.x.x`.
    Default is `8.3.3.40`.

-   `yarn-docker-image: nvidia/cuda:11.1.1-base-ubuntu20.04|<DOCKER IMAGE>` -
    this is an optional parameter to specify the docker image on which to run
    the nvidia container toolkit test.  Default is
    `nvidia/cuda:11.1.1-base-ubuntu20.04`
    
-   `yarn-container-runtime-type: docker|default|<CONTAINER RUNTIME TYPE>` -
    Determines whether an application will be launched in a Docker container. If
    the value is "docker", the application will be launched in a Docker
    container. Otherwise a regular process tree container will be used, and in
    order to execute spark jobs in docker containers, the
    `--properties=spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker` argument
    must be passed to your `gcloud dataproc jobs submit pyspark` command, or the
    property must otherwise be provided to the job.  Default is `default`

#### Verification

1.  Once the cluster has been created, you can access the Dataproc cluster and
    verify NVIDIA drivers are installed successfully.

    ```bash
    sudo nvidia-smi
    ```

2.  If you install the GPU collection service, verify installation by using the
    following command:

    ```bash
    sudo systemctl status gpu-utilization-agent.service
    ```

For more information about GPU support, take a look at
[Dataproc documentation](https://cloud.google.com/dataproc/docs/concepts/compute/gpus)

### Report metrics

The initialization action installs a
[monitoring agent](https://github.com/GoogleCloudPlatform/ml-on-gcp/tree/master/dlvm/gcp-gpu-utilization-metrics)
that monitors the GPU usage on the instance. This will auto create and send the
GPU metrics to the Cloud Monitoring service.

### Troubleshooting

Problem: Error when running `report_gpu_metrics`

```
google.api_core.exceptions.InvalidArgument: 400 One or more TimeSeries could not be written:
One or more points were written more frequently than the maximum sampling period configured for the metric.
:timeSeries[0]
```

Solution: Verify service is running in background

```bash
sudo systemctl status gpu-utilization-agent.service
```

## Important notes

*   This initialization script will install NVIDIA GPU drivers in all nodes in
    which a GPU is detected.
