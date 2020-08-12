# Dataproc - GPU driver installation

GPUs require special drivers and software. These items are not pre-installed on
Cloud Dataproc clusters by default. This
[initialization action](https://cloud.google.com/dataproc/init-actions) installs
GPU drivers for NVIDIA on master and workers node in a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

**Note:** This feature is in Beta mode.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with GPU
support - it will install NVIDIA GPU drivers and CUDA on cluster nodes with
attached GPU adapters.

This initialization action supports installation of OS-provided and
NVIDIA-provided GPU drivers and CUDA. By default, this initialization action
installs OS-provided GPU drivers and CUDA, to choose provider use `--metadata
gpu-driver-provider=<OS|NVIDIA>` metadata value.

1.  Use the `gcloud` command to create a new cluster with OS-provided GPU driver
    and CUDA installed by initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-accelerator type=nvidia-tesla-v100 \
        --worker-accelerator type=nvidia-tesla-v100,count=4 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh
    ```

1.  Use the `gcloud` command to create a new cluster with NVIDIA-provided GPU
    driver and CUDA installed by initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-accelerator type=nvidia-tesla-v100 \
        --worker-accelerator type=nvidia-tesla-v100,count=4 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh \
        --metadata gpu-driver-provider=NVIDIA
    ```

1.  Use the `gcloud` command to create a new cluster with OS-provided GPU driver
    and CUDA installed by initialization action. Additionally, it installs GPU
    monitoring service.

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

#### GPU Scheduling in YARN:

YARN is the default Resource Manager for Dataproc. To use GPU scheduling feature
in Spark, it requires YARN version >= 2.10 or >=3.1.1. If intended to use spark
with Deep Learning use case, it recommended to use Yarn >= 3.1.3 to get support
for [nvidia-docker version 2](https://github.com/NVIDIA/nvidia-docker).

In current Dataproc set up, we enable GPU resource isolation by initialization
script without NVIDIA Docker, you can find more information at
[NVIDIA Spark RAPIDS getting started guide](https://nvidia.github.io/spark-rapids/).

#### Supported metadata parameters:

-   `install-gpu-agent: true|false` - this is an optional parameter with
    case-sensitive value. Default is `false`.

    **Note:** This parameter will collect GPU utilization and send statistics to
    Stackdriver. Make sure you add the correct scope to access Stackdriver.

-   `gpu-driver-provider: OS|NVIDIA` - this is an optional parameter with
    case-sensitive value. Default is `OS`.

-   `gpu-driver-url: <URL>` - this is an optional parameter for customizing
    NVIDIA-provided GPU driver on Debian.

-   `cuda-url: <URL>` - this is an optional parameter for customizing
    NVIDIA-provided CUDA on Debian. This is required if not using CUDA 10.1
    or 10.2 with a Debian image. Please find the appropriate linux-based 
    runtime-file URL [here](https://developer.nvidia.com/cuda-toolkit-archive).

-   `cuda-version: 10.1|10.2|<VERSION>` - this is an optional parameter for
    customizing NVIDIA-provided CUDA version. Default is `10.2`. 

#### Verification

1.  Once the cluster has been created, you can access the Dataproc cluster and
    verify NVIDIA drivers are install successfully.

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
*   This initialization script can use OS-provided (Debian or Ubuntu) or
    NVIDIA-provided packages to install GPU drivers and CUDA.
