# Dataproc - GPU driver installation

GPUs require special drivers and software. These items are not pre-installed on
Cloud Dataproc clusters by default. This
[initialization action](https://cloud.google.com/dataproc/init-actions) installs
GPU drivers for NVIDIA on master and workers node in a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

**Note:** This feature is in Beta mode.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with GPU
support: this initialization action will install GPU drivers and CUDA. If you
need a more recent GPU driver please visit NVIDIA
[site](https://www.nvidia.com/Download/index.aspx?lang=en-us).

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-accelerator type=nvidia-tesla-v100 \
        --worker-accelerator type=nvidia-tesla-v100,count=4 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh \
        --metadata install_gpu_agent=false
    ```

2.  Use the `gcloud` command to create a new cluster with this initialization
    action. The following command will create a new cluster, install GPU drivers and add the GPU monitoring service.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --master-accelerator type=nvidia-tesla-v100 \
        --worker-accelerator type=nvidia-tesla-v100,count=4 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh \
        --metadata install_gpu_agent=true \
        --scopes https://www.googleapis.com/auth/monitoring.write
    ```

#### Supported metadata parameters:

-   `install_gpu_agent: true|false` - this is an optional parameter with case
    sensitive value.

    **Note:** This parameter will collect GPU utilization and send statistics to
    StackDriver. Make sure you add the correct scope to access StackDriver.

#### Verification

1.  Once the cluster has been created, you can access the Dataproc cluster and
    verify NVIDIA drivers are install successfully.

    ```bash
    sudo nvidia-smi
    ```

2.  If you install the GPU collection service, verify installation by using the
    following command:

    ```bash
    sudo systemctl status gpu_utilization_agent.service
    ```

For more information about GPU support, take a look at
[Dataproc documentation](https://cloud.google.com/dataproc/docs/concepts/compute/gpus)

### Report metrics

The initialization action installs a
[monitoring agent](https://github.com/GoogleCloudPlatform/ml-on-gcp/tree/master/dlvm/gcp-gpu-utilization-metrics)
that monitors the GPU usage on the instance. This will auto create the GPU
metrics.

```bash
pip3 install -r ./requirements.txt
python3 report_gpu_metrics.py &
```

### Generate metrics

If you need to create metrics using `create_metric_descriptor` first run the
following commands:

```bash
pip3 install -r ./requirements.txt
python3 create_gpu_metrics.py
```
Example:

```
Created projects/project-sample/metricDescriptors/custom.googleapis.com/gpu_utilization.
Created projects/project-sample/metricDescriptors/custom.googleapis.com/gpu_memory_utilization.
```

### Troubleshooting

Problem: Error when running report_gpu_metrics

```
google.api_core.exceptions.InvalidArgument: 400 One or more TimeSeries could not be written:
One or more points were written more frequently than the maximum sampling period configured for the metric.
:timeSeries[0]
```

Solution: Verify service is running in background

```
sudo systemctl status gpu_utilization_agent.service
```

## Important notes

*   This initialization script will install NVIDIA GPU drivers in all nodes in
    which a GPU is detected.
*   This initialization script uses Debian packages to install CUDA driver
*   Tested with Dataproc 1.2+.
