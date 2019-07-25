# Dataproc - GPU driver installation

GPUs require special drivers and software. These items are not pre-installed on Cloud Dataproc clusters by default.
This [initialization action](https://cloud.google.com/dataproc/init-actions) installs GPU drivers for NVIDIA
on master and workers node in a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

**Note:** This feature is in Beta mode.

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with GPU support:
This initialization action will install GPU drivers 390.87 and CUDA 9.1. If you need a more recent
GPU driver please visit NVIDIA [site](https://www.nvidia.com/Download/index.aspx?lang=en-us).

1. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>` and install GPU drivers.
   
    ```bash
    gcloud beta dataproc clusters create <CLUSTER_NAME> \
      --master-accelerator type=nvidia-tesla-v100 \
      --worker-accelerator type=nvidia-tesla-v100,count=4 \
      --initialization-actions gs://$MY_BUCKET/gpu/install_gpu_driver.sh \
      --metadata install_gpu_agent=false
    ```

2. Use the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER_NAME>`, install GPU drivers and add the GPU monitoring service.
  
    ```bash
    gcloud beta dataproc clusters create <CLUSTER_NAME> \
      --master-accelerator type=nvidia-tesla-v100 \
      --worker-accelerator type=nvidia-tesla-v100,count=4 \
      --initialization-actions gs://$MY_BUCKET/gpu/install_gpu_driver.sh \
      --metadata install_gpu_agent=true \
      --scopes https://www.googleapis.com/auth/monitoring.write
    ```
    

#### Supported metadata parameters:
    
  - install_gpu_agent: true|false. This is mandatory value and is case sensitive.
     
  **Note:** This parameter will collect GPU utilization and send statistics to StackDriver. 
            Make sure you add the correct scope to access StackDriver.
    
#### Verification

1. Once the cluster has been created, you can access the Dataproc cluster and verify NVIDIA drivers are install successfully.

    ```bash
    sudo nvidia-smi
    ```
    
2. If you install the GPU collection service, verify installation by using the following command:
    ```bash
    sudo systemctl status gpu_utilization_agent.service
    ```
    
For more information about GPU support, take a look at [Dataproc documentation](https://cloud.google.com/dataproc/docs/concepts/compute/gpus)

### Report metrics

The initialization action installs a monitoring agent that monitors the GPU usage on the instance.
This will auto create the GPU metrics.

```bash
pip3 install -r ./requirements.txt
python3 report_gpu_metrics.py &
```

### Generate metrics

If you need to create metrics using create_metric_descriptor first run the following commands:

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
google.api_core.exceptions.InvalidArgument: 400 One or more TimeSeries could not be written: One or more points were written more frequently than the maximum sampling period configured for the metric.
: timeSeries[0]
```

Solution:
Verify service is running in background

```
sudo systemctl status gpu_utilization_agent.service
```

## Important notes

* This initialization script will install NVIDIA GPU drivers in all nodes in which a GPU is detected.
* This initialization script uses Debian Stretch packages to install CUDA 9.1 driver
* Tested with Dataproc 1.0, 1.1, 1.2 and 1.3-deb9.