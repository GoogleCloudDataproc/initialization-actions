# GPU driver installation

GPUs require special drivers and software which are not pre-installed on
[Dataproc](https://cloud.google.com/dataproc) clusters by default.
This initialization action installs GPU driver for NVIDIA GPUs on master and
worker nodes in a Dataproc cluster.

## Default versions

A default version will be selected from the nvidia [support
matrix](https://docs.nvidia.com/deeplearning/frameworks/support-matrix/index.html)
for CUDA, the nvidia kernel driver, cuDNN, and NCCL.

Specifying a supported value for the `cuda-version` metadata variable
will select the following values for Driver, CuDNN and NCCL.  At the
time of writing, the default value for cuda-version, if unspecified is
12.4.  In addition to 12.4, we have also tested with 11.8, 12.0 and 12.6.

CUDA | Full Version | Driver    | CuDNN     | NCCL    | Tested Dataproc Image Versions
-----| ------------ | --------- | --------- | ------- | -------------------
11.8 | 11.8.0       | 560.35.03 | 8.6.0.163 | 2.15.5  | 2.0, 2.1, 2.2-ubuntu22
12.0 | 12.0.0       | 550.90.07 | 8.8.1.3,  | 2.16.5  | 2.0, 2.1, 2.2-rocky9, 2.2-ubuntu22
12.4 | 12.4.1       | 550.90.07 | 9.1.0.70  | 2.23.4  | 2.1-ubuntu20, 2.1-rocky8, 2.2
12.6 | 12.6.2       | 560.35.03 | 9.5.1.17  | 2.23.4  | 2.1-ubuntu20, 2.1-rocky8, 2.2

All variants in the preceeding table have been manually tested to work
with the installer.  Supported OSs at the time of writing are:

* Debian 10, 11 and 12
* Ubuntu 18.04, 20.04, and 22.04 LTS
* Rocky 8 and 9

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
        --master-accelerator type=nvidia-tesla-t4 \
        --worker-accelerator type=nvidia-tesla-t4,count=4 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh
    ```

1.  Use the `gcloud` command to create a new cluster with NVIDIA GPU drivers
    and CUDA installed by initialization action as well as the GPU
    monitoring service. The monitoring service is supported on Dataproc 2.0+ Debian
    and Ubuntu images.

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
        --master-accelerator type=nvidia-tesla-t4 \
        --worker-accelerator type=nvidia-tesla-t4,count=4 \
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
in Spark, it requires YARN version >= 2.10 or >= 3.1.1. If intended to use Spark
with Deep Learning use case, it recommended to use YARN >= 3.1.3 to get support
for [nvidia-container-toolkit](https://github.com/NVIDIA/nvidia-container-toolkit).

In current Dataproc set up, we enable GPU resource isolation by initialization
script with NVIDIA container toolkit.  You can find more information at
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

-   `private_secret_name: <STRING>` -
-   `public_secret_name: <STRING>` -
-   `secret_version: <INTEGER>` -
-   `secret_project: <STRING>` -
-   `cert_modulus_md5sum: <STRING>` -These arguments can be used to
    specify the driver signing parameters.  The certificate named by
    `public_secret_name` must be included in the boot sector of the
    disk from which the cluster is booted.  The key named by
    `private_secret_name` must correspond to the certificate named by
    `public_secret_name`, and the `cert_modulus_md5sum` must match the
    modulus md5sum of the files referenced by both the private and
    public secret names.

#### Loading built kernel module

For platforms which do not have pre-built binary kernel drivers, the
script will execute the .run file, installing the kernel driver
support libraries.

In addition to installing the support libraries, the open kernel
module is fetched from github and built locally.  There are metadata
attributes which can be used to specify the MOK key used to sign
kernel modules for use with secure boot.

-   `private_secret_name: <STRING>` -
-   `public_secret_name: <STRING>` -
-   `secret_version: <INTEGER>` -
-   `secret_project: <STRING>` -

Please see custom-images/examples/secure-boot/create-key-pair.sh for
details on what these attributes are and how they are used.

In order to load a kernel module built from source, either the
`--no-shielded-secure-boot` argument must be passed to `gcloud
dataproc clusters create`, or a trusted certificate must be included
in the cluster's base image using the custom-image script, and secret
names storing signing material must be supplied using metadata
arguments.  Attempts to build from source with misconfigured or
missing certificates will result in an error similar to the following:

```
ERROR: The kernel module failed to load. Secure boot is enabled on this system, so this is likely because it was not signed by a key that is trusted by the kernel. Please try installing the driver again, and sign the kernel module when prompted to do so.
ERROR: Unable to load the kernel module 'nvidia.ko'.  This happens most frequently when this kernel module was built against the wrong or improperly configured kernel sources, with a version of gcc that differs from the one used to build the target kernel, or if another driver, such as nouveau, is present and prevents the NVIDIA kernel module from obtaining ownership of the NVIDIA device(s), or no NVIDIA device installed in this system is supported by this NVIDIA Linux graphics driver release.
Please see the log entries 'Kernel module load error' and 'Kernel messages' at the end of the file '/var/log/nvidia-installer.log' for more information.
ERROR: Installation has failed.  Please see the file '/var/log/nvidia-installer.log' for details.  You may find suggestions on fixing installation problems in the README available on the Linux driver download page at www.nvidia.com.
```

The simple but unsecured resolution to this problem is to pass the
`--no-shielded-secure-boot` argument to `gcloud dataproc clusters
create` so that the unsigned kernel module built from source can be
loaded into the running kernel.

The complex but secure resolution is to run the
custom-images/examples/secure-boot/create-key-pair.sh so that the tls/
directory is populated with the certificates, and on first run, cloud
secrets are populated with the signing material.

The `custom-images/examples/secure-boot/create-key-pair.sh` script
emits bash code which can be evaluated in order to populate
appropriate environment variables.  You will need to run `gcloud
config set project ${PROJECT_ID}` before running `create-key-pair.sh`
to specify the project of the secret manager service.

```bash
$ bash custom-images/examples/secure-boot/create-key-pair.sh
modulus_md5sum=ffffffffffffffffffffffffffffffff
private_secret_name=efi-db-priv-key-042
public_secret_name=efi-db-pub-key-042
secret_project=your-project-id
secret_version=1
```


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
