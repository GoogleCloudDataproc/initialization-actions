# GPU driver installation

GPUs require special drivers and software which are not pre-installed on
[Dataproc](https://cloud.google.com/dataproc) clusters by default.
This initialization action installs GPU driver for NVIDIA GPUs on master and
worker nodes in a Dataproc cluster.

## Default versions

A default version will be selected from NVIDIA's guidance, similar to the
[NVIDIA Deep Learning Frameworks Support Matrix](https://docs.nvidia.com/deeplearning/frameworks/support-matrix/index.html),
for CUDA, the NVIDIA kernel driver, cuDNN, and NCCL.

Specifying a supported value for the `cuda-version` metadata variable
will select compatible values for Driver, cuDNN, and NCCL from the script's
internal matrix. Default CUDA versions are typically:

  * Dataproc 2.0: `12.1.1`
  * Dataproc 2.1: `12.4.1`
  * Dataproc 2.2 & 2.3: `12.6.3`

*(Note: The script supports a wider range of specific versions.
Refer to internal arrays in `install_gpu_driver.sh` for the full matrix.)*

**Example Tested Configurations (Illustrative):**

CUDA | Full Version | Driver    | cuDNN     | NCCL   | Tested Dataproc Image Versions
-----| ------------ | --------- | --------- | -------| ---------------------------
11.8 | 11.8.0       | 525.147.05| 9.5.1.17  | 2.21.5 | 2.0, 2.1 (Debian/Ubuntu/Rocky); 2.2 (Ubuntu 22.04)
12.0 | 12.0.1       | 525.147.05| 8.8.1.3   | 2.16.5 | 2.0, 2.1 (Debian/Ubuntu/Rocky); 2.2 (Rocky 9, Ubuntu 22.04)
12.4 | 12.4.1       | 550.135   | 9.1.0.70  | 2.23.4 | 2.1 (Ubuntu 20.04, Rocky 8); Dataproc 2.2+
12.6 | 12.6.3       | 550.142   | 9.6.0.74  | 2.23.4 | 2.1 (Ubuntu 20.04, Rocky 8); Dataproc 2.2+

**Supported Operating Systems:**

  * Debian 10, 11, 12
  * Ubuntu 18.04, 20.04, 22.04 LTS
  * Rocky Linux 8, 9

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used)
of using initialization actions in production.

This initialization action will install NVIDIA GPU drivers and the CUDA toolkit.
Optional components like cuDNN, NCCL, and PyTorch can be included via
metadata.

1.  Use the `gcloud` command to create a new cluster with this initialization
    action. The following command will create a new cluster named
    `<CLUSTER_NAME>` and install default GPU drivers (GPU agent is enabled
    by default).

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    DATAPROC_IMAGE_VERSION=<image_version> # e.g., 2.2-debian12

    gcloud dataproc clusters create ${CLUSTER_NAME} \
      --region ${REGION} \
      --image-version ${DATAPROC_IMAGE_VERSION} \
      --master-accelerator type=nvidia-tesla-t4,count=1 \
      --worker-accelerator type=nvidia-tesla-t4,count=2 \
      --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh \
      --scopes https://www.googleapis.com/auth/monitoring.write # For GPU agent
    ```

2.  Use the `gcloud` command to create a new cluster specifying a custom CUDA
    version and providing direct HTTP/HTTPS URLs for the driver and CUDA
    `.run` files. This example also disables the GPU agent.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    DATAPROC_IMAGE_VERSION=<image_version> # e.g., 2.2-ubuntu22
    MY_DRIVER_URL="https://us.download.nvidia.com/XFree86/Linux-x86_64/550.90.07/NVIDIA-Linux-x86_64-550.90.07.run"
    MY_CUDA_URL="https://developer.download.nvidia.com/compute/cuda/12.4.1/local_installers/cuda_12.4.1_550.54.15_linux.run"

    gcloud dataproc clusters create ${CLUSTER_NAME} \
      --region ${REGION} \
      --image-version ${DATAPROC_IMAGE_VERSION} \
      --master-accelerator type=nvidia-tesla-t4,count=1 \
      --worker-accelerator type=nvidia-tesla-t4,count=2 \
      --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh \
      --metadata gpu-driver-url=${MY_DRIVER_URL},cuda-url=${MY_CUDA_URL},install-gpu-agent=false
    ```

3.  To create a cluster with Multi-Instance GPU (MIG) enabled (e.g., for
    NVIDIA A100 GPUs), you must use this `install_gpu_driver.sh` script
    for the base driver installation, and additionally specify `gpu/mig.sh`
    as a startup script.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    DATAPROC_IMAGE_VERSION=<image_version> # e.g., 2.2-rocky9

    gcloud dataproc clusters create ${CLUSTER_NAME} \
      --region ${REGION} \
      --image-version ${DATAPROC_IMAGE_VERSION} \
      --worker-machine-type a2-highgpu-1g \
      --worker-accelerator type=nvidia-tesla-a100,count=1 \
      --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/gpu/install_gpu_driver.sh \
      --properties "dataproc:startup.script.uri=gs://goog-dataproc-initialization-actions-${REGION}/gpu/mig.sh" \
      --metadata MIG_CGI='1g.5gb,1g.5gb,1g.5gb,1g.5gb,1g.5gb,1g.5gb,1g.5gb' # Example MIG profiles
    ```

### Using for Custom Image Creation

When this `install_gpu_driver.sh` script is used as a `customization-script`
for building custom Dataproc images (e.g., with tools from the
`GoogleCloudDataproc/custom-images` repository like `generate_custom_image.py`),
some configurations need to be deferred.

  * The image building tool should pass the metadata
    `--metadata invocation-type=custom-images` to the temporary instance
    used during image creation.
  * This instructs `install_gpu_driver.sh` to install drivers and tools
    but defer Hadoop/Spark-specific configurations to the first boot of an
    instance created from this custom image. This is handled via a systemd
    service (`dataproc-gpu-config.service`).
  * End-users creating clusters *from* such a custom image do **not** set
    the `invocation-type` metadata.

Example command for `generate_custom_image.py` (simplified):

```bash
python generate_custom_image.py \
    # ... other generate_custom_image.py arguments ...
    --customization-script gs://<your-bucket>/gpu/install_gpu_driver.sh \
    --metadata invocation-type=custom-images,cuda-version=12.6 # Plus other desired metadata
```

### GPU Scheduling in YARN:

This script configures YARN, Dataproc's default Resource Manager, for GPU
awareness.

  * It sets `yarn.io/gpu` as a resource type.
  * It configures the `LinuxContainerExecutor` and cgroups for GPU isolation.
  * It installs a GPU discovery script (`getGpusResources.sh`) for Spark, which
    caches results to minimize `nvidia-smi` calls.
  * Spark default configurations in `/etc/spark/conf/spark-defaults.conf`
    are updated with GPU-related properties (e.g.,
    `spark.executor.resource.gpu.amount`) and the RAPIDS Spark plugin
    (`com.nvidia.spark.SQLPlugin`) is commonly configured.

### cuDNN

This script can install [NVIDIA cuDNN](https://developer.nvidia.com/CUDNN),
a GPU-accelerated library for deep neural networks.

  * If `include-pytorch=yes` is specified or `cudnn-version` is provided,
    a compatible version of cuDNN will be selected and installed based on the
    determined CUDA version.
  * To install a specific version of cuDNN, use the `cudnn-version` metadata
    parameter (e.g., `--metadata cudnn-version=8.9.7.29`). Please consult the
    [cuDNN Archive](https://developer.nvidia.com/rdp/cudnn-archive) and your
    deep learning framework's documentation for CUDA compatibility. The script
    may use `libcudnn` packages or tarball installations.

**Example cuDNN Version Mapping (Illustrative):**

| cuDNN Major.Minor | Example Full Version | Compatible CUDA Versions (General) |
|-------------------|----------------------|------------------------------------|
| 8.6               | 8.6.0.163            | 10.2, 11.x                         |
| 8.9               | 8.9.7.29             | 11.x, 12.x                         |
| 9.x               | e.g., 9.6.0.74       | 12.x                               |

### Metadata Parameters:

This script accepts the following metadata parameters:

  * `install-gpu-agent`: `true`|`false`. **Default: `true`**.
    Installs GPU monitoring agent. Requires the
    `https://www.googleapis.com/auth/monitoring.write` scope.
  * `cuda-version`: (Optional) Specify desired CUDA version (e.g., `11.8`,
    `12.4.1`). Overrides default CUDA selection.
  * `cuda-url`: (Optional) HTTP/HTTPS URL to a specific CUDA toolkit `.run` file
    (e.g., `https://developer.download.nvidia.com/.../cuda_12.4.1_..._linux.run`).
    Fetched using `curl`. Overrides `cuda-version` and default selection.
  * `gpu-driver-version`: (Optional) Specify NVIDIA driver version (e.g.,
    `550.90.07`). Overrides default compatible driver selection.
  * `gpu-driver-url`: (Optional) HTTP/HTTPS URL to a specific NVIDIA driver
    `.run` file (e.g., `https://us.download.nvidia.com/.../NVIDIA-Linux-x86_64-...run`).
    Fetched using `curl`. Overrides `gpu-driver-version`.
  * `gpu-driver-provider`: (Optional) `OS`|`NVIDIA`. Default: `NVIDIA`.
    Determines preference for OS-provided vs. NVIDIA-direct drivers.
    The script often prioritizes `.run` files or source builds for reliability.
  * `cudnn-version`: (Optional) Specify cuDNN version (e.g., `8.9.7.29`).
  * `nccl-version`: (Optional) Specify NCCL version.
  * `include-pytorch`: (Optional) `yes`|`no`. Default: `no`.
    If `yes`, installs PyTorch, TensorFlow, RAPIDS, and PySpark in a Conda
    environment.
  * `gpu-conda-env`: (Optional) Name for the PyTorch Conda environment.
    Default: `dpgce`.
  * `container-runtime`: (Optional) E.g., `docker`, `containerd`, `crio`.
    For NVIDIA Container Toolkit configuration. Auto-detected if not specified.
  * `http-proxy`: (Optional) URL of an HTTP proxy for downloads.
  * `http-proxy-pem-uri`: (Optional) A `gs://` path to the
    PEM-encoded certificate file used by the proxy specified in
    `http-proxy`. This is needed if the proxy uses TLS and its
    certificate is not already trusted by the cluster's default trust
    store (e.g., if it's a self-signed certificate or signed by an
    internal CA). The script will install this certificate into the
    system and Java trust stores.
  * `invocation-type`: (For Custom Images) Set to `custom-images` by image
    building tools. Not typically set by end-users creating clusters.
  * **Secure Boot Signing Parameters:** Used if Secure Boot is enabled and
    you need to sign kernel modules built from source.
    ```text
    private_secret_name=<your-private-key-secret-name>
    public_secret_name=<your-public-cert-secret-name>
    secret_project=<your-gcp-project-id>
    secret_version=<your-secret-version>
    modulus_md5sum=<md5sum-of-your-mok-key-modulus>
    ```

### Loading Built Kernel Module & Secure Boot

When the script needs to build NVIDIA kernel modules from source (e.g., using
NVIDIA's open-gpu-kernel-modules repository, or if pre-built OS packages are
not suitable), special considerations apply if Secure Boot is enabled.

  * **Secure Boot Active:** Locally compiled modules must be signed with a key
    trusted by the system's UEFI firmware.
      * **MOK Key Signing:** Provide the Secure Boot signing metadata parameters
        (listed above) to use keys stored in GCP Secret Manager. The public MOK
        certificate must be enrolled in your base image's UEFI keystore. See
        `GoogleCloudDataproc/custom-images/examples/secure-boot/create-key-pair.sh`
        for guidance on key creation and management.
      * **Disabling Secure Boot (Unsecured Workaround):** You can pass the
        `--no-shielded-secure-boot` flag to `gcloud dataproc clusters create`.
        This allows unsigned modules but disables Secure Boot's protections.
  * **Error Indication:** If a kernel module fails to load due to signature
    issues while Secure Boot is active, check `/var/log/nvidia-installer.log`
    or `dmesg` output for errors like "Operation not permitted" or messages
    related to signature verification failure.

### Verification

1.  Once the cluster has been created, you can access the Dataproc cluster and
    verify NVIDIA drivers are installed successfully.

    ```bash
    sudo nvidia-smi
    ```

2.  If the CUDA toolkit was installed, verify the compiler:

    ```bash
    /usr/local/cuda/bin/nvcc --version
    ```

3.  If you install the GPU collection service (`install-gpu-agent=true`, default),
    verify installation by using the following command:

    ```bash
    sudo systemctl status gpu-utilization-agent.service
    ```

    (The service should be `active (running)`).

For more information about GPU support, take a look at
[Dataproc documentation](https://cloud.google.com/dataproc/docs/concepts/compute/gpus).

### Report Metrics

The GPU monitoring agent (installed when `install-gpu-agent=true`) automatically
collects and sends GPU utilization and memory usage metrics to Cloud Monitoring.
The agent is based on code from the
[ml-on-gcp/gcp-gpu-utilization-metrics](https://www.google.com/search?q=https://github.com/GoogleCloudPlatform/ml-on-gcp/tree/master/dlvm/gcp-gpu-utilization-metrics)
repository. The `create_gpu_metrics.py` script mentioned in older
documentation is no longer used by this initialization action, as the agent
handles metric creation and reporting.

### Troubleshooting

  * **Installation Failures:** Examine the initialization action log on the
    affected node, typically `/var/log/dataproc-initialization-script-0.log`
    (or a similar name if multiple init actions are used).
  * **GPU Agent Issues:** If the agent was installed (`install-gpu-agent=true`),
    check its service logs using `sudo journalctl -u gpu-utilization-agent.service`.
  * **Driver Load or Secure Boot Problems:** Review `dmesg` output and
    `/var/log/nvidia-installer.log` for errors related to module loading or
    signature verification.
  * **"Points written too frequently" (GPU Agent):** This was a known issue with
    older versions of the `report_gpu_metrics.py` service. The current script
    and agent versions aim to mitigate this. If encountered, check agent logs.

## Important notes

  * This initialization script will install NVIDIA GPU drivers in all nodes in
    which a GPU is detected. If no GPUs are present on a node, most
    GPU-specific installation steps are skipped.
  * **Performance & Caching:**
      * The script extensively caches downloaded artifacts (drivers, CUDA `.run`
        files) and compiled components (kernel modules, NCCL, Conda environments)
        to a GCS bucket. This bucket is typically specified by the
        `dataproc-temp-bucket` cluster property or metadata.
      * **First Run / Cache Warming:** Initial runs on new configurations (OS,
        kernel, or driver version combinations) that require source compilation
        (e.g., for NCCL or kernel modules when no pre-compiled version is
        available or suitable) can be time-consuming.
          * On small instances (e.g., 2-core nodes), this process can take
            up to **150 minutes**.
          * To optimize and avoid long startup times on production clusters,
            it is highly recommended to "pre-warm" the GCS cache. This can be
            done by running the script once on a temporary, larger instance
            (e.g., a single-node, 32-core machine) with your target OS and
            desired GPU configuration. This will build and cache the necessary
            components. Subsequent cluster creations using the same cache bucket
            will be significantly faster (e.g., the init action might take
            12-20 minutes on a large instance for the initial build, and then
            much faster on subsequent nodes using the cache).
      * **Security Benefit of Caching:** When the script successfully finds and
        uses cached, pre-built artifacts, it often bypasses the need to
        install build tools (e.g., `gcc`, `kernel-devel`, `make`) on the
        cluster nodes. This reduces the attack surface area of the
        resulting cluster instances.
  * SSHD configuration is hardened by default by the script.
  * The script includes logic to manage APT sources and GPG keys for
    Debian-based systems, including handling of archived backports repositories
    to ensure dependencies can be met.
  * Tested primarily with Dataproc 2.0+ images. Support for older Dataproc
    1.5 images is limited.