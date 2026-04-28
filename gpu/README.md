# GPU driver installation

GPUs require special drivers and software which are not pre-installed on
[Dataproc](https://cloud.google.com/dataproc) clusters by default.
This initialization action installs GPU driver for NVIDIA GPUs on -m node(s) and
-w nodes in a Dataproc cluster.

## Default versions

A default version will be selected from NVIDIA's guidance, similar to the
[NVIDIA Deep Learning Frameworks Support Matrix](https://docs.nvidia.com/deeplearning/frameworks/support-matrix/index.html),
for CUDA, the NVIDIA kernel driver, cuDNN, and NCCL.

Specifying a supported value for the `cuda-version` metadata variable
will select compatible values for Driver, cuDNN, and NCCL from the script's
internal matrix. Default CUDA versions are typically:

  * Dataproc 1.5: `11.6.2`
  * Dataproc 2.0: `12.1.1`
  * Dataproc 2.1: `12.4.1`
  * Dataproc 2.2 & 2.3: `12.6.3`

*(Note: The script supports a wider range of specific versions.
Refer to internal arrays in `install_gpu_driver.sh` for the full matrix.)*

**Example Tested Configurations (Illustrative):**

CUDA | Full Version | Driver    | cuDNN     | NCCL   | Tested Dataproc Image Versions
-----| ------------ | --------- | --------- | -------| ---------------------------
11.8 | 11.8.0       | 525.147.05| 9.5.1.17  | 2.21.5 | 2.0, 2.1 (Debian, Ubuntu)
12.0 | 12.0.1       | 525.147.05| 8.8.1.3   | 2.16.5 | 2.0, 2.1 (Debian, Ubuntu)
12.4 | 12.4.1       | 550.135   | 9.1.0.70  | 2.23.4 | 2.0, 2.1 (Debian, Ubuntu); 2.2+ (Debian, Ubuntu, Rocky)
12.6 | 12.6.3       | 550.142   | 9.6.0.74  | 2.23.4 | 2.2+ (Debian, Ubuntu, Rocky)

*Note: Secure Boot is only supported on Dataproc 2.2+ images.*

**Supported Operating Systems:**

  * Debian 10, 11, 12
  * Ubuntu 18.04, 20.04, 22.04 LTS
  * Rocky Linux 8, 9

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used)
of using initialization actions in production.

The recommended way to create a Dataproc cluster with GPU support, especially for environments requiring custom images, Secure Boot, or private networks with proxies, is to use the tooling provided in the [GoogleCloudDataproc/cloud-dataproc](https://github.com/GoogleCloudDataproc/cloud-dataproc) repository. This approach simplifies configuration and automates the `gcloud` command generation.

**Steps:**

1.  **Clone the `cloud-dataproc` Repository:**
    ```bash
    git clone https://github.com/GoogleCloudDataproc/cloud-dataproc.git
    cd cloud-dataproc/gcloud
    ```

2.  **Configure Your Environment:**
    *   Copy the sample configuration: `cp env.json.sample env.json`
    *   Edit `env.json` to match your desired cluster setup.

    **Note on JSON Examples:** Any lines in the JSON example below starting with `//` are comments for explanation and should be removed before using the JSON.

    **Key `env.json` Properties:**

    *   **Required:**
        *   `PROJECT_ID`: Your Google Cloud Project ID.
        *   `REGION`: The GCP region for the cluster.
        *   `ZONE`: The GCP zone within the region.
        *   `BUCKET`: A GCS bucket for staging and temporary files.
    *   **GPU Related:**
        *   `GPU_MASTER_ACCELERATORS`: e.g., "type=nvidia-tesla-t4,count=1" (Optional, can be omitted if no GPU on master)
        *   `GPU_WORKER_ACCELERATORS`: e.g., "type=nvidia-tesla-t4,count=1" (Optional, to have GPUs on workers)
    *   **Image:**
        *   `DATAPROC_IMAGE_VERSION`: e.g., "2.2-debian12" (Required, if not using `CUSTOM_IMAGE_NAME`)
        *   `CUSTOM_IMAGE_NAME`: Set this to the name of your pre-built custom image if you have one (e.g., from the Secure Boot image building process).
    *   **Optional (Defaults & Advanced):**
        *   `MACHINE_TYPE_MASTER`, `MACHINE_TYPE_WORKER`
        *   `NUM_MASTERS`, `NUM_WORKERS`
        *   `BOOT_DISK_SIZE`, `BOOT_DISK_TYPE`
        *   `NETWORK`, `SUBNET`: For specifying existing networks.
        *   `INTERNAL_IP_ONLY`: Set to `true` for private clusters.
        *   **Proxy Settings:** `SWP_IP`, `SWP_PORT`, `SWP_HOSTNAME`, `PROXY_PEM_URI`, `PROXY_PEM_HASH` (for private networks with Secure Web Proxy).
        *   **Secure Boot:** `ENABLE_SECURE_BOOT` (set to `true` if using a Secure Boot enabled custom image).

    The `install_gpu_driver.sh` initialization action is automatically added by the scripts in `bin/` if any `GPU_*_ACCELERATORS` are defined in `env.json`.

3.  **Create the Cluster:**
    Make sure you are in the `cloud-dataproc/gcloud` directory before running these commands.
    *   To create a new environment (VPC, subnet, proxy if configured) and the cluster:
        ```bash
        bash bin/create-dpgce
        ```
    *   To recreate the cluster in an existing environment defined by `env.json`:
        ```bash
        bash bin/recreate-dpgce
        ```

These scripts will parse `env.json` and construct the appropriate `gcloud dataproc clusters create` command with all necessary flags, including the initialization action, metadata, scopes, and network settings.

For detailed instructions on Secure Boot custom image creation and private network setup, see the "Building Custom Images with Secure Boot and Proxy Support" section below.

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
    If `yes`, installs PyTorch, Numba, TensorFlow, RAPIDS, and PySpark
    in a Conda environment (named by `gpu-conda-env`). **This also registers
    the created Conda environment as a Jupyter kernel.**
  * `gpu-conda-env`: (Optional) Name for the PyTorch Conda environment.
    Default: `dpgce`.
  * `container-runtime`: (Optional) E.g., `docker`, `containerd`, `crio`.
    For NVIDIA Container Toolkit configuration. Auto-detected if not specified.
  * `http-proxy`: (Optional) Proxy address and port for HTTP requests (e.g., `your-proxy.com:3128`).
  * `https-proxy`: (Optional) Proxy address and port for HTTPS requests (e.g., `your-proxy.com:3128`). Defaults to `http-proxy` if not set.
  * `proxy-uri`: (Optional) A single proxy URI for both HTTP and HTTPS. Overridden by `http-proxy` or `https-proxy` if they are set.
  * `no-proxy`: (Optional) Comma or space-separated list of hosts/domains to bypass the proxy. Defaults include localhost, metadata server, and Google APIs. User-provided values are appended to the defaults.
  * `http-proxy-pem-uri`: (Optional) A `gs://` path to the
    PEM-encoded CA certificate file for the proxy specified in
    `http-proxy`/`https-proxy`. Required if the proxy uses TLS with a certificate not in the default system trust store. This certificate will be added to the system, Java, and Conda trust stores, and proxy connections will use HTTPS.
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

### Network Evaluation

This script now includes a network evaluation function (`evaluate_network`) that runs early during execution. It gathers detailed information about the instance's network environment, including:

*   GCP Metadata (instance, project, network interface details)
*   Local IP and routing table information (`ip` commands)
*   DNS configuration (`/etc/resolv.conf`)
*   Proxy settings from metadata
*   External connectivity tests (e.g., public IP, reachability of key services)
*   Kerberos configuration status

The results are stored in `/run/dpgce-network.json` and printed to the log. This allows subsequent script logic to make more informed decisions based on the actual network state. Helper functions like `has_default_route()`, `is_proxy_enabled()`, and `can_reach_gstatic()` are available to query this information.

### Enhanced Proxy Support

This script includes robust support for environments requiring an HTTP/HTTPS proxy:

  *   **Configuration:** Use the `http-proxy`, `https-proxy`, or `proxy-uri` metadata to specify your proxy server (host:port).
  *   **Custom CA Certificates:** If your proxy uses a custom CA (e.g., self-signed), provide the CA certificate in PEM format via the `http-proxy-pem-uri` metadata (as a `gs://` path).
      *   **Integrity Check:** Optionally, provide the SHA256 hash of the PEM file via `http-proxy-pem-sha256` to ensure the downloaded file is correct.
      *   The script will:
          *   Install the CA into the system trust store (`update-ca-certificates` or `update-ca-trust`).
          *   Add the CA to the Java cacerts trust store.
          *   Configure Conda to use the system trust store.
          *   Switch proxy communications to use HTTPS.
  *   **Tool Configuration:** The script automatically configures `curl`, `apt`, `dnf`, `gpg`, `pip`, and Java to use the specified proxy settings and custom CA if provided. This is now guided by the results of the `evaluate_network` function.
  *   **Bypass:** The `no-proxy` metadata allows specifying hosts to bypass the proxy. Defaults include `localhost`, the metadata server, `.google.com`, and `.googleapis.com` to ensure essential services function correctly.
  *   **Verification:** The script performs connection tests to the proxy and attempts to reach external sites (google.com, nvidia.com) through the proxy to validate the configuration before proceeding with downloads.

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

## Building Custom Images with Secure Boot and Proxy Support

For environments requiring NVIDIA drivers to be signed for Secure Boot, especially when operating behind an HTTP/S proxy, you must first build a custom Dataproc image. This process uses tools from the [GoogleCloudDataproc/custom-images](https://github.com/GoogleCloudDataproc/custom-images) repository, specifically the scripts within the `examples/secure-boot/` directory.

**Base Image:** Typically Dataproc 2.2-debian12 or newer.

**Process Overview:**

1.  **Clone `custom-images` Repository:**
    ```bash
    git clone https://github.com/GoogleCloudDataproc/custom-images.git
    cd custom-images
    ```

2.  **Configure Build:** Set up `env.json` with your project, network, and bucket details. See the `examples/secure-boot/env.json.sample` in the `custom-images` repo.

3.  **Prepare Signing Keys:** Ensure Secure Boot signing keys are available in GCP Secret Manager. Use `examples/secure-boot/create-key-pair.sh` from the `custom-images` repo to create/manage these.

4.  **Build Docker Image:** Build the builder environment: `docker build -t dataproc-secure-boot-builder:latest .`

5.  **Run Image Generation:** Use `generate_custom_image.py` within the Docker container, typically orchestrated by `examples/secure-boot/pre-init.sh`. The core customization script `examples/secure-boot/install_gpu_driver.sh` handles driver installation, proxy setup, and module signing.

    *   Refer to the [Secure Boot example documentation](https://github.com/GoogleCloudDataproc/custom-images/tree/master/examples/secure-boot) for detailed `docker run` commands and metadata requirements (proxy settings, secret names, etc.).

### Launching a Cluster with the Secure Boot Custom Image

Once you have successfully built a custom image with signed drivers, you can create a Dataproc cluster with Secure Boot enabled.

**Important:** To launch a Dataproc cluster with the `--shielded-secure-boot` flag and have NVIDIA drivers function correctly, you MUST use a custom image created through the process detailed above. Standard Dataproc images do not contain the necessary signed modules.

**Network and Cluster Setup:**

To create the cluster in a private network environment with a Secure Web Proxy, use the scripts from the [GoogleCloudDataproc/cloud-dataproc](https://github.com/GoogleCloudDataproc/cloud-dataproc) repository:

1.  **Clone `cloud-dataproc` Repository:**
    ```bash
    git clone https://github.com/GoogleCloudDataproc/cloud-dataproc.git
    cd cloud-dataproc/gcloud
    ```

2.  **Configure Environment:**
    *   Copy `env.json.sample` to `env.json`.
    *   Edit `env.json` with your project details, ensuring you specify the custom image name and any necessary proxy details if you intend to run in a private network. Example:
        ```json
        {
          "PROJECT_ID": "YOUR_GCP_PROJECT_ID",
          "REGION": "us-west4",
          "ZONE": "us-west4-a",
          "BUCKET": "YOUR_STAGING_BUCKET",
          "TEMP_BUCKET": "YOUR_TEMP_BUCKET",
          "CUSTOM_IMAGE_NAME": "YOUR_BUILT_IMAGE_NAME",
          "PURPOSE": "secure-boot-cluster",
          // Add these for a private, proxied environment
          "PRIVATE_RANGE": "10.43.79.0/24",
          "SWP_RANGE": "10.44.79.0/24",
          "SWP_IP": "10.43.79.245",
          "SWP_PORT": "3128",
          "SWP_HOSTNAME": "swp.your-project.example.com"
          // ... other variables as needed
        }
        ```
    *   Set `CUSTOM_IMAGE_NAME` to the image you built in the `custom-images` process.

3.  **Create the Private Environment and Cluster:**
    This script sets up the VPC, subnets, Secure Web Proxy, and then creates the Dataproc cluster using the custom image. The `--shielded-secure-boot` flag is handled internally by the scripts when a `CUSTOM_IMAGE_NAME` is provided.
    ```bash
    bash bin/create-dpgce-private
    ```

**Verification:**

1.  SSH into the -m node of the created cluster.
2.  Check driver status: `sudo nvidia-smi`
3.  Verify module signature: `sudo modinfo nvidia | grep signer` (should show your custom CA).
4.  Check for errors: `dmesg | grep -iE "Secure Boot|NVRM|nvidia"`

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
  * **Network/Proxy Issues:** If using a proxy, double-check the `http-proxy`, `https-proxy`, `proxy-uri`, `no-proxy`, `http-proxy-pem-uri`, and `http-proxy-pem-sha256` metadata settings. Ensure the proxy allows access to NVIDIA domains, GitHub, and package repositories. Check the init action log for curl errors or proxy test failures. The `/run/dpgce-network.json` file contains detailed network diagnostics.
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
        `dataproc-temp-bucket` cluster property or metadata. Downloads and cache operations are proxy-aware.
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
