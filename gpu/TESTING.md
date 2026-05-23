# Testing the GPU Initialization Script

This document details the recommended iterative development and testing process for the `install_gpu_driver.sh` script, bypassing the slow integration runner when developing and ensuring comprehensive testing when complete.

## Fast Iterative Development (SSH/Manual)

This initialization action is designed to be **idempotent**, meaning it can be run multiple times on the same node without breaking the environment. It achieves this by writing "completion sentinels" to `/opt/install-dpgce/complete/` after successfully finishing each phase (e.g., `build-dependencies`, `nccl`, `cuda`).

To facilitate rapid iteration, we use the tooling provided in the companion `cloud-dataproc/gcloud` repository. This repo contains the test infrastructure, environment configuration (`env.json`), and lifecycle management scripts (`recreate-dpgce`, `ssh-m`, `scp-m`) necessary to provision and interact with test clusters efficiently.

When making structural or execution logic changes, you want to avoid destroying and recreating the entire Dataproc cluster during each test cycle. Instead, follow this incremental workflow:

### 1. Provision a "Bare" GPU Cluster
First, configure your target OS and versions in `cloud-dataproc/gcloud/env.json`. Then, use the `--no-init-action` flag on the recreation script to provision a cluster with GPUs attached, but *without* running any initialization actions during boot.

```bash
cd cloud-dataproc/gcloud
./bin/recreate-dpgce --gpu --no-init-action
```

### 2. Compile and Stage the Script
The `install_gpu_driver.sh` script is built from fragments. First, compile the fragments, then use the optimized `scp-m` command to transfer your local changes to the -m node. This script stages the file in the GCS temp bucket and pulls it down to `/tmp/install_gpu_driver.sh` over SSH.

```bash
cd initialization-actions
cat gpu/install_gpu_driver.sh.d/*.sh > gpu/install_gpu_driver.sh
cd ../cloud-dataproc/gcloud
./bin/scp-m ../../initialization-actions/gpu/install_gpu_driver.sh
```

### 3. Execute and Monitor (Incremental Testing)
Execute the script manually over SSH as root. Pumping the output through `tee` captures the logs identically to how Dataproc normally records initialization scripts.

**Crucially, when re-running the script to test a specific fix, you must purge the relevant completion sentinels** (and partial build directories like `nccl`) so the script doesn't skip the phase you are trying to test.

*   To run the *entire* script from scratch: `sudo rm -rf /opt/install-dpgce/complete`
*   To re-test only the NCCL build: `sudo rm -f /opt/install-dpgce/complete/nccl && sudo rm -rf /opt/install-dpgce/nccl`

```bash
cd cloud-dataproc/gcloud
./bin/ssh-m 'sudo rm -rf /opt/install-dpgce/complete' # Example: clear everything
cd ../../initialization-actions
./gpu/install-in-screen.sh
```

If your SSH connection drops, simply run `./gpu/install-in-screen.sh` again to instantly re-attach to the running session without losing context or interrupting the installation.

### 4. Verify with the Test Suite
Once the installation script completes without errors, run the external testing suite to ensure all Conda environments (PyTorch, TensorFlow, RAPIDS) and Spark services correctly bind to the GPU.

```bash
cd cloud-dataproc/gcloud
bash t/spark-gpu-test.sh
```

## Continuous Integration Testing (Bazel/Podman)

Once the manual tests pass, verify the script behaves correctly within the isolated Python `absl` test harness running inside Podman.

```bash
cd initialization-actions
./gpu/run-bazel-tests-with-podman.sh "2.2-debian12"
```

**Note:** Ensure your `key.json` (ADC credentials) and `--test_env` mappings are properly configured so the sandbox can authenticate against GCP APIs.

## Compiling the AST Splitter Tool (`split.go`)

If you need to re-split `install_gpu_driver.sh` into its `.d/` fragments (e.g. if the main script was modified instead of the fragments), we use a Go-based AST parsing tool (`split.go`) to accurately chunk the bash script.

To compile the tool locally:

```bash
cd initialization-actions/gpu
go mod init split
go get mvdan.cc/sh/v3/syntax
go build -o split_ast split.go
```

Once compiled, executing `./split_ast install_gpu_driver.sh` will parse the script and populate the `install_gpu_driver.sh.d/` directory with the chunked components.
