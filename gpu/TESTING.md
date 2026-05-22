# Testing the GPU Initialization Script

This document details the recommended iterative development and testing process for the `install_gpu_driver.sh` script, bypassing the slow integration runner when developing and ensuring comprehensive testing when complete.

## Fast Iterative Development (SSH/Manual)

When making structural or execution logic changes, you want to avoid destroying and recreating the entire Dataproc cluster during each test cycle.

### 1. Provision a "Bare" GPU Cluster
Use the `--no-init-action` flag on the recreation script to provision a cluster with GPUs attached, but without running any initialization actions during boot.

```bash
cd cloud-dataproc/gcloud
./bin/recreate-dpgce --gpu --no-init-action
```

### 2. Stage and Transfer the Script
Use the optimized `scp-m` command to transfer your local changes to the master node. This script stages the file in the GCS temp bucket and pulls it down to `/tmp/install_gpu_driver.sh` over SSH.

```bash
cd cloud-dataproc/gcloud
./bin/scp-m ../../initialization-actions/gpu/install_gpu_driver.sh
```

### 3. Execute and Monitor
Execute the script manually over SSH as root. Pumping the output through `tee` captures the logs identically to how Dataproc normally records initialization scripts.

```bash
cd cloud-dataproc/gcloud
./bin/ssh-m 'sudo bash -x /tmp/install_gpu_driver.sh 2>&1 | tee /tmp/install_gpu_driver.log'
```

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
