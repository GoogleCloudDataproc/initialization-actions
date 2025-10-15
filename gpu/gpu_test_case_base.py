import os
import time
import random
from packaging import version
from integration_tests.dataproc_test_case import DataprocTestCase

DEFAULT_TIMEOUT = 45  # minutes

class GpuTestCaseBase(DataprocTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_dataproc_job(self,
                         cluster_name,
                         job_type,
                         job_params,
                         timeout_in_minutes=DEFAULT_TIMEOUT):
        """Executes Dataproc job on a cluster and returns results.

        Args:
            cluster_name: cluster name to submit job to
            job_type: type of the job, e.g. spark, hadoop, pyspark
            job_params: job parameters
            timeout_in_minutes: timeout in minutes

        Returns:
            ret_code: the return code of the job
            stdout: standard output of the job
            stderr: error output of the job
        """

        ret_code, stdout, stderr = DataprocTestCase.run_command(
            'gcloud dataproc jobs submit {} --cluster={} --region={} {}'.
            format(job_type, cluster_name, self.REGION,
                   job_params), timeout_in_minutes)
        return ret_code, stdout, stderr

    # Tests for PyTorch
    TORCH_TEST_SCRIPT_FILE_NAME = "verify_pytorch.py"

    # Tests for TensorFlow
    TF_TEST_SCRIPT_FILE_NAME = "verify_tensorflow.py"

    def assert_instance_command(self,
                             instance,
                             cmd,
                             timeout_in_minutes=DEFAULT_TIMEOUT):
        retry_count = 5
        ssh_cmd = 'gcloud compute ssh -q {} --zone={} --command="{}" -- -o ConnectTimeout=60 -o StrictHostKeyChecking=no'.format(
            instance, self.cluster_zone, cmd.replace('"', '\"'))

        while retry_count > 0:
            try:
                # Use self.assert_command from DataprocTestCase
                ret_code, stdout, stderr = self.assert_command(ssh_cmd, timeout_in_minutes)
                return ret_code, stdout, stderr
            except Exception as e:
                print(f"An error occurred in assert_instance_command: {e}")
                retry_count -= 1
                if retry_count > 0:
                    print(f"Retrying in 10 seconds...")
                    time.sleep(10)
                    continue
                else:
                    print("Max retries reached.")
                    raise

    def verify_instance(self, name):
        # Verify that nvidia-smi works
        self.assert_instance_command(name, "nvidia-smi", 1)
        print(f"OK: nvidia-smi on {name}")

    def verify_instance_gpu_agent(self, name):
        print(f"--- Verifying GPU Agent on {name} ---")
        self.assert_instance_command(
            name, "systemctl is-active gpu-utilization-agent.service")
        print(f"OK: GPU Agent on {name}")

    def get_dataproc_image_version(self, instance):
        _, stdout, _ = self.assert_instance_command(instance, "grep DATAPROC_IMAGE_VERSION /etc/environment | cut -d= -f2")
        return stdout.strip()

    def version_lt(self, v1, v2):
        return version.parse(v1) < version.parse(v2)

    def verify_pytorch(self, name):
        print(f"--- Verifying PyTorch on {name} ---")
        test_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gpu",
                                   self.TORCH_TEST_SCRIPT_FILE_NAME)
        self.upload_test_file(test_filename, name)

        image_version = self.get_dataproc_image_version(name)
        conda_root_path = "/opt/conda/miniconda3"
        if not self.version_lt(image_version, "2.3"):
            conda_root_path = "/opt/conda"

        conda_env = "dpgce"
        env_path = f"{conda_root_path}/envs/{conda_env}"
        python_bin = f"{env_path}/bin/python3"

        verify_cmd = (
            f"for f in /sys/module/nvidia/drivers/pci:nvidia/*/numa_node; do "
            f"  if [[ -e \\\"$f\\\" ]]; then echo 0 > \\\"$f\\\"; fi; "
            f"done; "
            f"if /usr/share/google/get_metadata_value attributes/include-pytorch; then"
            f"  {python_bin} {self.TORCH_TEST_SCRIPT_FILE_NAME}; "
            f"else echo 'PyTorch test skipped as include-pytorch is not set'; fi"
        )
        _, stdout, _ = self.assert_instance_command(name, verify_cmd)
        if "PyTorch test skipped" not in stdout:
             self.assertTrue("True" in stdout, f"PyTorch CUDA not available or python not found in {env_path}")
        print(f"OK: PyTorch on {name}")
        self.remove_test_script(self.TORCH_TEST_SCRIPT_FILE_NAME, name)

    def verify_tensorflow(self, name):
        print(f"--- Verifying TensorFlow on {name} ---")
        test_filename=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gpu",
                               self.TF_TEST_SCRIPT_FILE_NAME)
        self.upload_test_file(test_filename, name)

        image_version = self.get_dataproc_image_version(name)
        conda_root_path = "/opt/conda/miniconda3"
        if not self.version_lt(image_version, "2.3"):
            conda_root_path = "/opt/conda"

        conda_env="dpgce"
        env_path = f"{conda_root_path}/envs/{conda_env}"
        python_bin = f"{env_path}/bin/python3"

        verify_cmd = (
            f"for f in $(ls /sys/module/nvidia/drivers/pci:nvidia/*/numa_node) ; do echo 0 > ${{f}} ; done ;"
            f"{python_bin} {self.TF_TEST_SCRIPT_FILE_NAME}"
        )
        self.assert_instance_command(name, verify_cmd)
        print(f"OK: TensorFlow on {name}")
        self.remove_test_script(self.TF_TEST_SCRIPT_FILE_NAME, name)
