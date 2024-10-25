import os
import time

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class RapidsTestCase(DataprocTestCase):
  COMPONENT = "rapids"
  INIT_ACTIONS = [
      "gpu/install_gpu_driver.sh", "rapids/rapids.sh"
  ]

  GPU_A100 = "type=nvidia-tesla-a100,count=2"
  GPU_H100 = "type=nvidia-h100-80gb,count=2"
  GPU_T4   = "type=nvidia-tesla-t4"

  # Tests for RAPIDS init action
  DASK_RAPIDS_TEST_SCRIPT_FILE_NAME = "verify_rapids_dask.py"

  def verify_dask_worker_service(self, name):
    # Retry the first ssh to ensure it has enough time to propagate SSH keys
    for try_number in range(0, 3):
      try:
        self.assert_instance_command(
            name, "[[ X$(systemctl show dask-worker -p SubState --value)X == XrunningX ]]")
        break
      except:
        time.sleep(2**try_number)

  def verify_dask_config(self, name):
    self.assert_instance_command(
        name, "grep 'class: \"dask_cuda.CUDAWorker\"' /etc/dask/config.yaml")

  def run_dask_script(self, name):
    test_filename=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               self.DASK_RAPIDS_TEST_SCRIPT_FILE_NAME)
    self.upload_test_file(test_filename, name)
    verify_cmd = "/opt/conda/miniconda3/envs/dask-rapids/bin/python {}".format(
        self.DASK_RAPIDS_TEST_SCRIPT_FILE_NAME)
    self.assert_instance_command(name, verify_cmd)
    self.remove_test_script(self.DASK_RAPIDS_TEST_SCRIPT_FILE_NAME, name)

                            
  @parameterized.parameters(
# If a new version of dask-yarn is released, add this test back in.
#    ("STANDARD", ["m", "w-0"], GPU_T4, "yarn"),
#    ("STANDARD", ["m"], GPU_T4, None),
    ("STANDARD", ["m", "w-0"], GPU_T4, "standalone")
  )
  def test_rapids_dask(self, configuration, machine_suffixes, accelerator,
                       dask_runtime):

    metadata = "gpu-driver-provider=NVIDIA,rapids-runtime=DASK"
    if dask_runtime:
      metadata += ",dask-runtime={}".format(dask_runtime)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        machine_type="n1-standard-8",
        master_accelerator=accelerator,
        worker_accelerator=accelerator,
        boot_disk_size="50GB",
        timeout_in_minutes=60)

    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(self.getClusterName(),machine_suffix)
      if dask_runtime == 'standalone' or dask_runtime == None:
        self.verify_dask_worker_service(machine_name)
      elif dask_runtime == 'yarn':
        self.verify_dask_config(machine_name)

      self.run_dask_script(machine_name)


if __name__ == "__main__":
  absltest.main()
