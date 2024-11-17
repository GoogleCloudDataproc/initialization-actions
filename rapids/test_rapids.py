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
  INTERPRETER = '/opt/conda/miniconda3/envs/dask-rapids/bin/python'

  GPU_A100 = "type=nvidia-tesla-a100,count=2"
  GPU_H100 = "type=nvidia-h100-80gb,count=2"
  GPU_T4   = "type=nvidia-tesla-t4"

  # Tests for RAPIDS init action
  DASK_RAPIDS_TEST_SCRIPT_FILE_NAME = "verify_rapids_dask.py"

  DASK_YARN_TEST_SCRIPT = 'verify_dask_yarn.py'
  DASK_STANDALONE_TEST_SCRIPT = 'verify_dask_standalone.py'

  def verify_dask_yarn(self, name):
    self._run_dask_test_script(name, self.DASK_YARN_TEST_SCRIPT)

  def verify_dask_standalone(self, name, master_hostname):
    script=self.DASK_STANDALONE_TEST_SCRIPT
    script_and_arg="{} {}".format( script, master_hostname )
    self._run_dask_test_script(name, script_and_arg)

  def _run_dask_test_script(self, name, script):
    verify_cmd = "{} {}".format( INTERPRETER, script )
    abspath=os.path.join(os.path.dirname(os.path.abspath(__file__)),script)
    self.upload_test_file(abspath, name)
    self.retry_assert_instance_command( name, verify_cmd )
    self.remove_test_script(script, name)

  def retry_assert_instance_command(self, name, verify_cmd):
    command_asserted=0
    for try_number in range(0, 3):
      try:
        self.assert_instance_command(name, verify_cmd)
        command_asserted=1
        break
      except Exception as err:
        print('command failed with exception «{}»'.format(err))
        time.sleep(2**try_number)
    if command_asserted == 0:
      raise Exception("Unable to assert instance command «{}»".format(verify_cmd))

  def verify_dask_worker_service(self, name):
    verify_cmd = "[[ X$(systemctl show dask-worker -p SubState --value)X == XrunningX ]]"
    self.retry_assert_instance_command( name, verify_cmd )

  def verify_dask_config(self, name):
    self.assert_instance_command(
        name, "grep 'class: \"dask_cuda.CUDAWorker\"' /etc/dask/config.yaml")

  @parameterized.parameters(
    ("STANDARD", ["m", "w-0"], GPU_T4, "yarn"),
    ("STANDARD", ["m"], GPU_T4, None),
    ("STANDARD", ["m", "w-0"], GPU_T4, "standalone")
  )
  def test_dask(self, configuration, machine_suffixes, accelerator,
                       dask_runtime):

    if self.getImageVersion() < pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in pre-2.0 images")

    metadata = "gpu-driver-provider=NVIDIA,rapids-runtime=DASK"
    if dask_runtime:
      metadata += ",dask-runtime={}".format(dask_runtime)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        machine_type='n1-standard-8',
        master_accelerator=accelerator,
        worker_accelerator=accelerator,
        timeout_in_minutes=40
    )

    c_name=self.getClusterName()
    if configuration == 'HA':
      master_hostname = c_name + '-m-0'
    else:
      master_hostname = c_name + '-m'

    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(c_name,machine_suffix)

      if dask_runtime == 'standalone' or dask_runtime == None:
        self.verify_dask_worker_service(machine_name)
        self.verify_dask_standalone(machine_name, master_hostname)
      elif dask_runtime == 'yarn':
        self.verify_dask_config(machine_name)
        self._run_dask_test_script(name, self.DASK_YARN_TEST_SCRIPT)

      self._run_dask_test_script(machine_name, self.DASK_RAPIDS_TEST_SCRIPT_FILE_NAME)


if __name__ == '__main__':
  absltest.main()
