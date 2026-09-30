import os
import time

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class DaskTestCase(DataprocTestCase):
  COMPONENT = 'dask'
  INIT_ACTIONS = [
      'dask/dask.sh'
  ]
  INTERPRETER = '/opt/conda/miniconda3/envs/dask/bin/python'

  DASK_YARN_TEST_SCRIPT = 'verify_dask_yarn.py'
  DASK_STANDALONE_TEST_SCRIPT = 'verify_dask_standalone.py'

  def verify_dask_standalone(self, name, master_hostname):
    script=self.DASK_STANDALONE_TEST_SCRIPT
    verify_cmd = "{} {} {}".format(
      INTERPRETER,
      script,
      master_hostname
    )
    abspath=os.path.join(os.path.dirname(os.path.abspath(__file__)),script)
    self.upload_test_file(abspath, name)
    self.assert_instance_command(name, verify_cmd)
    self.remove_test_script(script, name)

  def _run_dask_test_script(self, name, script):
    test_filename=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               script), name)
    self.upload_test_file(test_filename, name)
    verify_cmd = "{} {}".format(
      INTERPRETER,
      script)
    command_asserted=0
    for try_number in range(0, 3):
      try:
      self.assert_instance_command(name, verify_cmd)
      command_asserted=1
      break
      except:
      time.sleep(2**try_number)
    if command_asserted == 0:
      raise Exception("Unable to assert instance command [{}]".format(verify_cmd))

    self.remove_test_script(script, name)

  def verify_dask_worker_service(self, name):
    verify_cmd = "[[ X$(systemctl show dask-worker -p SubState --value)X == XrunningX ]]"
    # Retry the first ssh to ensure it has enough time to propagate SSH keys
    command_asserted=0
    for try_number in range(0, 3):
      try:
        self.assert_instance_command(name, verify_cmd)
        command_asserted=1
        break
      except:
        time.sleep(2**try_number)
    if command_asserted == 0:
      raise Exception("Unable to assert instance command [{}]".format(verify_cmd))

  def verify_dask_config(self, name):
    self.assert_instance_command(
        name, "[[ $(wc -l /etc/dask/config.yaml) == 11 ]]")

  @parameterized.parameters(
    ("STANDARD", ["m", "w-0"], "yarn"),
    ("STANDARD", ["m", "w-0", "w-1"], "standalone"),
    ("KERBEROS", ["m"], None),
    ("HA", ["m-0"], None),
    ("SINGLE", ["m"], None),
  )
  def test_dask(self, configuration, machine_suffixes, dask_runtime):

    if self.getImageVersion() < pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in pre-2.0 images")

    metadata = None
    if dask_runtime:
      metadata = "dask-runtime={}".format(dask_runtime)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        machine_type='n1-standard-8',
        timeout_in_minutes=20
    )

    c_name=self.getClusterName()
    if configuration == 'HA':
      master_hostname = c_name + '-m-0'
    else:
      master_hostname = c_name + '-m'

    for machine_suffix in machine_suffixes:
      machine_name = "{}-{}".format(c_name, machine_suffix)

      if dask_runtime == 'standalone' or dask_runtime == None:
        self.verify_dask_worker_service(machine_name)
        self.verify_dask_standalone(machine_name, master_hostname)
      elif dask_runtime == 'yarn':
        self.verify_dask_config(machine_name)
        self._run_dask_test_script(name, self.DASK_YARN_TEST_SCRIPT)

if __name__ == '__main__':
  absltest.main()
