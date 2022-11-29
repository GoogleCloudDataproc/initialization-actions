import pkg_resources
import logging
import os
import subprocess
import time
from threading import Timer

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

DEFAULT_TIMEOUT = 15  # minutes

class KernelTestCase(DataprocTestCase):
  COMPONENT = "kernel"

  @staticmethod
  def run_command(cmd, timeout_in_minutes=DEFAULT_TIMEOUT):
    cmd = cmd.replace("gcloud compute ssh ", "gcloud compute ssh --tunnel-through-iap ") if (
                      "gcloud compute ssh " in cmd) else cmd
    cmd = cmd.replace("gcloud compute scp ", "gcloud beta compute scp --tunnel-through-iap ") if (
                      "gcloud compute scp " in cmd) else cmd
    p = subprocess.Popen(
        cmd,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    timeout = timeout_in_minutes * 60
    my_timer = Timer(timeout, lambda process: process.kill(), [p])
    try:
        my_timer.start()
        stdout, stderr = p.communicate()
        stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    finally:
        my_timer.cancel()
    logging.debug("Ran %s: retcode: %d, stdout: %s, stderr: %s", cmd,
                  p.returncode, stdout, stderr)
    return p.returncode, stdout, stderr

  def assert_command(self, cmd, timeout_in_minutes=DEFAULT_TIMEOUT):
    ret_code, stdout, stderr = ( None, None, None )
    for try_number in range(10):
      time.sleep(30 * try_number)
      ret_code, stdout, stderr = KernelTestCase.run_command(
          cmd, timeout_in_minutes)
      if ( ret_code == 0 ):
        break

    self.assertEqual(
        ret_code, 0,
        "Failed to execute command:\n{}\nSTDOUT:\n{}\nSTDERR:\n{}".format(
            cmd, stdout, stderr))
    return ret_code, stdout, stderr


class KernelUpgradeTestCase(KernelTestCase):
  INIT_ACTIONS = ["kernel/upgrade-kernel.sh"]

  def verify_kernel(self, name):
    self.assert_instance_command(
        name, "cat /proc/version", 2)

  @parameterized.parameters(
      ("SINGLE", ["m"]),
      ("STANDARD", ["m", "w-0", "w-1"]),
  )
  def test_upgrade_kernel(self, configuration, machine_suffixes):

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-2",
        timeout_in_minutes=30,
        boot_disk_size="200GB")

    for machine_suffix in machine_suffixes:
        self.verify_kernel("{}-{}".format(self.getClusterName(),
                                          machine_suffix))
"""
class KernelDowngradeTestCase(KernelTestCase):
  INIT_ACTIONS = ["kernel/revert-kernel.sh"]

  @parameterized.parameters(
      ("SINGLE", ["m"]),
      ("STANDARD", ["m", "w-0", "w-1"]),
  )
  def test_downgrade_kernel(self, configuration, machine_suffixes):

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-2",
        timeout_in_minutes=30,
        boot_disk_size="200GB")

    for machine_suffix in machine_suffixes:
      self.verify_kernel("{}-{}".format(self.getClusterName(),
                                        machine_suffix))
"""

if __name__ == "__main__":
  absltest.main()
