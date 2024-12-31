import os
import time

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class MigTestCase(DataprocTestCase):
  COMPONENT = "rapids"
  INIT_ACTIONS = ["spark-rapids/mig.sh"]

  GPU_H100 = "type=nvidia-h100-80gb,count=2"
  default_machine_type = "n1-standard-32"

  def verify_mig_instance(self, name):
    self.assert_instance_command(name,
        "/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | xargs -I % test % = 'Enabled'")

  @parameterized.parameters( ("SINGLE", ["m"], GPU_H100, None, "NVIDIA", "us-central1-c")
#                             ("STANDARD", ["m", "w-0", "w-1"], None, self.GPU_H100, "NVIDIA", "us-central1-c")
#                             ("KERBEROS", ["m", "w-0", "w-1"], None, self.GPU_H100, "NVIDIA", "us-central1-c")
                            )
  def test_install_gpu_with_mig(self, configuration, machine_suffixes,
                                  master_accelerator, worker_accelerator,
                                  driver_provider, zone):

    if ( self.getImageOs() == 'ubuntu' ) \
    and self.getImageVersion() >= pkg_resources.parse_version("2.1"):
      self.skipTest("2.1-ubuntu20 and 2.2-ubuntu22 tests are known to Failed to initialize Spark session")

    if configuration == 'SINGLE' and master_accelerator == None:
      master_accelerator=self.GPU_H100

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        zone=zone,
        master_machine_type="a3-highgpu-2g" if master_accelerator == self.GPU_H100 else self.default_machine_type,
        worker_machine_type="a3-highgpu-2g" if worker_accelerator == self.GPU_H100 else self.default_machine_type,
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=None,
        timeout_in_minutes=30,
        network_interface="nic-type=GVNIC,address=,network=default",
        boot_disk_size="40GB",
        )

    for machine_suffix in machine_suffixes:
      self.verify_mig_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))
