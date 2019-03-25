import unittest
from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class NvidiaGpuDriverTestCase(DataprocTestCase):
  COMPONENT = 'gpu'
  INIT_ACTION = 'gs://dataproc-initialization-actions/gpu/install_gpu_driver.sh'
  MASTER_GPU_TYPE = 'type=nvidia-tesla-v100'
  WORKER_GPU_TYPE = 'type=nvidia-tesla-v100'

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    _, region, _ = cls.run_command("gcloud config get-value compute/region")
    cls.REGION = region.strip() or "global"

  def verify_instance(self, name):
    ret_code, stdout, stderr = self.run_command(
      'gcloud compute ssh {} --command '
      '"nvidia-smi"'.format(name)
    )
    self.assertEqual(ret_code, 0,
                     "Failed to validate cluster. Error: {}".format(stderr))

  def verify_instance_gpu_agent(self, name):
    ret_code, stdout, stderr = self.run_command(
      'gcloud compute ssh {} --command '
      '"systemctl status gpu_utilization_agent.service"'.format(name)
    )
    self.assertEqual(ret_code, 0,
                     "Failed to validate cluster. Error: {}".format(stderr))

  @parameterized.expand([
    ("STANDARD", "1.3", ["m"], MASTER_GPU_TYPE, WORKER_GPU_TYPE),
  ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
  def test_install_gpu(self, configuration, dataproc_version, machine_suffixes,
                       master_accelerator, worker_accelerator):
    init_actions = self.INIT_ACTION
    self.createCluster(configuration, init_actions, dataproc_version,
                       beta=True,
                       master_accelerator=master_accelerator,
                       worker_accelerator=worker_accelerator,
                       metadata='install_gpu_agent=false')
    for machine_suffix in machine_suffixes:
      self.verify_instance(
        "{}-{}".format(
          self.getClusterName(),
          machine_suffix
        )
      )

  @parameterized.expand([
    ("STANDARD", "1.0", ["m"], MASTER_GPU_TYPE, WORKER_GPU_TYPE),
    ("STANDARD", "1.1", ["m"], MASTER_GPU_TYPE, WORKER_GPU_TYPE),
    ("STANDARD", "1.2", ["m"], MASTER_GPU_TYPE, WORKER_GPU_TYPE),
    ("STANDARD", "1.3", ["m"], MASTER_GPU_TYPE, WORKER_GPU_TYPE),
  ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
  def test_install_gpu_agent(self, configuration, dataproc_version,
                             machine_suffixes, master_accelerator,
                             worker_accelerator):

    init_actions = self.INIT_ACTION
    self.createCluster(configuration, init_actions, dataproc_version,
                       beta=True,
                       master_accelerator=master_accelerator,
                       worker_accelerator=worker_accelerator,
                       metadata='install_gpu_agent=true',
                       scopes='https://www.googleapis.com/auth/monitoring.write'
                       )
    for machine_suffix in machine_suffixes:
      self.verify_instance_gpu_agent(
        "{}-{}".format(
          self.getClusterName(),
          machine_suffix
        )
      )


if __name__ == '__main__':
  unittest.main()
