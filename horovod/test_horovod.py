import pkg_resources

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HorovodTestCase(DataprocTestCase):
  COMPONENT = "horovod"
  INIT_ACTIONS = ["horovod/horovod.sh"]
  GPU_INIT_ACTIONS = ["gpu/install_gpu_driver.sh"] + INIT_ACTIONS
  GPU_P100 = "type=nvidia-tesla-p100"
  GPU_T4 = "type=nvidia-tesla-t4"

  TENSORFLOW_TEST_SCRIPT = "scripts/verify_tensorflow.py"
  PYTORCH_TEST_SCRIPT = "scripts/verify_pytorch.py"
  MXNET_TEST_SCRIPT = "scripts/verify_mxnet.py"

  def _submit_spark_job(self, script):
    self.assert_dataproc_job(
        self.name, "pyspark",
        "{}/horovod/scripts/{}".format(script, self.INIT_ACTIONS_REPO))

  @parameterized.parameters(
      ("STANDARD", "mpi"),
      ("STANDARD", "gloo"),
  )
  def test_horovod_cpu(self, configuration, controller):
    if self.getImageOs() == 'rocky':
      self.skipTest("Not supported in Rocky Linux-based images")
    if self.getImageVersion() > pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in Dataproc image version 2.1 and 2.2")

    metadata = ""
    if controller == "mpi":
      metadata += "install-mpi=true"

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        timeout_in_minutes=60,
        machine_type="e2-standard-8",
        metadata=metadata)

  @parameterized.parameters(
      ("STANDARD", "gloo"),
  )
  def test_horovod_gpu(self, configuration, controller):
    if self.getImageOs() == 'rocky':
      self.skipTest("Not supported in Rocky Linux-based images")
    if self.getImageVersion() > pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in Dataproc image version 2.1 and 2.2")

    metadata = "cuda-version=12.4,cudnn-version=9.1.0.70,gpu-driver-provider=NVIDIA"

    self.createCluster(
        configuration,
        self.GPU_INIT_ACTIONS,
        timeout_in_minutes=60,
        machine_type="n1-standard-8",
        master_accelerator=self.GPU_T4,
        worker_accelerator=self.GPU_T4,
        metadata=metadata)


if __name__ == "__main__":
  absltest.main()
