import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class RapidsTestCase(DataprocTestCase):
  COMPONENT = "rapids"
  INIT_ACTIONS = ["sparkRapids/spark-rapids.sh"]

  GPU_P100 = "type=nvidia-tesla-p100"

  # Tests for RAPIDS init action
  XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME = "verify_xgboost_spark_rapids.scala"

  def verify_spark_instance(self, name):
    self.assert_instance_command(name, "nvidia-smi")

  def verify_spark_job(self):
    instance_name = "{}-m".format(self.getClusterName())
    self.upload_test_file(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME), instance_name)
    self.assert_instance_command(
        instance_name, """echo :quit | spark-shell \
         --conf spark.executor.resource.gpu.amount=1 \
         --conf spark.task.resource.gpu.amount=1 \
         --conf spark.dynamicAllocation.enabled=false -i {}""".format(
             self.XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME))
    self.remove_test_script(self.XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME,
                            instance_name)

  @parameterized.parameters(("SINGLE", ["m"], GPU_P100),
                            ("STANDARD", ["w-0"], GPU_P100))
  def test_rapids_spark(self, configuration, machine_suffixes, accelerator):
    if self.getImageOs() == "rocky":
      self.skipTest("Not supported in Rocky Linux-based images")

    optional_components = None
    metadata = "gpu-driver-provider=NVIDIA,rapids-runtime=SPARK"
    if self.getImageVersion() < pkg_resources.parse_version("2.0"):
      optional_components = ["ANACONDA"]
      metadata += ",cuda-version=10.1"

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        optional_components=optional_components,
        metadata=metadata,
        machine_type="n1-standard-4",
        master_accelerator=accelerator if configuration == "SINGLE" else None,
        worker_accelerator=accelerator,
        timeout_in_minutes=30)

    for machine_suffix in machine_suffixes:
      self.verify_spark_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
    # Only need to do this once
    self.verify_spark_job()

  @parameterized.parameters(("STANDARD", ["w-0"], GPU_P100, "11.2"))
  def test_non_default_cuda_versions(self, configuration, machine_suffixes,
                                     accelerator, cuda_version):
    if self.getImageOs() == "rocky":
      self.skipTest("Not supported in Rocky Linux-based images")

    if self.getImageVersion() < pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in pre 2.0 images")

    metadata = ("gpu-driver-provider=NVIDIA,rapids-runtime=SPARK"
                ",cuda-version={}".format(cuda_version))

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        machine_type="n1-standard-4",
        master_accelerator=accelerator if configuration == "SINGLE" else None,
        worker_accelerator=accelerator,
        timeout_in_minutes=30)

    for machine_suffix in machine_suffixes:
      self.verify_spark_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
    # Only need to do this once
    self.verify_spark_job()


if __name__ == "__main__":
  absltest.main()
