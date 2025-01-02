import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class SparkRapidsTestCase(DataprocTestCase):
  COMPONENT = "rapids"
  INIT_ACTIONS = ["spark-rapids/spark-rapids.sh"]

  GPU_T4 = "type=nvidia-tesla-t4"
  GPU_A100 = "type=nvidia-tesla-a100"

  # Tests for RAPIDS init action
  XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME = "verify_xgboost_spark_rapids.scala"
  XGBOOST_SPARK_SQL_TEST_SCRIPT_FILE_NAME = "verify_xgboost_spark_rapids_sql.scala"

  def verify_spark_instance(self, name):
    self.assert_instance_command(name, "nvidia-smi")

  def verify_mig_instance(self, name):
    self.assert_instance_command(name,
        "/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | xargs -I % test % = 'Enabled'")

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

  def verify_spark_job_sql(self):
    instance_name = "{}-m".format(self.getClusterName())
    self.upload_test_file(
      os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        self.XGBOOST_SPARK_SQL_TEST_SCRIPT_FILE_NAME), instance_name)
    self.assert_instance_command(
      instance_name, """echo :quit | spark-shell \
         --conf spark.executor.resource.gpu.amount=1 \
         --conf spark.task.resource.gpu.amount=1 \
         --conf spark.dynamicAllocation.enabled=false -i {}""".format(
        self.XGBOOST_SPARK_SQL_TEST_SCRIPT_FILE_NAME))
    self.remove_test_script(self.XGBOOST_SPARK_SQL_TEST_SCRIPT_FILE_NAME,
                            instance_name)

  @parameterized.parameters(("SINGLE", ["m"], GPU_T4),
                            ("STANDARD", ["w-0"], GPU_T4))
  def test_spark_rapids(self, configuration, machine_suffixes, accelerator):

    if self.getImageOs() == "rocky":
      self.skipTest("Not supported for Rocky OS")

    if self.getImageVersion() <= pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in 2.0 and earlier images")

    optional_components = None
    metadata = "gpu-driver-provider=NVIDIA,rapids-runtime=SPARK"

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        optional_components=optional_components,
        metadata=metadata,
        machine_type="n1-standard-4",
        master_accelerator=accelerator if configuration == "SINGLE" else None,
        worker_accelerator=accelerator,
        boot_disk_size="50GB",
        timeout_in_minutes=30)

    for machine_suffix in machine_suffixes:
      self.verify_spark_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
    # Only need to do this once
    self.verify_spark_job()

  @parameterized.parameters(("SINGLE", ["m"], GPU_T4),
                            ("STANDARD", ["w-0"], GPU_T4))
  def test_spark_rapids_sql(self, configuration, machine_suffixes, accelerator):

    if self.getImageOs() == "rocky":
      self.skipTest("Not supported for Rocky OS")

    if self.getImageVersion() <= pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in 2.0 and earlier images")

    optional_components = None
    metadata = "gpu-driver-provider=NVIDIA,rapids-runtime=SPARK"

    self.createCluster(
      configuration,
      self.INIT_ACTIONS,
      optional_components=optional_components,
      metadata=metadata,
      machine_type="n1-standard-4",
      master_accelerator=accelerator if configuration == "SINGLE" else None,
      worker_accelerator=accelerator,
      boot_disk_size="50GB",
      timeout_in_minutes=30)

    for machine_suffix in machine_suffixes:
      self.verify_spark_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
    # Only need to do this once
    self.verify_spark_job_sql()

  @parameterized.parameters(("STANDARD", ["w-0"], GPU_T4, "12.4.0", "550.54.14"))
  def test_non_default_cuda_versions(self, configuration, machine_suffixes,
                                     accelerator, cuda_version, driver_version):

    if self.getImageOs() == "rocky":
      self.skipTest("Not supported for Rocky OS")

    if self.getImageVersion() <= pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in 2.0 and earlier images")

    metadata = ("gpu-driver-provider=NVIDIA,rapids-runtime=SPARK"
                ",cuda-version={0},driver-version={1}".format(cuda_version, driver_version))

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        machine_type="n1-standard-4",
        master_accelerator=accelerator if configuration == "SINGLE" else None,
        worker_accelerator=accelerator,
        boot_disk_size="50GB",
        timeout_in_minutes=30)

    for machine_suffix in machine_suffixes:
      self.verify_spark_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
    # Only need to do this once
    self.verify_spark_job()

  # Disable MIG related test due to the lack of A100 GPUs, more detail see
  # https://github.com/GoogleCloudDataproc/initialization-actions/pull/1070

  # @parameterized.parameters(("STANDARD", ["m", "w-0", "w-1"], None, GPU_A100, "NVIDIA", "us-central1-c"))
  # def test_install_gpu_with_mig(self, configuration, machine_suffixes,
  #                                 master_accelerator, worker_accelerator,
  #                                 driver_provider, zone):
  #   if self.getImageVersion() < pkg_resources.parse_version("2.0") or self.getImageOs() == "rocky":
  #     self.skipTest("Not supported in pre 2.0 or Rocky images")
  # 
  #   if self.getImageVersion() == pkg_resources.parse_version("2.1"):
  #     self.skipTest("Not supported in 2.1 images")
  # 
  #   self.createCluster(
  #       configuration,
  #       self.INIT_ACTIONS,
  #       zone=zone,
  #       master_machine_type="n1-standard-4",
  #       worker_machine_type="a2-highgpu-1g",
  #       master_accelerator=master_accelerator,
  #       worker_accelerator=worker_accelerator,
  #       metadata=None,
  #       timeout_in_minutes=30,
  #       boot_disk_size="200GB",
  #       startup_script="spark-rapids/mig.sh")
  # 
  #   for machine_suffix in ["w-0", "w-1"]:
  #     self.verify_mig_instance("{}-{}".format(self.getClusterName(),
  #                                         machine_suffix))

if __name__ == "__main__":
  absltest.main()
