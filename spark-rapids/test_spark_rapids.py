import os
import time

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class SparkRapidsTestCase(DataprocTestCase):
  COMPONENT = "rapids"
  INIT_ACTIONS = ["spark-rapids/spark-rapids.sh"]

  GPU_T4 = "type=nvidia-tesla-t4"
  default_machine_type = "n1-highmem-8"

  # Tests for RAPIDS init action
  XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME = "verify_xgboost_spark_rapids.scala"
  XGBOOST_SPARK_SQL_TEST_SCRIPT_FILE_NAME = "verify_xgboost_spark_rapids_sql.scala"
  cmd_template="""echo :quit | spark-shell \
         --conf spark.executor.resource.gpu.amount=1 \
         --conf spark.task.resource.gpu.amount=1 \
         --conf spark.dynamicAllocation.enabled=false -i {}"""

  def verify_spark_instance(self, name):
    self.assert_instance_command(name, "nvidia-smi")

  def verify_spark_job(self):
    instance_name = "{}-m".format(self.getClusterName())
    self.upload_test_file(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME), instance_name)
    self.assert_instance_command(
        instance_name, self.cmd_template.format(self.XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME))
    self.remove_test_script(self.XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME,
                            instance_name)

  def verify_spark_job_sql(self):
    instance_name = "{}-m".format(self.getClusterName())
    self.upload_test_file(
      os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        self.XGBOOST_SPARK_SQL_TEST_SCRIPT_FILE_NAME), instance_name)
    self.assert_instance_command(
        instance_name, self.cmd_template.format(self.XGBOOST_SPARK_TEST_SCRIPT_FILE_NAME))
    self.remove_test_script(self.XGBOOST_SPARK_SQL_TEST_SCRIPT_FILE_NAME,
                            instance_name)

  @parameterized.parameters(("SINGLE", ["m"], GPU_T4),
                            ("STANDARD", ["w-0"], GPU_T4))
  def test_spark_rapids(self, configuration, machine_suffixes, accelerator):

    optional_components = None
    metadata = "gpu-driver-provider=NVIDIA,rapids-runtime=SPARK"

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        optional_components=optional_components,
        metadata=metadata,
        machine_type=self.default_machine_type,
        master_accelerator=accelerator if configuration == "SINGLE" else None,
        worker_accelerator=accelerator,
        boot_disk_size="40GB",
        timeout_in_minutes=30)

    for machine_suffix in machine_suffixes:
      self.verify_spark_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    if ( self.getImageOs() == 'rocky' ) \
    and self.getImageVersion() <= pkg_resources.parse_version("2.1") \
    and configuration == 'SINGLE':
      print("skipping spark job test ; 2.1-rocky8 and 2.0-rocky8 single instance tests are known to fail")
    elif ( self.getImageOs() == 'ubuntu' ) \
    and self.getImageVersion() >= pkg_resources.parse_version("2.1"):
      print("skipping spark job test ; 2.1-ubuntu20 and 2.2-ubuntu22 tests are known to Failed to initialize Spark session")
    else:
      # Only need to do this once
      self.verify_spark_job()
      # Only need to do this once
      self.verify_spark_job_sql()

  @parameterized.parameters(("STANDARD", ["w-0"], GPU_T4, "12.4.0", "550.54.14"))
  def test_non_default_cuda_versions(self, configuration, machine_suffixes,
                                     accelerator, cuda_version, driver_version):

    metadata = ("gpu-driver-provider=NVIDIA,rapids-runtime=SPARK"
                ",cuda-version={0},driver-version={1}".format(cuda_version, driver_version))

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        machine_type="n1-standard-32",
        master_accelerator=accelerator if configuration == "SINGLE" else None,
        worker_accelerator=accelerator,
        boot_disk_size="40GB",
        timeout_in_minutes=30)

    for machine_suffix in machine_suffixes:
      self.verify_spark_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
    # Only need to do this once
    self.verify_spark_job()
    # Only need to do this once
    self.verify_spark_job_sql()

if __name__ == "__main__":
  absltest.main()
