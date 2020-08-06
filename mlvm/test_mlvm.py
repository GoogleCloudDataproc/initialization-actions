import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class MLVMTestCase(DataprocTestCase):
  COMPONENT = "mlvm"
  INIT_ACTIONS = ["mlvm/mlvm.sh"]
  OPTIONAL_COMPONENTS = ["ANACONDA", "JUPYTER"]

  PYTHON_SCRIPT = "mlvm/scripts/python_packages.py"
  R_SCRIPT = "mlvm/scripts/r_packages.R"
  SPARK_BQ_SCRIPT = "mlvm/scripts/spark_bq.py"
  RAPIDS_SPARK_SCRIPT = "mlvm/scripts/verify_rapids_spark.py"
  RAPIDS_DASK_SCRIPT = "verify_rapids_dask.py"

  GPU_V100 = "type=nvidia-tesla-v100"

  def verify_python(self):
    self.assert_dataproc_job(
        self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                             self.PYTHON_SCRIPT))

  def verify_r(self):
    self.assert_dataproc_job(
        self.name, "spark-r", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                             self.R_SCRIPT))

  def verify_spark_bigquery_connector(self):
    self.assert_dataproc_job(
        self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                             self.SPARK_BQ_SCRIPT))

  def verify_gpu(self):
    for machine_suffix in ["m", "w-0", "w-1"]:
      self.assert_instance_command("{}-{}".format(self.name, machine_suffix),
                                   "nvidia-smi")

  def verify_rapids_spark(self):
    self.assert_dataproc_job(
        self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                             self.RAPIDS_SPARK_SCRIPT))

  def verify_rapids_dask(self):
    for machine_suffix in ["m", "w-0", "w-1"]:
      name = "{}-{}".format(self.name, machine_suffix)
      self.upload_test_file(
          os.path.join(
              os.path.dirname(os.path.abspath(__file__)), "scripts",
              self.RAPIDS_DASK_SCRIPT), name)

      verify_cmd = "/opt/conda/anaconda/envs/RAPIDS/bin/python {}".format(
          self.RAPIDS_DASK_SCRIPT)

      self.assert_instance_command(name, verify_cmd)

      self.remove_test_script(self.RAPIDS_DASK_SCRIPT, name)

  @parameterized.parameters(("STANDARD", None, None, None),
                            ("STANDARD", GPU_V100, "NVIDIA", None),
                            ("STANDARD", GPU_V100, "NVIDIA", "SPARK"),
                            ("STANDARD", GPU_V100, "NVIDIA", "DASK"))
  def test_mlvm(self, configuration, accelerator, gpu_provider, rapids_runtime):
    # Supported on Dataproc 1.5+
    if self.getImageVersion() < pkg_resources.parse_version("1.5"):
      return

    metadata = "init-actions-repo={}".format(self.INIT_ACTIONS_REPO)
    if accelerator:
      metadata += ",include-gpus=true,gpu-driver-provider={}".format(
          gpu_provider)
      if rapids_runtime:
        metadata += ",rapids-runtime={}".format(rapids_runtime)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        optional_components=self.OPTIONAL_COMPONENTS,
        machine_type="n1-standard-8",
        master_accelerator=accelerator,
        worker_accelerator=accelerator,
        timeout_in_minutes=60,
        metadata=metadata)

    self.verify_python()
    self.verify_r()
    self.verify_spark_bigquery_connector()

    if accelerator:
      self.verify_gpu()
      if rapids_runtime is "SPARK":
        self.verify_rapids_spark()
      elif rapids_runtime is "DASK":
        self.verify_rapids_dask()


if __name__ == "__main__":
  absltest.main()
