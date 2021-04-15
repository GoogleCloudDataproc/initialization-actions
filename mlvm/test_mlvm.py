import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class MLVMTestCase(DataprocTestCase):
  COMPONENT = "mlvm"
  INIT_ACTIONS = ["mlvm/mlvm.sh"]
  OPTIONAL_COMPONENTS = ["JUPYTER"]

  # Submit using the jobs API, stored in GCS
  PYTHON_SCRIPT = "mlvm/scripts/python_packages.py"
  R_SCRIPT = "mlvm/scripts/r_packages.R"
  SPARK_BQ_SCRIPT = "mlvm/scripts/spark_bq.py"
  RAPIDS_SPARK_SCRIPT = "mlvm/scripts/verify_rapids_spark.py"

  # Copied from local directories to cluster, then ran
  RAPIDS_DASK_SCRIPT = "verify_rapids_dask.py"
  DASK_YARN_SCRIPT = "verify_dask_yarn.py"
  DASK_STANDALONE_SCRIPT = "verify_dask_standalone.py"

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

  def verify_dask(self, dask_runtime):
    if dask_runtime == "standalone":
      script = self.DASK_STANDALONE_SCRIPT
    else:
      script = self.DASK_YARN_SCRIPT

    name = "{}-m".format(self.getClusterName())
    verify_cmd = "/opt/conda/default/bin/python {}".format(script)
    self.upload_test_file(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "scripts", script),
        name)
    self.assert_instance_command(name, verify_cmd)
    self.remove_test_script(script, name)

  def verify_rapids_spark(self):
    self.assert_dataproc_job(
        self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                             self.RAPIDS_SPARK_SCRIPT))

  def verify_rapids_dask(self):
    name = "{}-m".format(self.name)
    self.upload_test_file(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "scripts",
            self.RAPIDS_DASK_SCRIPT), name)

    verify_cmd = "/opt/conda/default/bin/python {}".format(
        self.RAPIDS_DASK_SCRIPT)

    self.assert_instance_command(name, verify_cmd)
    self.remove_test_script(self.RAPIDS_DASK_SCRIPT, name)

  def verify_all(self):
    self.verify_python()
    self.verify_r()
    self.verify_spark_bigquery_connector()

  @parameterized.parameters(
      ("STANDARD", None),
      ("STANDARD", "yarn"),
      ("STANDARD", "standalone"),
  )
  def test_mlvm(self, configuration, dask_runtime):
    if self.getImageOs() == 'centos':
      self.skipTest("Not supported in CentOS-based images")

    # Supported on Dataproc 1.5+
    if self.getImageVersion() < pkg_resources.parse_version("1.5"):
      self.skipTest("Not supported in pre 1.5 images")

    if self.getImageVersion() < pkg_resources.parse_version("2.0"):
      self.OPTIONAL_COMPONENTS.append("ANACONDA")

    metadata = "init-actions-repo={}".format(self.INIT_ACTIONS_REPO)
    if dask_runtime:
      metadata += ",dask-runtime={}".format(dask_runtime)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        optional_components=self.OPTIONAL_COMPONENTS,
        machine_type="n1-standard-4",
        timeout_in_minutes=60,
        metadata=metadata)

    self.verify_all()
    self.verify_dask(dask_runtime)

  @parameterized.parameters(
      ("STANDARD", None, None),
      ("STANDARD", None, "SPARK"),
      ("STANDARD", "yarn", "DASK"),
      ("STANDARD", "standalone", "DASK"),
  )
  def test_mlvm_gpu(self, configuration, dask_runtime, rapids_runtime):
    if self.getImageOs() == 'centos':
      self.skipTest("Not supported in CentOS-based images")

    # Supported on Dataproc 1.5+
    if self.getImageVersion() < pkg_resources.parse_version("1.5"):
      self.skipTest("Not supported in pre 1.5 images")

    metadata = ("init-actions-repo={},include-gpus=true"
                ",gpu-driver-provider=NVIDIA").format(self.INIT_ACTIONS_REPO)

    if self.getImageVersion() < pkg_resources.parse_version("2.0"):
      cudnn_version = "7.6.5.32"
      cuda_version = "10.1"
      if rapids_runtime == "DASK":
        self.skipTest("RAPIDS with Dask not supported in pre 2.0 images.")
      else:
        self.OPTIONAL_COMPONENTS.append("ANACONDA")
    else:
      cudnn_version = "8.0.5.39"
      cuda_version = "11.0"

    metadata = ("init-actions-repo={},include-gpus=true"
                ",gpu-driver-provider=NVIDIA,"
                "cuda-version={},cudnn-version={}").format(
                    self.INIT_ACTIONS_REPO, cuda_version, cudnn_version)

    if dask_runtime:
      metadata += ",dask-runtime={}".format(dask_runtime)
    if rapids_runtime:
      metadata += ",rapids-runtime={}".format(rapids_runtime)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        optional_components=self.OPTIONAL_COMPONENTS,
        machine_type="n1-standard-4",
        master_accelerator="type=nvidia-tesla-t4",
        worker_accelerator="type=nvidia-tesla-t4",
        timeout_in_minutes=60,
        metadata=metadata)

    self.verify_all()

    self.verify_gpu()
    if rapids_runtime == "SPARK":
      self.verify_rapids_spark()
    elif rapids_runtime == "DASK":
      self.verify_rapids_dask()

if __name__ == "__main__":
  absltest.main()
