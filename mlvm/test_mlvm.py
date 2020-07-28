import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class MLVMTestCase(DataprocTestCase):
    COMPONENT = "mlvm"
    INIT_ACTIONS = ["mlvm/mlvm.sh"]
    OPTIONAL_COMPONENTS = ["ANACONDA", "JUPYTER"]

    def createCluster(self, configuration, **config):
        config["optional_components"] = self.OPTIONAL_COMPONENTS
        config["scopes"] = "cloud-platform"
        config["timeout_in_minutes"] = 60
        config["machine_type"] = "n1-standard-8"
        
        connector_metadata = "spark-bigquery-connector-version=0.13.1-beta"
        metadata = config.get("metadata", "")
        if metadata:
            if "connector" not in metadata:
                metadata = "{},{}".format(metadata, connector_metadata)
        else:
            metadata = connector_metadata
        
        config["metadata"] = metadata

        super().createCluster(configuration, self.INIT_ACTIONS,
                              **config)

class ConnectorsTestCase(MLVMTestCase):
    COMPONENT = "mlvm-connectors"
    SPARK_BQ_CONNECTOR_VERSION = "0.13.1-beta"

    CONNECTORS_DIR = "/usr/local/share/google/dataproc/lib"
    SCALA_VERSION = "2.12"

    def verify_instances(self, cluster, instances, connector,
                         connector_version):
        for instance in instances:
            self._verify_instance("{}-{}".format(cluster, instance), connector,
                                  connector_version)

    def _verify_instance(self, instance, connector, connector_version):
        if connector == "spark-bigquery-connector":
            connector_jar = "spark-bigquery-with-dependencies_{}-{}.jar".format(
                self.SCALA_VERSION, connector_version)
        else:
            connector_jar = "{}-hadoop2-{}.jar".format(connector,
                                                       connector_version)

        self.assert_instance_command(
            instance, "test -f {}/{}".format(self.CONNECTORS_DIR,
                                             connector_jar))
        self.assert_instance_command(
            instance, "test -L {}/{}.jar".format(self.CONNECTORS_DIR,
                                                 connector))


    @parameterized.parameters(("SINGLE", ["m"]),
                              ("HA", ["m-0", "m-1", "m-2", "w-0", "w-1"]))
    def test_spark_bq_connector_version(self, configuration, instances):
        # Init action supported on Dataproc 1.5+
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return
        
        self.createCluster(
            configuration,
            metadata="spark-bigquery-connector-version={}".format(
                self.SPARK_BQ_CONNECTOR_VERSION))
        self.verify_instances(self.getClusterName(), instances,
                              "spark-bigquery-connector",
                              self.SPARK_BQ_CONNECTOR_VERSION)


class NvidiaGpuDriverTestCase(MLVMTestCase):
    COMPONENT = "mlvm-gpu"
    GPU_V100 = 'type=nvidia-tesla-v100'

    def verify_instance(self, name):
        self.assert_instance_command(name, "nvidia-smi")

    def verify_instance_gpu_agent(self, name):
        self.assert_instance_command(
            name, "systemctl status gpu-utilization-agent.service")

    @parameterized.parameters(
        ("STANDARD", ["m"], GPU_V100, None, None),
        ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, "OS"),
        ("STANDARD", ["w-0", "w-1"], None, GPU_V100, "NVIDIA"),
    )
    def test_install_gpu_default_agent(self, configuration, machine_suffixes,
                                       master_accelerator, worker_accelerator,
                                       driver_provider):
        # Init action supported on Dataproc 1.5+
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return

        metadata = "include-gpus=true"
        if driver_provider is not None:
            metadata += ",gpu-driver-provider={}".format(driver_provider)
        self.createCluster(configuration,
                           master_accelerator=master_accelerator,
                           worker_accelerator=worker_accelerator,
                           metadata=metadata)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
        ("STANDARD", ["w-0", "w-1"], None, GPU_V100, None),
        ("STANDARD", ["m"], GPU_V100, None, "OS"),
        ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, "NVIDIA"),
    )
    def test_install_gpu_without_agent(self, configuration, machine_suffixes,
                                       master_accelerator, worker_accelerator,
                                       driver_provider):
        # Init action supported on Dataproc 1.5+
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return

        metadata = 'include-gpus=true,install-gpu-agent=false'
        if driver_provider is not None:
            metadata += ",gpu-driver-provider={}".format(driver_provider)
        self.createCluster(configuration,
                           master_accelerator=master_accelerator,
                           worker_accelerator=worker_accelerator,
                           metadata=metadata)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
        ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, None),
        ("STANDARD", ["w-0", "w-1"], None, GPU_V100, "OS"),
        ("STANDARD", ["m"], GPU_V100, None, "NVIDIA"),
    )
    def test_install_gpu_with_agent(self, configuration, machine_suffixes,
                                    master_accelerator, worker_accelerator,
                                    driver_provider):
        # Init action supported on Dataproc 1.5+
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return

        metadata = 'include-gpus=true,install-gpu-agent=true'
        if driver_provider is not None:
            metadata += ",gpu-driver-provider={}".format(driver_provider)
        self.createCluster(
            configuration,
            master_accelerator=master_accelerator,
            worker_accelerator=worker_accelerator,
            metadata=metadata
        )
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
            self.verify_instance_gpu_agent("{}-{}".format(
                self.getClusterName(), machine_suffix))


class RapidsTestCase(MLVMTestCase):
    COMPONENT = "mlvm-rapids"
    GPU_P100 = 'type=nvidia-tesla-p100'

    DASK_TEST_SCRIPT_FILE_NAME = 'verify_rapids_dask.py'
    SPARK_TEST_SCRIPT_FILE_NAME = 'mlvm/scripts/verify_rapids_spark.py'

    def verify_dask_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "scripts",
                         self.DASK_TEST_SCRIPT_FILE_NAME), name)
        self._run_dask_test_script(name)
        self.remove_test_script(self.DASK_TEST_SCRIPT_FILE_NAME, name)

    def _run_dask_test_script(self, name):
        verify_cmd = "/opt/conda/anaconda/envs/RAPIDS/bin/python {}".format(
            self.DASK_TEST_SCRIPT_FILE_NAME)
        self.assert_instance_command(name, verify_cmd)

    def verify_spark2_instance(self, name):
        self.assert_instance_command(name, "nvidia-smi")

    def verify_spark3_job(self):
        self.assert_dataproc_job(
            self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                                        self.SPARK_TEST_SCRIPT_FILE_NAME))

    @parameterized.parameters(("STANDARD", ["m", "w-0"], GPU_P100))
    def test_rapids_dask(self, configuration, machine_suffixes, accelerator):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return

        metadata = 'include-gpus=true,gpu-driver-provider=NVIDIA,rapids-runtime=DASK'
        master_accelerator = accelerator
        self.createCluster(configuration,
                           metadata=metadata,
                           master_accelerator=master_accelerator,
                           worker_accelerator=accelerator,
                           timeout_in_minutes=70)

        for machine_suffix in machine_suffixes:
            self.verify_dask_instance("{}-{}".format(self.getClusterName(),
                                                     machine_suffix))

    @parameterized.parameters(("STANDARD", ["w-0"], GPU_P100))
    def test_rapids_spark(self, configuration, machine_suffixes, accelerator):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
                  return

        self.createCluster(
            configuration,
            metadata='include-gpus=true,gpu-driver-provider=NVIDIA,rapids-runtime=SPARK',
            worker_accelerator=accelerator,
            timeout_in_minutes=30)
        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            for machine_suffix in machine_suffixes:
                self.verify_spark2_instance("{}-{}".format(self.getClusterName(),
                                                           machine_suffix))
        else:
            self.verify_spark3_job()


class PythonTestCase(MLVMTestCase):
    COMPONENT = "mlvm-python"
    PYTHON_SCRIPT = "mlvm/scripts/python_packages.py"
          
    @parameterized.parameters(("STANDARD",))
    def test_python(self, configuration):
        # Init action supported on Dataproc 1.5+
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return

        self.createCluster(configuration)

        self.assert_dataproc_job(
            self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                                self.PYTHON_SCRIPT))


class RTestCase(MLVMTestCase):
    COMPONENT = "mlvm-r"
    R_SCRIPT = "mlvm/scripts/r_packages.R"

    @parameterized.parameters(("STANDARD",))
    def test_r(self, configuration):
        # Init action supported on Dataproc 1.5+
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return

        self.createCluster(configuration)

        self.assert_dataproc_job(
            self.name, "spark-r", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                                self.R_SCRIPT))

if __name__ == "__main__":
    absltest.main()
