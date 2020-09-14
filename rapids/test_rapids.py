import os
import pathlib

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RapidsTestCase(DataprocTestCase):
    COMPONENT = 'rapids'
    INIT_ACTIONS = ['gpu/install_gpu_driver.sh', 'rapids/rapids.sh']

    GPU_P100 = 'type=nvidia-tesla-p100'

    DASK_RAPIDS_TEST_SCRIPT_FILE_NAME = 'verify_rapids_dask.py'
    DASK_YARN_TEST_SCRIPT_FILE_NAME = 'dask/verify_dask_yarn.py'
    DASK_STANDALONE_TEST_SCRIPT_FILE_NAME = 'dask/verify_dask_standalone.py'
    SPARK_TEST_SCRIPT_FILE_NAME = 'verify_rapids_spark.py'

    def verify_dask_instance(self, name, dask_runtime):
        parent_dir = os.path.dirname(os.path.abspath(__file__))
        grandparent_dir = os.path.dirname(parent_dir)

        # Verify RAPIDS
        self._run_dask_test_script(os.path.join(parent_dir, self.DASK_RAPIDS_TEST_SCRIPT_FILE_NAME))
      
        # Verify Dask installation integrity
        if dask_runtime is "standalone":
            runtime_test_script = self.DASK_STANDALONE_TEST_SCRIPT_FILE_NAME
        else:
            runtime_test_script = self.DASK_YARN_TEST_SCRIPT_FILE_NAME
        
        self._run_dask_test_script(os.path.join(grandparent_dir, runtime_test_script))

    def _run_dask_test_script(self, script, name):
        self.upload_test_file(script, name)
        verify_cmd = "/opt/conda/miniconda3/bin/python {}".format(
            script)
        self.assert_instance_command(name, verify_cmd)
        self.remove_test_script(script, name)

    def verify_spark_instance(self, name):
        self.assert_instance_command(name, "nvidia-smi")

    def verify_spark_job(self):
        self.assert_dataproc_job(
            self.name, "pyspark", "{}/rapids/{}".format(self.INIT_ACTIONS_REPO,
                                                        self.SPARK_TEST_SCRIPT_FILE_NAME))

    @parameterized.parameters(
        ("STANDARD", ["m", "w-0"], GPU_P100, None),
        ("STANDARD", ["m", "w-0"], GPU_P100, "yarn"),
        ("STANDARD", ["m"], GPU_P100, "standalone"))
    def test_rapids_dask(self, configuration, machine_suffixes, accelerator, dask_runtime):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            self.skipTest("Not supported in pre 1.5 images")

        init_actions = self.INIT_ACTIONS
        init_actions.insert(1, "dask/dask.sh")

        metadata='gpu-driver-provider=NVIDIA,rapids-runtime=DASK'
        if dask_runtime:
            metadata+=',dask-runtime={}'.format(dask_runtime)

        self.createCluster(configuration,
                           init_actions,
                           metadata=metadata,
                           machine_type='n1-standard-2',
                           master_accelerator=accelerator,
                           worker_accelerator=accelerator,
                           optional_components=['ANACONDA'],
                           timeout_in_minutes=60)

        for machine_suffix in machine_suffixes:
            self.verify_dask_instance("{}-{}".format(self.getClusterName(),
                                                     machine_suffix), 
                                      dask_runtime)

    @parameterized.parameters(("STANDARD", ["w-0"], GPU_P100))
    def test_rapids_spark(self, configuration, machine_suffixes, accelerator):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            self.skipTest("Not supported in pre 1.5 images")
        
        metadata = 'gpu-driver-provider=NVIDIA,rapids-runtime=SPARK'
        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            metadata += ",cuda-version=10.1"

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata=metadata,
            machine_type='n1-standard-2',
            worker_accelerator=accelerator,
            timeout_in_minutes=30)

        for machine_suffix in machine_suffixes:
            self.verify_spark_instance("{}-{}".format(self.getClusterName(),
                                                        machine_suffix))
        # Only need to do this once                                           
        self.verify_spark_job()


if __name__ == '__main__':
    absltest.main()
