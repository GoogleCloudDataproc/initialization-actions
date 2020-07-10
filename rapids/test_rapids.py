import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RapidsTestCase(DataprocTestCase):
    COMPONENT = 'rapids'
    INIT_ACTIONS = ['gpu/install_gpu_driver.sh', 'rapids/rapids.sh']

    GPU_P100 = 'type=nvidia-tesla-p100'

    DASK_TEST_SCRIPT_FILE_NAME = 'verify_rapids_dask.py'

    def verify_dask_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.DASK_TEST_SCRIPT_FILE_NAME), name)
        self._run_dask_test_script(name)
        self.remove_test_script(self.DASK_TEST_SCRIPT_FILE_NAME, name)

    def _run_dask_test_script(self, name):
        verify_cmd = "/opt/conda/anaconda/envs/RAPIDS/bin/python {}".format(
            self.DASK_TEST_SCRIPT_FILE_NAME)
        self.assert_instance_command(name, verify_cmd)

    def verify_spark_instance(self, name):
        self.assert_instance_command(name, "nvidia-smi")

    @parameterized.parameters(("STANDARD", ["m"], GPU_P100))
    def test_rapids_dask(self, configuration, machine_suffixes, accelerator):
        # Init action supported on Dataproc 1.3+
        if self.getImageVersion() < pkg_resources.parse_version("1.3"):
            return

        metadata = 'gpu-driver-provider=NVIDIA,rapids-runtime=DASK'
        master_accelerator = accelerator
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata=metadata,
                           master_accelerator=master_accelerator,
                           worker_accelerator=accelerator,
                           optional_components=['ANACONDA'],
                           machine_type='n1-standard-2',
                           timeout_in_minutes=20)

        for machine_suffix in machine_suffixes:
            self.verify_dask_instance("{}-{}".format(self.getClusterName(),
                                                     machine_suffix))

    @parameterized.parameters(("STANDARD", ["w-0"], GPU_P100))
    def test_rapids_spark(self, configuration, machine_suffixes, accelerator):
        # Init action supported on Dataproc 1.3+
        if self.getImageVersion() < pkg_resources.parse_version("1.3"):
            return

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata='gpu-driver-provider=NVIDIA,rapids-runtime=SPARK',
            machine_type='n1-standard-2',
            worker_accelerator=accelerator,
            timeout_in_minutes=20)
        for machine_suffix in machine_suffixes:
            self.verify_spark_instance("{}-{}".format(self.getClusterName(),
                                                      machine_suffix))


if __name__ == '__main__':
    absltest.main()
