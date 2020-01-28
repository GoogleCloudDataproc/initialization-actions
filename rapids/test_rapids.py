import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RapidsTestCase(DataprocTestCase):
    COMPONENT = 'rapids'
    INIT_ACTIONS = ['gpu/install_gpu_driver.sh', 'rapids/rapids.sh']
    GPU_P100 = 'type=nvidia-tesla-p100'

    TEST_SCRIPT_FILE_NAME = 'verify_rapids.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self._run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def _run_test_script(self, name):
        verify_cmd = "/opt/conda/anaconda/envs/RAPIDS/bin/python {}".format(
            self.TEST_SCRIPT_FILE_NAME)
        self.assert_instance_command(name, verify_cmd)

    @parameterized.parameters(("STANDARD", ["m"], GPU_P100, GPU_P100, False),
                              ("STANDARD", ["m"], GPU_P100, GPU_P100, True))
    def test_rapids(self, configuration, machine_suffixes, master_accelerator,
                    worker_accelerator, dask_cuda_worker_on_master):
        # Init action supported on Dataproc 1.3+
        if self.getImageVersion() < pkg_resources.parse_version("1.3"):
            return

        metadata = 'gpu-driver-provider=NVIDIA'
        if not dask_cuda_worker_on_master:
            metadata += ',dask-cuda-worker-on-master=false'
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata=metadata,
                           beta=True,
                           master_accelerator=master_accelerator,
                           worker_accelerator=worker_accelerator,
                           optional_components='ANACONDA',
                           machine_type='n1-standard-2',
                           timeout_in_minutes=20)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
