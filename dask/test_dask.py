import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class DaskTestCase(DataprocTestCase):
    COMPONENT = 'dask'
    INIT_ACTIONS = ['gpu/install_gpu_driver.sh', 'dask/dask.sh']

    GPU_P100 = 'type=nvidia-tesla-p100'

    DASK_TEST_SCRIPT = 'verify_dask.py'
    DASK_RAPIDS_TEST_SCRIPT = 'verify_dask_rapids.py'

    def verify_dask(self, script):
        name = "{}-{}".format(self.getClusterName(), "m")
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         script), name)
        self._run_dask_test_script(name, script)
        self.remove_test_script(script, name)

    def _run_dask_test_script(self, name, script):
        verify_cmd = "sudo dask-python {}".format(
            script)
        self.assert_instance_command(name, verify_cmd)        

    @parameterized.parameters(("STANDARD",))
    def test_dask(self, configuration):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type='n1-standard-2',
                           timeout_in_minutes=45)

        self.verify_dask(self.DASK_TEST_SCRIPT)

    @parameterized.parameters(("STANDARD",))
    def test_dask_rapids(self, configuration):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return    
        
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata="include-rapids=true,gpu-driver-provider=NVIDIA",
            machine_type='n1-standard-2',
            master_accelerator=self.GPU_P100,
            worker_accelerator=self.GPU_P100,
            timeout_in_minutes=45)
        
        self.verify_dask(self.DASK_TEST_SCRIPT)
        self.verify_dask(self.DASK_RAPIDS_TEST_SCRIPT)

if __name__ == '__main__':
    absltest.main()
