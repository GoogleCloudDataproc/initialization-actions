import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class DaskTestCase(DataprocTestCase):
    COMPONENT = 'dask'
    INIT_ACTIONS = ['dask/dask.sh']

    DASK_TEST_SCRIPT = 'verify_dask.py'

    def verify_dask(self):
        name = "{}-{}".format(self.getClusterName(), "m")
        self._run_dask_test_script(name, self.DASK_TEST_SCRIPT)
        self.assert_instance_command(name, "set -u; echo $DASK_ENV")
        self.assert_instance_command(name, "set -u; echo $DASK_ENV_PACK")


    def _run_dask_test_script(self, name, script):
        verify_cmd = "sudo dask-python {}".format(
            script)
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         script), name)
        self.assert_instance_command(name, verify_cmd)
        self.remove_test_script(script, name)        

    @parameterized.parameters("SINGLE", "STANDARD")
    def test_dask(self, configuration):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type='n1-standard-2',
                           timeout_in_minutes=20)

        self.verify_dask()

if __name__ == '__main__':
    absltest.main()
