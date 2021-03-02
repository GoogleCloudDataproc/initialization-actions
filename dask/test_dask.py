import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class DaskTestCase(DataprocTestCase):
    COMPONENT = 'dask'
    INIT_ACTIONS = ['dask/dask.sh']

    DASK_YARN_TEST_SCRIPT = 'verify_dask_yarn.py'
    DASK_STANDALONE_TEST_SCRIPT = 'verify_dask_standalone.py'

    def verify_dask_yarn(self, name):
        self._run_dask_test_script(name, self.DASK_YARN_TEST_SCRIPT)

    def verify_dask_standalone(self, name):
        self._run_dask_test_script(name, self.DASK_STANDALONE_TEST_SCRIPT)

    def _run_dask_test_script(self, name, script):
        verify_cmd = "/opt/conda/default/bin/python {}".format(
            script)
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         script), name)
        self.assert_instance_command(name, verify_cmd)
        self.remove_test_script(script, name)        


    @parameterized.parameters(
        ("STANDARD", ["m", "w-0"], None),
        ("STANDARD", ["m", "w-0"], "yarn"),
        ("STANDARD", ["m"], "standalone"))
    def test_dask(self, configuration, instances, runtime):
        if self.getImageOs() == 'centos':
            self.skipTest("Not supported in CentOS-based images")

        if self.getImageVersion() <= pkg_resources.parse_version("1.4"):
            self.skipTest("Not supported in pre 1.5 images")

        metadata = None
        if runtime:
            metadata = "dask-runtime={}".format(runtime)

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type='n1-standard-2',
                           metadata=metadata,
                           timeout_in_minutes=20)
        
        for instance in instances:
            name = "{}-{}".format(self.getClusterName(), instance)

            if runtime is "standalone":
                self.verify_dask_standalone(name)
            else:
                self.verify_dask_yarn(name)

if __name__ == '__main__':
    absltest.main()
