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

    def verify_dask(self, name, script):
        self._run_dask_test_script(name, script)
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

    @parameterized.parameters(
        ("STANDARD", ["m"], None),
        ("STANDARD", ["m"], "yarn"),
        ("STANDARD", ["m"], "standalone"))
    def test_dask(self, configuration, instances, runtime):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return
        
        metadata = None
        if runtime:
            metadata = "dask-runtime={}".format(runtime)

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type='n1-standard-2',
                           metadata=metadata,
                           timeout_in_minutes=20)
        
        script = (self.DASK_STANDALONE_TEST_SCRIPT if runtime is "standalone" 
                  else self.DASK_YARN_TEST_SCRIPT)

        for instance in instances:
            self.verify_dask("{}-{}".format(self.getClusterName(), instance), 
                             script)

if __name__ == '__main__':
    absltest.main()
