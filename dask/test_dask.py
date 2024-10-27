import os
import time

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

    def verify_dask_standalone(self, name, master_hostname):
        script=self.DASK_STANDALONE_TEST_SCRIPT
        verify_cmd = "/opt/conda/miniconda3/envs/dask/bin/python {} {}".format(
            script,
            master_hostname
        )
        abspath=os.path.join(os.path.dirname(os.path.abspath(__file__)),script)
        self.upload_test_file(abspath, name)
        self.assert_instance_command(name, verify_cmd)
        self.remove_test_script(script, name)

    def _run_dask_test_script(self, name, script):
        verify_cmd = "/opt/conda/miniconda3/envs/dask/bin/python {}".format(
            script)
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         script), name)
        command_asserted=0
        for try_number in range(0, 7):
          try:
            self.assert_instance_command(name, verify_cmd)
            command_asserted=1
            break
          except:
            time.sleep(2**try_number)
        if command_asserted == 0:
          raise Exception("Unable to assert instance command [{}]".format(verify_cmd))

        self.remove_test_script(script, name)        


    @parameterized.parameters(
        ("STANDARD", ["m", "w-0"], "yarn"),
        ("STANDARD", ["m"], "standalone"),
        ("KERBEROS", ["m"], "standalone"),
    )
    def test_dask(self, configuration, instances, runtime):

        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            self.skipTest("Not supported in pre-2.0 images")

        metadata = None
        if runtime:
            metadata = "dask-runtime={}".format(runtime)

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type='n1-standard-16',
                           metadata=metadata,
                           timeout_in_minutes=20)

        if configuration == 'HA':
            master_hostname = self.getClusterName() + '-m-0'
        else:
            master_hostname = self.getClusterName() + '-m'

        for instance in instances:
            name = "{}-{}".format(self.getClusterName(), instance)

            if runtime == "standalone":
                self.verify_dask_standalone(name, master_hostname)
            else:
                self.verify_dask_yarn(name)

if __name__ == '__main__':
    absltest.main()
