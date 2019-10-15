import os
import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


class LivyTestCase(DataprocTestCase):
    COMPONENT = 'livy'
    INIT_ACTIONS = ['livy/livy.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_livy_running.py'

    def _verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self._run_python_test_file(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def _run_python_test_file(self, name):
        self.assert_instance_command(
            name, "sudo python3 {}".format(self.TEST_SCRIPT_FILE_NAME))

    def buildParameters():
        """Builds parameters from flags arguments passed to the test."""
        flags_parameters = FLAGS.params
        params = []
        if not flags_parameters[0]:
            # Default parameters
            params = [
                ("SINGLE",  ["m"]),
                ("STANDARD", ["m"]),
                ("HA", ["m-0", "m-1", "m-2"]),
            ]
        else:
            for param in flags_parameters:
                (config, machine_suffixes) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                params.append((config, machine_suffixes))
        return params

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_livy(self, configuration, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS)
        for machine_suffix in machine_suffixes:
            self._verify_instance("{}-{}".format(self.getClusterName(),
                                                 machine_suffix))


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
