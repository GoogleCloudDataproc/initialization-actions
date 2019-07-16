import os
import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


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

    @parameterized.expand(
        [
            ("SINGLE", "1.0", ["m"]),
            ("STANDARD", "1.0", ["m"]),
            ("HA", "1.0", ["m-0", "m-1", "m-2"]),
            ("SINGLE", "1.1", ["m"]),
            ("STANDARD", "1.1", ["m"]),
            ("HA", "1.1", ["m-0", "m-1", "m-2"]),
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("HA", "1.2", ["m-0", "m-1", "m-2"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.3", ["m-0", "m-1", "m-2"]),
            ("SINGLE", "1.4", ["m"]),
            ("STANDARD", "1.4", ["m"]),
            ("HA", "1.4", ["m-0", "m-1", "m-2"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_livy(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS, dataproc_version)
        for machine_suffix in machine_suffixes:
            self._verify_instance("{}-{}".format(self.getClusterName(),
                                                 machine_suffix))


if __name__ == '__main__':
    unittest.main()
