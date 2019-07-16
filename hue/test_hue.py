import os
import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HueTestCase(DataprocTestCase):
    COMPONENT = 'hue'
    INIT_ACTIONS = ['hue/hue.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_hue_running.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self.__run_test_file(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_file(self, name):
        self.assert_instance_command(
            name, "python3 {}".format(self.TEST_SCRIPT_FILE_NAME))

    @parameterized.expand(
        [
            # ("SINGLE", "1.1", ["m"]),
            # ("STANDARD", "1.1", ["m", "w-0"]),
            # ("HA", "1.1", ["m-0", "m-1", "m-2", "w-0"]),
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m", "w-0"]),
            ("HA", "1.2", ["m-0", "m-1", "m-2", "w-0"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m", "w-0"]),
            ("HA", "1.3", ["m-0", "m-1", "m-2", "w-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hue(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    unittest.main()
