import os
import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class OozieTestCase(DataprocTestCase):
    COMPONENT = 'oozie'
    INIT_ACTIONS = ['oozie/oozie.sh']
    TEST_SCRIPT_FILE_NAME = 'validate.sh'

    def verify_instance(self, name):
        test_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.TEST_SCRIPT_FILE_NAME)
        self.upload_test_file(test_script_path, name)
        self.__run_test_file(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_file(self, name):
        self.assert_instance_command(
            name, "bash {}".format(self.TEST_SCRIPT_FILE_NAME))

    @parameterized.expand(
        [
            ("SINGLE", "1.1", ["m"]),
            ("SINGLE", "1.2", ["m"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.1", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.1", ["m-0", "m-1", "m-2"]),
            ("HA", "1.2", ["m-0", "m-1", "m-2"]),
            ("HA", "1.3", ["m-0", "m-1", "m-2"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_oozie(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           machine_type="n1-standard-4",
                           boot_disk_size="200GB")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    unittest.main()
