import unittest
import os

from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase


class KnoxTestCase(DataprocTestCase):
    COMPONENT = 'knox'
    INIT_ACTION = 'gs://dataproc-initialization-actions/knox/knox.sh'
    TEST_SCRIPT_FILE_NAME = 'verify_knox_running.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME
            ),
            name)
        self.__run_command_on_cluster(name, 'yes | sudo apt-get install python3-pip')
        self.__run_command_on_cluster(name, 'sudo pip3 install requests')
        self.__run_test_file(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_file(self, name):
        cmd = 'gcloud compute ssh {} -- "python3 {} "'.format(
            name,
            self.TEST_SCRIPT_FILE_NAME
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        self.assertEqual(ret_code, 0, "Failed to run test file. Error: {}".format(stderr))

    def __run_command_on_cluster(self, name, command):
        cmd = 'gcloud compute ssh {} -- "{} "'.format(
            name,
            command
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        self.assertEqual(ret_code, 0, "Failed to run command. Error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.3", ["m"]),
        ("HA", "1.3", ["m-0", "m-1", "m-2"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_knox(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )


if __name__ == '__main__':
    unittest.main()
