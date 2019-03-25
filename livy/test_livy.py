import os
import unittest

from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase


class LivyTestCase(DataprocTestCase):
    COMPONENT = 'livy'
    TEST_SCRIPT_FILE_NAME = 'verify_livy_running.py'
    INIT_ACTION = 'gs://dataproc-initialization-actions/livy/livy.sh'
    INIT_ACTION_FOR_STANDARD = '\'gs://dataproc-initialization-actions/zookeeper/zookeeper.sh\',\'gs://dataproc-initialization-actions/livy/livy.sh\''

    def _verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME
            ),
            name)
        self._run_python_test_file(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def _run_python_test_file(self, name):
        cmd = 'gcloud compute ssh {} -- "sudo python3 {}"'.format(
            name,
            self.TEST_SCRIPT_FILE_NAME
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        self.assertEqual(ret_code, 0, "Failed to run test file. Error: {}".format(stderr))

    @parameterized.expand([
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
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_livy(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version)
        for machine_suffix in machine_suffixes:
            self._verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix

                )
            )


if __name__ == '__main__':
    unittest.main()
