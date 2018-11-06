import os
import unittest

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase


class HueTestCase(DataprocTestCase):
    COMPONENT = 'hue'
    INIT_ACTION = 'gs://dataproc-initialization-actions/hue/hue.sh'
    TEST_SCRIPT_FILE_NAME = 'verify_hue_running.py'

    def verify_instance(self, name):
        self.upload_test_file(name)
        self.__run_test_file(name)
        self.remove_test_script(name)

    def __run_test_file(self, name):
        cmd = 'gcloud compute ssh {} -- "python {} "'.format(
            name,
            self.TEST_SCRIPT_FILE_NAME
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        print("stdout", stdout)
        print("retcode", ret_code)
        print("stderr", stderr)
        self.assertEqual(ret_code, 0, "Failed to run test file. Error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.0", ["m"]),
        ("STANDARD", "1.0", ["m", "w-0"]),
        ("HA", "1.0", ["m-0","m-1","m-2", "w-0"]),
        ("SINGLE", "1.1", ["m"]),
        ("STANDARD", "1.1", ["m", "w-0"]),
        ("HA", "1.1", ["m-0","m-1","m-2", "w-0"]),
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m", "w-0"]),
        ("HA", "1.2", ["m-0","m-1","m-2", "w-0"]),
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.3", ["m", "w-0"]),
        ("HA", "1.3", ["m-0","m-1","m-2", "w-0"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hue(self, configuration, dataproc_version, machine_suffixes):
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
