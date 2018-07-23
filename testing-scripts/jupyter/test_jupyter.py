import os
import unittest

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase


class JupyterTestCase(DataprocTestCase):
    COMPONENT = 'jupyter'
    INIT_ACTION = 'gs://polidea-dataproc-utils/jupyter/init-actions/6/jupyter/jupyter.sh'
    METADATA = "JUPYTER_AUTH_TOKEN=abc123"
    TEST_SCRIPT_FILE_NAME = 'verify_jupyter_running.py'

    def verify_instance(self, name, expected_version):
        self.upload_test_file(name)
        self.__run_test_file(expected_version, name)
        self.remove_test_script(name)

    def __run_test_file(self, expected_version, name):
        cmd = 'gcloud compute ssh {} -- "python {} {}"'.format(
            name,
            self.TEST_SCRIPT_FILE_NAME,
            expected_version
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        self.assertEqual(ret_code, 0, "Failed to run test file. Error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.0", ["m"], "5.0.0"),
        ("STANDARD", "1.0", ["m"], "5.0.0"),
        ("HA", "1.0", ["m-0", "m-1", "m-2"], "5.0.0"),
        ("SINGLE", "1.1", ["m"], "5.0.0"),
        ("STANDARD", "1.1", ["m"], "5.0.0"),
        ("HA", "1.1", ["m-0", "m-1", "m-2"], "5.0.0"),
        ("SINGLE", "1.2", ["m"], "5.4.1"),
        ("STANDARD", "1.2", ["m"], "5.4.1"),
        ("HA", "1.2", ["m-0", "m-1", "m-2"], "5.4.1"),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_jupyter(self, configuration, dataproc_version, machine_suffixes, expected_version):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version, self.METADATA)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                ),
                expected_version
            )


if __name__ == '__main__':
    unittest.main()
