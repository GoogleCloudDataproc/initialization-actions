"""
This module provides testing functionality of the Apache Tez Init Action.

Test logic:
1. On dataproc 1.1 or 1.2 cluster is created using Tez init action. Test script verify_tez.py is
executed on every master node. Test script run example Tez job and successful execution is expected.
2. On dataproc 1.3 Tez is pre-installed, so clusters are created without --initialization-actions
flag. Test script is executed on every master node.
"""
import unittest
import os

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class TezTestCase(DataprocTestCase):
    COMPONENT = 'tez'
    INIT_ACTION = 'gs://dataproc-initialization-actions/tez/tez.sh'
    TEST_SCRIPT_FILE_NAME = 'verify_tez.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME
            ),
            name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "python {}"'.format(
                name,
                self.TEST_SCRIPT_FILE_NAME,
            )
        )
        self.assertEqual(ret_code, 0, "Failed to validate cluster. Last error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.1", ["m"]),
        ("STANDARD", "1.1", ["m"]),
        ("HA", "1.1", ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0", "m-1", "m-2"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_tez(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @parameterized.expand([
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.3", ["m"]),
        ("HA", "1.3", ["m-0", "m-1", "m-2"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_tez_on_image_version_1_3(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, init_action=None, dataproc_version=dataproc_version,
                           properties='\'hadoop-env:HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/etc/tez/conf:/usr/lib/tez/*:/usr/lib/tez/lib/*\'')
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )


if __name__ == '__main__':
    unittest.main()
