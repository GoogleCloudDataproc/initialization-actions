"""
This module provides testing functionality of the Apache Ranger Init Action.
"""
import unittest
import os

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RangerTestCase(DataprocTestCase):
    COMPONENT = 'ranger'
    INIT_ACTIONS = ['solr/solr.sh', 'ranger/ranger.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_ranger.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        self.run_and_assert_command(
            'gcloud compute ssh {} --command="python {}"'.format(
                name, self.TEST_SCRIPT_FILE_NAME))

    @parameterized.expand(
        [
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("HA", "1.2", ["m-0"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.3", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_ranger(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            dataproc_version,
            metadata="ranger-port={},default-admin-password={}".format(
                6080, "dataproc2019"))
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    unittest.main()
