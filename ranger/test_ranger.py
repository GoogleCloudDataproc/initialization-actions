"""This module provides testing functionality of the Apache Ranger Init Action.
"""
import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RangerTestCase(DataprocTestCase):
    COMPONENT = "ranger"
    INIT_ACTIONS = ["solr/solr.sh", "ranger/ranger.sh"]
    TEST_SCRIPT_FILE_NAME = "verify_ranger.py"

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME), name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        self.assert_instance_command(
            name, "python {}".format(self.TEST_SCRIPT_FILE_NAME))

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
        ("HA", ["m-0"]),
    )
    def test_ranger(self, configuration, machine_suffixes):
        if self.getImageOs() == 'centos':
            self.skipTest("Not supported in CentOS-based images")

        # Skip on 2.0+ version of Dataproc because it's not supported
        if self.getImageVersion() >= pkg_resources.parse_version("2.0"):
            self.skipTest("Not supported in 2.0+ images")

        # Init action supported on Dataproc 1.3+
        if self.getImageVersion() < pkg_resources.parse_version("1.3"):
            self.skipTest("Not supported in pre 1.3 images")

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata="ranger-port={},default-admin-password={}".format(
                6080, "dataproc2019"))
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == "__main__":
    absltest.main()
