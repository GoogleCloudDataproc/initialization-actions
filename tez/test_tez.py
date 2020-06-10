"""
This module provides testing functionality of the Apache Tez Init Action.

Test logic:
1. On dataproc 1.1 or 1.2 cluster is created using Tez init action. Test script verify_tez.py is
executed on every master node. Test script run example Tez job and successful execution is expected.
2. On dataproc 1.3 Tez is pre-installed, so clusters are created without --initialization-actions
flag. Test script is executed on every master node.
"""
import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class TezTestCase(DataprocTestCase):
    COMPONENT = 'tez'
    INIT_ACTIONS = ['tez/tez.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_tez.py'

    def verify_instance(self, name):
        test_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.TEST_SCRIPT_FILE_NAME)
        self.upload_test_file(test_script_path, name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        self.assert_instance_command(
            name, "python {}".format(self.TEST_SCRIPT_FILE_NAME))

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
        ("HA", ["m-0", "m-1", "m-2"]),
    )
    def test_tez(self, configuration, machine_suffixes):
        # Skip on 2.0+ version of Dataproc because it's not supported
        if self.getImageVersion() >= pkg_resources.parse_version("2.0"):
            return

        init_actions = self.INIT_ACTIONS
        properties = None
        if self.getImageVersion() >= pkg_resources.parse_version("1.3"):
            # Remove init action - in Dataproc 1.3+ Tez installed by default
            init_actions = []
            tez_classpath = "/etc/tez/conf:/usr/lib/tez/*:/usr/lib/tez/lib/*"
            properties = "'hadoop-env:HADOOP_CLASSPATH={}:{}'".format(
                "${HADOOP_CLASSPATH}", tez_classpath)

        self.createCluster(configuration, init_actions, properties=properties)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
