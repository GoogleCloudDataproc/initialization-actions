"""
This module provides testing functionality of the Apache Tez Init Action.

Test logic:
1. On dataproc 1.1 or 1.2 cluster is created using Tez init action. Test script verify_tez.py is
executed on every master node. Test script run example Tez job and successful execution is expected.
2. On dataproc 1.3 Tez is pre-installed, so clusters are created without --initialization-actions
flag. Test script is executed on every master node.
"""
import os
import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


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

    def buildParameters():
        """Builds parameters from flags arguments passed to the test."""
        params = []
        print(FLAGS.params)
        if not FLAGS.params[0]:
            # Default parameters
            params = [
                ("SINGLE", ["m"]),
                ("STANDARD", ["m"]),
                ("HA", ["m-0", "m-1", "m-2"]),
            ]
        else:
            for param in FLAGS.params:
                (config, machine_suffixes) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                params.append((config, machine_suffixes))
        return params

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_tez(self, configuration, machine_suffixes):
        print(FLAGS.image_version)
        if FLAGS.image_version == "1.3":
            tez_classpath = "/etc/tez/conf:/usr/lib/tez/*:/usr/lib/tez/lib/*"
            self.createCluster(
                configuration,
                init_actions=[],
                properties="'hadoop-env:HADOOP_CLASSPATH={}:{}'".format(
                    "${HADOOP_CLASSPATH}", tez_classpath))
            for machine_suffix in machine_suffixes:
                self.verify_instance("{}-{}".format(self.getClusterName(),
                                                    machine_suffix))
        else:
            self.createCluster(configuration, self.INIT_ACTIONS)
            for machine_suffix in machine_suffixes:
                self.verify_instance("{}-{}".format(self.getClusterName(),
                                                    machine_suffix))


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
