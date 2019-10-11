"""
This module provides testing functionality of the Apache Solr Init Action.
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


class SolrTestCase(DataprocTestCase):
    COMPONENT = 'solr'
    INIT_ACTIONS = ['solr/solr.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_solr.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        self.assert_instance_command(
            name, "python3 {}".format(self.TEST_SCRIPT_FILE_NAME))

    def buildParameters():
        """Builds parameters from flags arguments passed to the test."""
        params = []
        print(FLAGS.params)
        if not FLAGS.params[0]:
            # Default parameters
            params = [
                ("SINGLE", "1.2", ["m"]),
                ("STANDARD", "1.2", ["m"]),
                ("HA", "1.2", ["m-0"]),
                ("SINGLE", "1.3", ["m"]),
                ("STANDARD", "1.3", ["m"]),
                ("HA", "1.3", ["m-0"])
            ]
        else:
            for param in FLAGS.params:
                (config, version, machine_suffixes) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                params.append((config, version, machine_suffixes))
        print(params)
        return params

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_solr(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
