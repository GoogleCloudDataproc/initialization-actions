"""
This module provides testing functionality of the BigTable Init Action.

Test logic:
1. Create test table and fill it with some data by injecting commands into hbase shell.
2. Validate from local station that BigTable has test table created with right data.

Note:
    Test REQUIRES cbt tool installed which provides CLI access to BigTable instances.
    See: https://cloud.google.com/bigtable/docs/cbt-overview
"""
import argparse
import unittest
import random
import os
import logging
import sys
from absl import flags

from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

# parser = argparse.ArgumentParser()
# parser.add_argument('-p', '--parameter_list', type=list, nargs='+')
# parser.add_argument('unittest_args', type=list, nargs='*')
# # parser.add_argument('-p', '--parameter_list', action='append', nargs=3,
# #     metavar=('config', 'version', 'machine_suffixes'))
# args = parser.parse_args()

# parser = argparse.ArgumentParser()
# parser.add_argument('--parameter_list', action='append', nargs='+')
# parser.add_argument('--parameter_list', type=str)
# args = parser.parse_args()

# del_all_flags(flags.FLAGS)

FLAGS = flags.FLAGS


flags.DEFINE_multi_string('params', 'SINGLE 1.3 m', 'Configuration to test')
FLAGS(sys.argv)


class BigTableTestCase(DataprocTestCase):
    COMPONENT = 'bigtable'
    INIT_ACTIONS = ['bigtable/bigtable.sh']
    TEST_SCRIPT_FILE_NAME = "run_hbase_commands.py"

    def __init__(self, method_name='runTest'):
        super().__init__(method_name)
        self.metadata = None
        self.db_name = None
        self.zone = None

    def setUp(self):
        super().setUp()
        self.db_name = "test-bt-{}-{}".format(self.datetime_str(),
                                              self.random_str())
        _, zone, _ = self.run_command("gcloud config get-value compute/zone")
        self.zone = zone.strip()
        _, project, _ = self.run_command("gcloud config get-value project")
        project = project.strip()
        self.metadata = "bigtable-instance={},bigtable-project={}"\
            .format(self.db_name, project)

        self.assert_command(
            'gcloud bigtable instances create {}'
            ' --cluster {} --cluster-zone {}'
            ' --display-name={} --instance-type=DEVELOPMENT'.format(
                self.db_name, self.db_name, self.zone, self.db_name))

    def tearDown(self):
        super().tearDown()
        self.assert_command('gcloud bigtable instances delete {}'.format(
            self.db_name))

    def buildParameters():
        # print("Hello")
        # print(args.parameter_list)
        # parameters = [
        #     (parameters.split()[0], parameters.split()[1], [parameters.split()[2]])
        #     for _,_, parameters in args.parameter_list]
        flags_parameters = FLAGS.params
        print(flags_parameters)
        params = []
        for param in flags_parameters:
            (config, version, machine_suffixes) = param.split()
            machine_suffixes = (machine_suffixes.split(',')
                if ',' in machine_suffixes
                else [machine_suffixes])
            params.append((config, version, machine_suffixes))
        return params

    def _validate_bigtable(self):
        _, stdout, _ = self.assert_command(
            'cbt -instance {} count test-bigtable '.format(self.db_name))
        self.assertEqual(int(float(stdout)), 4,
                         "Invalid BigTable instance count")

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self.assert_instance_command(
            name, "python {}".format(self.TEST_SCRIPT_FILE_NAME))
        self._validate_bigtable()

    """ Dataproc versions 1.0 and 1.1 are excluded from automatic testing.
    Hbase shell is not working properly on older Dataproc Clusters when
    admin commands are provided from text file.
    """

    @parameterized.expand(
        buildParameters(),
        # [
        #     ("SINGLE", "1.2", ["m"]),
        #     ("STANDARD", "1.2", ["m"]),
        #     ("HA", "1.2", ["m-0"]),
        #     ("SINGLE", "1.3", ["m"]),
        #     ("STANDARD", "1.3", ["m"]),
        #     ("HA", "1.3", ["m-0"]),
        # ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name,
        skip_on_empty=True)
    def test_bigtable(self, configuration, dataproc_version, machine_suffixes):
        # print(PARAMETERS)
        # print(self.PARAMETERS)
        # for param in self.PARAMETERS:
        #     configuration, dataproc_version, machine_suffixes = param
        log = logging.getLogger( "BigTableTestCase.test_bigtable" )
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           metadata=self.metadata)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

def del_all_flags(FLAGS):
    flags_dict = FLAGS._flags()    
    keys_list = [keys for keys in flags_dict]    
    for keys in keys_list:
        FLAGS.__delattr__(keys)


if __name__ == '__main__':
    unittest.main(argv=FLAGS.params)
