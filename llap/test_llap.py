
"""
This module provides testing functionality of the LLAP Init Action.
Test logic:
1. Create test table and fill it with some data by running commands in beeline.
2. Validate from local station that Hive table has test table created with right data.
Note:
    Test REQUIRES cbt tool installed which provides CLI access to BigTable instances.
    See: https://cloud.google.com/bigtable/docs/cbt-overview
"""

import os
import pkg_resources

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class LLAPTestCase(DataprocTestCase):
    COMPONENT = 'llap'
    INIT_ACTIONS = ['llap/llap.sh']
    TEST_SCRIPT_FILE_NAME = "run_hive_commands.py"

    def __init__(self, method_name='runTest'):
        super().__init__(method_name)
        self.cluster_name="llap"
        self.region="us-central1"
        self.bigtable_zone = "us-central1-f"
        self.workers=3
        self.project=""

    def setUp(self):
        super().setUp()

        self.assert_command(
        	'gcloud beta dataproc clusters create {} '
        	'--enable-component-gateway --region {} --zone {} '
        	'--master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers {}' 
        	'--worker-machine-type n1-standard-8 --worker-boot-disk-size 500' 
        	'--image-version 2.0-debian10 --optional-components ZOOKEEPER' 
        	'--initialization-actions \'gs://jtaras-init-actions/llap/llap.sh\' '
        	'--project {}'.format(
                self.cluster_name, self.region, self.zone, self.workers, self.project))


    def tearDown(self):
        super().tearDown()
        self.assert_command('gcloud beta dataproc clusters delete {} --region {}'.format(
            self.cluster_name, self.region))

    ##def _validate_llap(self):
    ##    _, stdout, _ = self.assert_command(
    ##        'cbt -instance {} count test-bigtable '.format(self.db_name))
    ##    self.assertEqual(
    ##        int(float(stdout)), 4, "Invalid BigTable instance count")

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME), name)
        self.assert_instance_command(
            name, "python {}".format(self.TEST_SCRIPT_FILE_NAME))
        self._validate_bigtable()

    ##@parameterized.parameters(
    ##    ("SINGLE", ["m"]),
    ##    ("STANDARD", ["m"]),
    ##   ("HA", ["m-0"]),
   ##)
    ##def test_llap(self, configuration, machine_suffixes):
    ##    if self.getImageOs() == 'centos':
    ##        self.skipTest("Not supported in CentOS-based images")

    ##    if self.getImageVersion() >= pkg_resources.parse_version("2.0"):
    ##        self.skipTest("Not supported in the 2.0+ images")

    ##    self.createCluster(
    ##        configuration, self.INIT_ACTIONS, metadata=self.metadata)

    ##    for machine_suffix in machine_suffixes:
    ##        self.verify_instance("{}-{}".format(self.getClusterName(),
     ##                                           machine_suffix))


if __name__ == '__main__':
    absltest.main()