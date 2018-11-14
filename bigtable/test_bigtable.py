"""
This module provides testing functionality of the BigTable Init Action.

Test logic:
1. Create test table and fill it with some data by injecting commands into hbase shell.
2. Validate from local station that BigTable has test table created with right data.

Note:
    Test REQUIRES cbt tool installed which provides CLI access to BigTable instances.
    See: https://cloud.google.com/bigtable/docs/cbt-overview
"""
import unittest
import random
import os

from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase


class BigTableTestCase(DataprocTestCase):
    COMPONENT = 'bigtable'
    INIT_ACTION = 'gs://dataproc-initialization-actions/bigtable/bigtable.sh'
    TEST_SCRIPT_FILE_NAME = "run_hbase_commands.py"

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.metadata = None
        self.db_name = None
        self.zone = None

    def setUp(self):
        super().setUp()
        self.db_name = "test-{}-db".format(random.randint(1, 10000))
        _, zone, _ = self.run_command("gcloud config get-value compute/zone")
        self.zone = zone.strip()
        _, project, _ = self.run_command("gcloud config get-value project")
        project = project.strip()
        self.metadata = "bigtable-instance={},bigtable-project={}"\
            .format(self.db_name, project)

        ret_code, stdout, stderr = self.run_command(
            'gcloud beta bigtable instances create {} --cluster {} --cluster-zone {} '
            '--display-name={} --instance-type=DEVELOPMENT'
            .format(self.db_name, self.db_name, self.zone, self.db_name)
        )
        self.assertEqual(ret_code, 0, "Failed to create bigtable instance {}. Last error: {}"
                         .format(self.db_name, stderr))

    def tearDown(self):
        super().tearDown()
        ret_code, stdout, stderr = self.run_command(
            'gcloud beta bigtable instances delete {}'.format(self.db_name)
        )
        self.assertEqual(ret_code, 0, "Failed to delete bigtable instance {}. Last error: {}"
                         .format(self.db_name, stderr))

    def _run_hbase_shell(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "python {}"'.format(
                name,
                self.TEST_SCRIPT_FILE_NAME,
                )
            )
        self.assertEqual(ret_code, 0, "Failed to validate cluster. Last error: {}".format(stderr))

    def _validate_bigtable(self):
        ret_code, stdout, stderr = self.run_command(
            'cbt -instance {} count test-bigtable '.format(
                self.db_name
            )
        )
        self.assertEqual(ret_code, 0, "Failed to validate cluster. Last error: {}".format(stderr))
        self.assertEqual(int(float(stdout)), 4, "Failed to validate cluster. Last error: {}"
                         .format(stderr))

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME
            ),
            name)
        self._run_hbase_shell(name)
        self._validate_bigtable()

    """ Dataproc versions 1.0 and 1.1 are excluded from automatic testing.
    Hbase shell is not working properly on older Dataproc Clusters when
    admin commands are provided from text file.
    """
    @parameterized.expand([
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0"]),
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.3", ["m"]),
        ("HA", "1.3", ["m-0"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_bigtable(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version, metadata=self.metadata)

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )


if __name__ == '__main__':
    unittest.main()

