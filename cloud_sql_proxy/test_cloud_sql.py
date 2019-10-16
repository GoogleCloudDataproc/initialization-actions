import json
import logging
import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
FLAGS(sys.argv)


class CloudSqlProxyTestCase(DataprocTestCase):
    COMPONENT = 'cloud-sql-proxy'
    INIT_ACTIONS = ['cloud-sql-proxy/cloud-sql-proxy.sh']
    TEST_SCRIPT_FILE_NAME = 'cloud-sql-proxy/pyspark_metastore_test.py'
    DB_NAME = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.PROJECT_METADATA = '{}:{}'.format(cls.PROJECT, cls.REGION)

    def setUp(self):
        super().setUp()
        self.DB_NAME = "test-cloud-sql-{}-{}".format(self.datetime_str(),
                                                     self.random_str())
        create_cmd_fmt = "gcloud sql instances create {}" \
            " --region {} --async --format=json"
        _, stdout, _ = self.assert_command(
            create_cmd_fmt.format(self.DB_NAME, self.REGION))
        operation_id = json.loads(stdout.strip())['name']
        self.wait_cloud_sql_operation(operation_id)

    def tearDown(self):
        super().tearDown()
        ret_code, _, stderr = self.run_command(
            'gcloud sql instances delete {} --async'.format(self.DB_NAME))
        if ret_code != 0:
            logging.warning("Failed to delete Cloud SQL instance %s:\n%s",
                            self.DB_NAME, stderr)

    def wait_cloud_sql_operation(self, operation_id):
        self.assert_command(
            'gcloud sql operations wait {} --timeout=600'.format(operation_id))

    def verify_cluster(self, name):
        self.__submit_pyspark_job(name)

    def __submit_pyspark_job(self, cluster_name):
        self.assert_dataproc_job(
            cluster_name, 'pyspark',
            '{}/{}'.format(self.INIT_ACTIONS_REPO, self.TEST_SCRIPT_FILE_NAME))

    @parameterized.expand(
        [
            ("SINGLE"),
            ("STANDARD"),
            ("HA"),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_cloud_sql_proxy(self, configuration):
        metadata = 'hive-metastore-instance={}:{}'.format(
            self.PROJECT_METADATA, self.DB_NAME)
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type="n1-standard-2",
                           metadata=metadata,
                           scopes='sql-admin')

        self.verify_cluster(self.getClusterName())


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
