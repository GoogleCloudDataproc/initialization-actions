import json
import logging

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


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

    @parameterized.parameters(
        "SINGLE",
        "STANDARD",
        "HA",
    )
    def test_cloud_sql_proxy(self, configuration):
        metadata = 'hive-metastore-instance={}:{}'.format(
            self.PROJECT_METADATA, self.DB_NAME)
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata=metadata,
            scopes='sql-admin')

        self.verify_cluster(self.getClusterName())


if __name__ == '__main__':
    absltest.main()
