import json
import logging

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class CloudSqlProxyTestCase(DataprocTestCase):
  COMPONENT = 'cloud-sql-proxy'
  INIT_ACTIONS = ['cloud-sql-proxy/cloud-sql-proxy.sh']
  TEST_SCRIPT_FILE_NAME = 'cloud-sql-proxy/pyspark_metastore_test.py'
  DB_NAME_MYSQL = None
  DB_NAME_POSTGRESQL = None
  DATABASE_VERSION_MYSQL = None
  DATABASE_VERSION_POSTGRESQL = None

  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    cls.PROJECT_METADATA = '{}:{}'.format(cls.PROJECT, cls.REGION)

  def setUp(self):
    super().setUp()
    self.create_mysql_instance()
    self.create_postgresql_instance()

  def tearDown(self):
    super().tearDown()
    self.delete_mysql_instance()
    self.delete_postgresql_instance()

  def create_mysql_instance(self):
    self.DB_NAME_MYSQL = 'test-cloud-mysql-{}-{}'.format(self.datetime_str(),
                                                         self.random_str())
    self.DATABASE_VERSION_MYSQL = 'MYSQL_8_0_31'
    create_cmd_fmt = 'gcloud sql instances create {}' \
                     ' --region {} --async --format=json --database-version={}'
    _, stdout, _ = self.assert_command(
      create_cmd_fmt.format(self.DB_NAME_MYSQL, self.REGION, self.DATABASE_VERSION))
    operation_id = json.loads(stdout.strip())['name']
    self.wait_cloud_sql_operation(operation_id)

  def create_postgresql_instance(self):
    self.DB_NAME_POSTGRESQL = 'test-cloud-postgresql-{}-{}'.format(self.datetime_str(),
                                                                   self.random_str())
    self.DATABASE_VERSION_POSTGRESQL = 'POSTGRES_14'
    create_cmd_fmt = 'gcloud sql instances create {}' \
                     ' --region {} --async --format=json --database-version={}'
    _, stdout, _ = self.assert_command(
      create_cmd_fmt.format(self.DB_NAME_MYSQL, self.REGION, self.DATABASE_VERSION))
    operation_id = json.loads(stdout.strip())['name']
    self.wait_cloud_sql_operation(operation_id)

  def delete_mysql_instance(self):
    ret_code, _, stderr = self.run_command(
      'gcloud sql instances delete {} --async'.format(self.DB_NAME_MYSQL))
    if ret_code != 0:
      logging.warning('Failed to delete Cloud SQL MySQL instance %s:\n%s',
                      self.DB_NAME_MYSQL, stderr)

  def delete_postgresql_instance(self):
    ret_code, _, stderr = self.run_command(
      'gcloud sql instances delete {} --async'.format(self.DB_NAME_POSTGRESQL))
    if ret_code != 0:
      logging.warning('Failed to delete Cloud SQL PostgreSQL instance %s:\n%s',
                      self.DB_NAME_POSTGRESQL, stderr)

  def test_mysql(self, configuration):
    metadata = 'hive-metastore-instance={}:{},hive-metastore-db=metastore'.format(self.PROJECT_METADATA,
                                                                                  self.DB_NAME_MYSQL)
    self.createCluster(
      configuration, self.INIT_ACTIONS, metadata=metadata, scopes='sql-admin')

    self.verify_cluster(self.getClusterName())

  def test_postgresql(self, configuration):
    metadata = 'hive-metastore-instance={}:{},hive-metastore-db=metastore'.format(self.PROJECT_METADATA,
                                                                                  self.DB_NAME_POSTGRESQL)
    self.createCluster(
      configuration, self.INIT_ACTIONS, metadata=metadata, scopes='sql-admin')

    self.verify_cluster(self.getClusterName())

  def wait_cloud_sql_operation(self, operation_id):
    self.assert_command(
      'gcloud sql operations wait {} --timeout=600'.format(operation_id))

  def verify_cluster(self, name):
    self.__submit_pyspark_job(name)

  def __submit_pyspark_job(self, cluster_name):
    self.assert_dataproc_job(
      cluster_name, 'pyspark', '{}/{}'.format(self.INIT_ACTIONS_REPO,
                                              self.TEST_SCRIPT_FILE_NAME))

  @parameterized.parameters(
    'SINGLE',
    'STANDARD',
    'HA',
  )
  def test_cloud_sql_proxy(self, configuration):
    self.test_mysql(configuration)
    self.test_postgresql(configuration)


if __name__ == '__main__':
  absltest.main()
