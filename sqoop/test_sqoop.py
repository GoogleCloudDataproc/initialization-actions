import os

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class SqoopTestCase(DataprocTestCase):
  COMPONENT = "sqoop"
  INIT_ACTIONS = ["sqoop/sqoop.sh"]
  TEST_DB_PATH = "test_sql_db_dump.gz"

  def verify_instance(self, name):
    self.assert_instance_command(name, "sqoop version")

  def verify_importing_to_hdfs(self, name):
    self.assert_instance_command(
        name, "sqoop import"
        " --connect jdbc:mysql://localhost:3306/employees"
        " --username root --table employees --m 1")
    self.assert_instance_command(
        name, "hadoop fs -cat /employees/part-m-*")

  def import_mysql_db(self, instance):
    self.upload_test_file(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), self.TEST_DB_PATH),
        instance)
    self.assert_instance_command(
        instance,
        "zcat {} | mysql -u root -proot-password".format(self.TEST_DB_PATH))

  @parameterized.parameters(
      ("SINGLE", ["m"]),
      ("STANDARD", ["m"]),
      ("HA", ["m-0", "m-1", "m-2"]),
  )
  def test_sqoop(self, configuration, machine_suffixes):
    self.createCluster(configuration, self.INIT_ACTIONS)
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))

  @parameterized.parameters(
      ("SINGLE", ["m"]),
      ("STANDARD", ["m"]),
      ("HA", ["m-0"]),
  )
  def test_sqoop_import_from_cloud_sql_to_hdfs(self, configuration,
                                               machine_suffixes):
    self.createCluster(configuration, self.INIT_ACTIONS)
    for machine_suffix in machine_suffixes:
      instance = "{}-{}".format(self.getClusterName(), machine_suffix)
      self.import_mysql_db(instance)
      self.verify_importing_to_hdfs(instance)


if __name__ == "__main__":
  absltest.main()
