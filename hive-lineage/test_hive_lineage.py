from absl.testing import absltest

from integration_tests.dataproc_test_case import DataprocTestCase

class HiveLineageTestCase(DataprocTestCase):
  INIT_ACTIONS = ["hive-lineage/hive-lineage.sh"]
  TEST_SCRIPT_FILE = "hive-lineage/hivetest.hive"

  def __submit_hive_job(self, cluster_name):
    self.assert_dataproc_job(
        cluster_name, 'hive', '--file={}/{}'.format(self.INIT_ACTIONS_REPO,
                                                    self.TEST_SCRIPT_FILE))
  def verify_cluster(self, name):
    self.__submit_hive_job(name)

  def test_hive_job_success(self, configuration):
    self.createCluster(configuration, self.INIT_ACTIONS, scopes='cloud-platform')
    self.verify_cluster(self.getClusterName())


if __name__ == "__main__":
  absltest.main()