from integration_tests.dataproc_test_case import DataprocTestCase

from absl.testing import absltest
from absl.testing import parameterized


class AlluxioTestCase(DataprocTestCase):
  COMPONENT = "alluxio"
  INIT_ACTIONS = ["alluxio/alluxio.sh"]
  METADATA = "alluxio_root_ufs_uri={}".format("/opt/alluxio/underFSStorage/")

  def verify_instance(self, name):
    # Ping Alluxio master
    self.assert_instance_command(name, "alluxio fs leader")

  @parameterized.parameters(
      ("STANDARD", ["m"]),)
  def test_alluxio(self, configuration, machine_suffixes):
    if self.getImageOs() == 'centos':
      self.skipTest("Not supported in CentOS-based images")

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata="alluxio_root_ufs_uri={}/alluxio_ufs_root_{}".format(
            self.INIT_ACTIONS_REPO, self.random_str()),
        machine_type="e2-standard-4")
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))

  @parameterized.parameters(
      ("STANDARD", ["m"]),)
  def test_alluxio_with_presto(self, configuration, machine_suffixes):
    if self.getImageOs() == 'centos':
      self.skipTest("Not supported in CentOS-based images")

    self.createCluster(
        configuration,
        init_actions=self.INIT_ACTIONS,
        optional_components=["PRESTO"],
        metadata=self.METADATA,
        machine_type="e2-standard-4")
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))


if __name__ == "__main__":
  absltest.main()
