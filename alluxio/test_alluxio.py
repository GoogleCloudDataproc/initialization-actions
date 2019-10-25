from integration_tests.dataproc_test_case import DataprocTestCase

from absl.testing import absltest
from absl.testing import parameterized


class AlluxioTestCase(DataprocTestCase):
    COMPONENT = 'alluxio'
    INIT_ACTIONS = ['alluxio/alluxio.sh']

    def verify_instance(self, name):
        # Ping Alluxio master
        self.assert_instance_command(name, "alluxio fs leader")

    @parameterized.parameters(
        ("STANDARD", ["m"]), )
    def test_alluxio(self, configuration, machine_suffixes):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata="alluxio_root_ufs_uri={}/alluxio_ufs_root_{}".format(
                self.INIT_ACTIONS_REPO, self.random_str()),
            timeout_in_minutes=30,
            machine_type="n1-standard-4")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
