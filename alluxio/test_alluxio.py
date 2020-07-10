from integration_tests.dataproc_test_case import DataprocTestCase

from absl.testing import absltest
from absl.testing import parameterized


class AlluxioTestCase(DataprocTestCase):
    COMPONENT = 'alluxio'
    INIT_ACTIONS = ['alluxio/alluxio.sh']
    METADATA = "alluxio_root_ufs_uri={}".format("/opt/alluxio/underFSStorage/")

    def verify_instance(self, name):
        # Ping Alluxio master
        self.assert_instance_command(name, "alluxio fs leader")

    @parameterized.parameters(
        ("STANDARD", ["m"]), )
    def test_alluxio(self, configuration, machine_suffixes):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata=self.METADATA,
            machine_type="e2-standard-4")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
        ("STANDARD", ["m"]), )
    def test_alluxio_with_presto(self, configuration, machine_suffixes):
        init_actions = ['presto/presto.sh'] + self.INIT_ACTIONS
        self.createCluster(
            configuration,
            init_actions,
            metadata=self.METADATA,
            machine_type="e2-standard-4")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
