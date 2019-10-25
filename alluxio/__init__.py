import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class AlluxioTestCase(DataprocTestCase):
    COMPONENT = 'alluxio'
    INIT_ACTIONS = ['alluxio/alluxio.sh']

    def verify_instance(self, name):
        # Ping Alluxio master
        self.assert_instance_command(name, "alluxio fs leader")

    @parameterized.expand(
        [
            ("STANDARD", "1.2", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("STANDARD", "1.4", ["m"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_alluxio(self, configuration, dataproc_version,
                         machine_suffixes):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           metadata="alluxio_root_ufs_uri=gs://alluxio-public/dataproc",
                           timeout_in_minutes=30,
                           machine_type="n1-standard-2")
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix))


if __name__ == '__main__':
    unittest.main()
