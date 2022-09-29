import pkg_resources

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class ToreeTestCase(DataprocTestCase):
    COMPONENT = 'toree'
    INIT_ACTIONS = ['toree/toree.sh']

    @parameterized.parameters(
        ("SINGLE", "m", True),
        ("STANDARD", "m", True),
        ("HA", "m-0", True),
        ("SINGLE", "m", False),
    )
    def test_toree(self, configuration, machine_suffix, install_explicit):
        properties = "dataproc:jupyter.port=12345"
        if install_explicit:
            properties = "dataproc:pip.packages='toree==0.5.0',dataproc:jupyter.port=12345"
        optional_components = ["JUPYTER"]
        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            optional_components = ["ANACONDA", "JUPYTER"]
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            optional_components=optional_components,
            properties=properties)
        instance_name = self.getClusterName() + "-" + machine_suffix
        _, stdout, _ = self.assert_instance_command(
            instance_name, "curl http://127.0.0.1:12345/api/kernelspecs")
        self.assertIn("Apache Toree", stdout)


if __name__ == '__main__':
    absltest.main()
