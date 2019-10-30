import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase


class JupyterTestCase(DataprocTestCase):
    OPTIONAL_COMPONENTS = 'ANACONDA,JUPYTER'
    COMPONENT = 'jupyter_sparkmonitor'
    INIT_ACTIONS = ['jupyter_sparkmonitor/sparkmonitor.sh']

    def verify_instance(self, name, jupyter_port):
        verify_cmd = "curl {} -L {}:{} | grep 'Jupyter Notebook'".format(
            "--retry 10 --retry-delay 10 --retry-connrefused", name,
            jupyter_port)
        self.assert_instance_command(name, verify_cmd)

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
    )
    def test_sparkmonitor(self, configuration, machine_suffixes):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           optional_components=self.OPTIONAL_COMPONENTS,
                           timeout_in_minutes=15,
                           machine_type="n1-standard-2")

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix), "8123")


if __name__ == '__main__':
    absltest.main()