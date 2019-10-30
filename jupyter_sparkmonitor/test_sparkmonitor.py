import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase


class JupyterTestCase(DataprocTestCase):
    COMPONENT = 'JUPYTER,ANACONDA'
    INIT_ACTIONS = ['jupyter_sparkmonitor/sparkmonitor.sh']

    def verify_instance(self, name, jupyter_port):
        verify_cmd = "curl {} -L {}:{} | grep 'Jupyter Notebook'".format(
            "--retry 10 --retry-delay 10 --retry-connrefused", name,
            jupyter_port)
        self.assert_instance_command(name, verify_cmd)

    @parameterized.expand(
        [
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("SINGLE", "1.4", ["m"]),
            ("STANDARD", "1.4", ["m"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_sparkmonitor(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           timeout_in_minutes=15,
                           machine_type="n1-standard-2")

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix), "8123")


if __name__ == '__main__':
    unittest.main()
