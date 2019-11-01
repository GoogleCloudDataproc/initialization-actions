from absl.testing import absltest
from absl.testing import parameterized

from absl import flags

from integration_tests.dataproc_test_case import DataprocTestCase


class JupyterTestCase(DataprocTestCase):
    OPTIONAL_COMPONENTS = 'ANACONDA,JUPYTER'
    COMPONENT = 'sparkmonitor'
    INIT_ACTIONS = ['jupyter_sparkmonitor/sparkmonitor.sh']

    def verify_instance(self, name, jupyter_port):
        verify_cmd_pip_check = "/opt/conda/default/bin/pip list | grep 'sparkmonitor'"
        self.assert_instance_command(name, verify_cmd_pip_check)

        verify_cmd = "curl {} -L {}:{} | grep 'Jupyter Notebook'".format(
            "--retry 10 --retry-delay 10 --retry-connrefused", name,
            jupyter_port)
        self.assert_instance_command(name, verify_cmd)


    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
    )
    def test_sparkmonitor(self, configuration, machine_suffixes):
        # Use 1.4 version of Dataproc to test because it requires Python 3
        dataproc_image_version = '1.4-debian9'
        FLAGS = flags.FLAGS
        FLAGS.image_version = dataproc_image_version
        jupyter_port = "8123"
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           optional_components=self.OPTIONAL_COMPONENTS,
                           timeout_in_minutes=10,
                           machine_type="n1-standard-2")

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix), jupyter_port)


if __name__ == '__main__':
    absltest.main()