import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class DatalabTestCase(DataprocTestCase):
    COMPONENT = 'datalab'
    INIT_ACTIONS = ['datalab/datalab.sh']
    PYTHON_3_INIT_ACTIONS = ['conda/bootstrap-conda.sh']

    def verify_instance(self, name):
        self.assert_instance_command(
            name, "curl {} -L {}:8080 | grep 'Google Cloud DataLab'".format(
                "--retry 10 --retry-delay 10 --retry-connrefused", name))

    @parameterized.parameters(
        ("STANDARD", ["m"], "python2"),
        ("STANDARD", ["m"], "python3"),
    )
    def test_datalab(self, configuration, machine_suffixes, python):
        # Skip on 2.0+ version of Dataproc because it's not supported
        if self.getImageVersion() >= pkg_resources.parse_version("2.0"):
            return

        init_actions = self.INIT_ACTIONS
        metadata = 'INIT_ACTIONS_REPO={}'.format(self.INIT_ACTIONS_REPO)
        if self.getImageVersion() <= pkg_resources.parse_version("1.3"):
            if python == "python3":
                init_actions = self.PYTHON_3_INIT_ACTIONS + init_actions
        elif python == "python2":
            return

        self.createCluster(
            configuration,
            init_actions,
            metadata=metadata,
            scopes='cloud-platform',
            timeout_in_minutes=30)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
