import os

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class LivyTestCase(DataprocTestCase):
    COMPONENT = 'livy'
    INIT_ACTIONS = ['livy/livy.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_livy_running.py'

    def _verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME), name)
        self._run_python_test_file(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def _run_python_test_file(self, name):
        self.assert_instance_command(
            name,
            "sudo apt-get install -y python3-pip && sudo pip3 install requests"
        )
        self.assert_instance_command(
            name, "sudo python3 {}".format(self.TEST_SCRIPT_FILE_NAME))

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
        ("HA", ["m-0", "m-1", "m-2"]),
    )
    def test_livy(self, configuration, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS)
        for machine_suffix in machine_suffixes:
            self._verify_instance("{}-{}".format(self.getClusterName(),
                                                 machine_suffix))


if __name__ == '__main__':
    absltest.main()
