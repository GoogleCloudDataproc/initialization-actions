import os
import pkg_resources

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class LivyTestCase(DataprocTestCase):
    COMPONENT = 'livy'
    INIT_ACTIONS = ['livy/livy.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_livy_running.py'
    DEFAULT_LIVY_VERSION = '0.7.1'
    DEFAULT_SCALA_VERSION = '2.11'
    LIVY_VERSION = '0.8.0'
    SCALA_VERSION = '2.12'

    def _verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME), name)
        self._run_python_test_file(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def _run_python_test_file(self, name):
        if self.getImageVersion() >= pkg_resources.parse_version("2.2"):
            self.assert_instance_command(
                name,
                "sudo apt install python3-requests"
            )
        else:
            self.assert_instance_command(
                name,
                "sudo apt-get install -y python3-pip && sudo pip3 install requests"
            )
        self.assert_instance_command(
            name, "sudo python3 {}".format(self.TEST_SCRIPT_FILE_NAME))

    def __submit_pyspark_job(self, cluster_name):
        self.assert_dataproc_job(cluster_name, 'pyspark',
                                 '{}/{}/{}'
                                 .format(self.INIT_ACTIONS_REPO,
                                         self.COMPONENT,
                                         self.TEST_SCRIPT_FILE_NAME))

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
        ("HA", ["m-0", "m-1", "m-2"]),
    )
    def test_livy(self, configuration, machine_suffixes):
        if self.getImageVersion() >= pkg_resources.parse_version("1.5"):
            self.skipTest("Not supported in 1.5+ images")

        self.createCluster(configuration, self.INIT_ACTIONS)
        for machine_suffix in machine_suffixes:
            self._verify_instance("{}-{}".format(self.getClusterName(),
                                                 machine_suffix))

    @parameterized.parameters(
        "SINGLE",
        "STANDARD",
        "HA",
    )
    def test_livy_job(self, configuration):
        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            self.skipTest("Not supported in 1.5 images")

        if self.getImageVersion() >= pkg_resources.parse_version("2.0"):
            metadata = 'livy-version={},scala-version={}'.format(self.LIVY_VERSION, self.SCALA_VERSION)
        else:
            metadata = 'livy-version={},scala-version={}'.format(self.DEFAULT_LIVY_VERSION, self.DEFAULT_SCALA_VERSION)
        self.createCluster(configuration, self.INIT_ACTIONS, metadata=metadata)
        self.__submit_pyspark_job(self.getClusterName())


if __name__ == '__main__':
    absltest.main()
