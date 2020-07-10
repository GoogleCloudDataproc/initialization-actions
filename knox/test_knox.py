import pkg_resources
import os

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class KnoxTestCase(DataprocTestCase):
    COMPONENT = 'knox'
    INIT_ACTIONS = ['knox/knox.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_knox.sh'

    def verify_instance(self, name, cert_type):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        print("Starting the tests")
        self._run_test_script(name, cert_type)

        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def _run_test_script(self, name, cert_type):
        self.assert_instance_command(
            name, "bash {} {}".format(self.TEST_SCRIPT_FILE_NAME, cert_type))

    @parameterized.parameters(("SINGLE", ["m"]), ("SINGLE", ["m"]),
                              ("STANDARD", ["m", "w-0"]),
                              ("HA", ["m-2", "w-0"]))
    def test_knox_localhost_cert(self, configuration, machine_suffixes):
        # Init action supported on Dataproc 1.3+
        if self.getImageVersion() < pkg_resources.parse_version("1.3"):
            return

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            # we don't want to run the auto update during the tests
            # since it overrides our changes in the tests.
            # So we assign cron date to 31/December. Hopefully
            # no one runs a test on that day
            metadata="knox-gw-config={}/knox,"
            "certificate_hostname=localhost,"
            "config_update_interval=\"0 0 31 12 * \"".format(
                self.INIT_ACTIONS_REPO))
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix),
                "localhost")

    @parameterized.parameters(("STANDARD", ["w-0", "m"]),
                              ("HA", ["m-1", "m-0"]))
    def test_knox_hostname_cert(self, configuration, machine_suffixes):
        # Init action supported on Dataproc 1.3+
        if self.getImageVersion() < pkg_resources.parse_version("1.3"):
            return

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            # we don't want to run the auto update during the tests
            # since it overrides our changes in the tests.
            # So we assign cron date to 31/December.
            # Hopefully no one runs a test on that day
            metadata="knox-gw-config={}/knox,"
            "certificate_hostname=HOSTNAME,"
            "config_update_interval=\"0 0 31 12 * \"".format(
                self.INIT_ACTIONS_REPO))
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix),
                "hostname")


if __name__ == '__main__':
    absltest.main()
