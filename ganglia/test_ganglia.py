import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class GangliaTestCase(DataprocTestCase):
    COMPONENT = 'ganglia'
    INIT_ACTIONS = ['ganglia/ganglia.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_ganglia_running.pl'

    def verify_instance(self, name):
        test_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.TEST_SCRIPT_FILE_NAME)
        self.upload_test_file(test_script_path, name)
        self.assert_instance_command(name,"/usr/bin/perl {}".format(self.TEST_SCRIPT_FILE_NAME))
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m", "w-0"]),
        ("HA", ["m-0", "m-1", "m-2", "w-0"]),
        ("KERBEROS", ["m"]),
    )
    def test_ganglia(self, configuration, machine_suffixes):
        if self.getImageOs() == 'rocky':
            self.skipTest("Not supported in Rocky Linux-based images")

        if self.getImageVersion() > pkg_resources.parse_version("2.0"):
            self.skipTest("Ganglia UI is not supported for 2.1+ versions")

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            machine_type="n1-standard-8",
        )
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
