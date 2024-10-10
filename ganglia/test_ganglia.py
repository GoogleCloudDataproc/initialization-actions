import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class GangliaTestCase(DataprocTestCase):
    COMPONENT = 'ganglia'
    INIT_ACTIONS = ['ganglia/ganglia.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_ganglia_running.py'

    def verify_instance(self, name):
        test_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.TEST_SCRIPT_FILE_NAME)
        self.upload_test_file(test_script_path, name)
        self.assert_instance_command(name,
                                     "yes | sudo apt-get install python3-pip libxml2-dev libxslt-dev")
        self.assert_instance_command(name, "sudo -H pip3 install --upgrade pip")
        self.assert_instance_command(name, "sudo pip3 install requests-html")
        self.assert_instance_command(name, "sudo pip install -U urllib3 requests")
        self.assert_instance_command(name, "pip install lxml[html_clean]")
        self.assert_instance_command(
            name, "python3 {}".format(self.TEST_SCRIPT_FILE_NAME))
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
            self.skipTest("Ganglia UI is not supported for 2.0+ versions")

        self.createCluster(configuration, self.INIT_ACTIONS)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
