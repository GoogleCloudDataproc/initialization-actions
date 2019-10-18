import os
import sys
import unittest

from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS


class DrillTestCase(DataprocTestCase):
    COMPONENT = 'drill'
    INIT_ACTIONS = ['drill/drill.sh']
    INIT_ACTIONS_FOR_STANDARD = ['zookeeper/zookeeper.sh']
    TEST_SCRIPT_FILE_NAME = 'validate.sh'

    def verify_instance(self, name, drill_mode, target_node):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self.__run_bash_test_file(name, drill_mode, target_node)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_bash_test_file(self, name, drill_mode, target_node):
        self.assert_instance_command(
            name, "sudo bash {} {} {}".format(self.TEST_SCRIPT_FILE_NAME,
                                              drill_mode, target_node))

    @parameterized.expand(
        [
            ("SINGLE", [("m", "m")]),
            ("STANDARD", [("m", "w-0"), ("m", "m")]),
            ("HA", [("m-0", "w-0"), ("w-0", "m-1")]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_drill(self, configuration, verify_options):
        init_actions = self.INIT_ACTIONS
        if configuration == "STANDARD":
            init_actions = self.INIT_ACTIONS_FOR_STANDARD + init_actions
        FLAGS.image_version = (
            "1.3-deb9"
            if FLAGS.image_version == "1.3"
            else FLAGS.image_version)  # Change default 1.3 to 1.3-deb9
        self.createCluster(configuration,
                           init_actions,
                           machine_type="n1-standard-2")

        drill_mode = "DISTRIBUTED"
        if configuration == "SINGLE":
            drill_mode = "EMBEDDED"
        for option in verify_options:
            machine_suffix, target_machine_suffix = option
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix),
                drill_mode,
                "{}-{}".format(self.getClusterName(), target_machine_suffix),
            )


if __name__ == '__main__':
    unittest.main()
