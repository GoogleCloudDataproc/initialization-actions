import os
import sys
import unittest

from absl import flags
from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS

flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


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

    def buildParameters():
        """Builds parameters from flags arguments passed to the test.

        If specified, parameters are given as strings, example:
        'STANDARD m,w-0;m,m'

        The image_version flag for this test must be provided as
        <major>.<minor>-os, e.g. 1.3-deb9

        For verify options, tuples are separated by ';' and elements
        within each tuple are separated by ','.
        """
        flags_parameters = FLAGS.params
        params = []
        if not flags_parameters[0]:
            # Default parameters
            params = [
                ("SINGLE", [("m", "m")]),
                ("STANDARD", [("m", "w-0"), ("m", "m")]),
                ("HA", [("m-0", "w-0"), ("w-0", "m-1")]),
            ]
        else:
            for param in flags_parameters:
                (config, verify_options) = param.split()
                verify_options = (verify_options.split(';')
                    if ';' in verify_options
                    else [verify_options])
                verify_options_list = []
                for verify_option in verify_options:
                    machine_suffixes = (tuple(verify_option.split(','))
                        if ',' in verify_option
                        else (verify_option,))
                    verify_options_list.append(machine_suffixes)
                params.append((config, verify_options_list))
        return params

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_drill(self, configuration, verify_options):
        init_actions = self.INIT_ACTIONS
        if configuration == "STANDARD":
            init_actions = self.INIT_ACTIONS_FOR_STANDARD + init_actions
        FLAGS.image_version = "1.3-deb9" if FLAGS.image_version == "1.3"  # Change default
        print(FLAGS.image_version)
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
    del sys.argv[1:]
    unittest.main()
