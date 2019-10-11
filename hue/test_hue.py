import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


class HueTestCase(DataprocTestCase):
    COMPONENT = 'hue'
    INIT_ACTIONS = ['hue/hue.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_hue_running.py'

    def verify_instance(self, instance_name):
        verify_cmd_fmt = '''
            COUNTER=0
            until curl -L {}:8888 | grep '{}' && exit 0 || ((COUNTER >= 60)); do
              ((COUNTER++))
              sleep 5
            done
            exit 1
            '''
        self.assert_instance_command(
            instance_name,
            verify_cmd_fmt.format(instance_name,
                                  "<h3>Query. Explore. Repeat.</h3>"))

    def buildParameters():
        """Builds parameters from flags arguments passed to the test."""
        flags_parameters = FLAGS.params
        params = []
        if not flags_parameters[0]:
            # Default parameters
            params = [
                # ("SINGLE", "1.1", ["m"]),
                # ("STANDARD", "1.1", ["m", "w-0"]),
                # ("HA", "1.1", ["m-0", "m-1", "m-2", "w-0"]),
                ("SINGLE", "1.2", ["m"]),
                ("STANDARD", "1.2", ["m", "w-0"]),
                ("HA", "1.2", ["m-0", "m-1", "m-2", "w-0"]),
                ("SINGLE", "1.3", ["m"]),
                ("STANDARD", "1.3", ["m", "w-0"]),
                ("HA", "1.3", ["m-0", "m-1", "m-2", "w-0"]),
            ]
        else:
            for param in flags_parameters:
                (config, version, machine_suffixes) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                params.append((config, version, machine_suffixes))
        return params

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hue(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
