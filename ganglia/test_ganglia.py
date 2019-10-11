import os
import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


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
                                     "yes | sudo apt-get install python3-pip")
        self.assert_instance_command(name, "sudo pip3 install requests-html")
        self.assert_instance_command(
            name, "python3 {}".format(self.TEST_SCRIPT_FILE_NAME))
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def buildParameters():
        """Builds parameters from flags arguments passed to the test."""
        flags_parameters = FLAGS.params
        params = []
        if not flags_parameters[0]:
            # Default parameters
            params = [
                ("SINGLE", "1.0", ["m"]),
                ("STANDARD", "1.0", ["m", "w-0"]),
                ("HA", "1.0", ["m-0", "m-1", "m-2", "w-0"]),
                ("SINGLE", "1.1", ["m"]),
                ("STANDARD", "1.1", ["m", "w-0"]),
                ("HA", "1.1", ["m-0", "m-1", "m-2", "w-0"]),
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
    def test_ganglia(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
