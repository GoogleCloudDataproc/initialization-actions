import os
import unittest
import sys
from absl import flags

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


class FlinkTestCase(DataprocTestCase):
    COMPONENT = 'flink'
    INIT_ACTIONS = ['flink/flink.sh']
    TEST_SCRIPT_FILE_NAME = 'validate.sh'

    def verify_instance(self, name, yarn_session=True):
        test_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.TEST_SCRIPT_FILE_NAME)
        self.upload_test_file(test_script_path, name)
        self.__run_test_file(name, yarn_session)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_file(self, name, yarn_session):
        self.assert_instance_command(
            name, "bash {} {}".format(self.TEST_SCRIPT_FILE_NAME,
                                      yarn_session))

    def buildParameters(with_optional_metadata):
        """Builds parameters from flags arguments passed to the test."""
        flags_parameters = FLAGS.params
        params = []
        if not flags_parameters[0]:
            # Default parameters
            if with_optional_metadata:
                params = [
                    ("STANDARD", ["m"]),
                    ("HA", ["m-0", "m-1", "m-2"]),
                    ("SINGLE", ["m"]),
                ]
            else:
                params = [
                    ("STANDARD", ["m"]),
                    ("HA", ["m-0", "m-1", "m-2"]),
                ]
        else:
            for param in flags_parameters:
                (config, machine_suffixes) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                params.append((config, machine_suffixes))
        return params

    @parameterized.expand(
        buildParameters(with_optional_metadata=False),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_flink(self, configuration, machine_suffixes):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type="n1-standard-2")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.expand(
        buildParameters(with_optional_metadata=True),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_flink_with_optional_metadata(self, configuration,
                                          machine_suffixes):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type="n1-standard-2",
                           metadata="flink-start-yarn-session=false")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix),
                                 yarn_session=False)


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
