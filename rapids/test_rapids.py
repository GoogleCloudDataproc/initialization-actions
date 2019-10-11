import os
import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


class RapidsTestCase(DataprocTestCase):
    COMPONENT = 'rapids'
    INIT_ACTIONS = ['rapids/rapids.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_rapids.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        verify_cmd = "/opt/conda/anaconda/envs/RAPIDS/bin/python {}".format(
            self.TEST_SCRIPT_FILE_NAME)
        self.assert_instance_command(name, verify_cmd)

    def buildParameters():
        """Builds parameters from flags arguments passed to the test."""
        params = []
        if not FLAGS.params[0]:
            # Default parameters
            params = [("STANDARD", "1.3", ["m"])]
        else:
            for param in FLAGS.params:
                (config, version, machine_suffixes) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                params.append((config, version, machine_suffixes))
        return params
    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_rapids(self, configuration, dataproc_version, machine_suffixes):
        metadata = 'INIT_ACTIONS_REPO={}'.format(self.INIT_ACTIONS_REPO)

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           metadata=metadata,
                           beta=True,
                           master_accelerator='type=nvidia-tesla-p100',
                           worker_accelerator='type=nvidia-tesla-p100',
                           optional_components='ANACONDA',
                           machine_type='n1-standard-2',
                           timeout_in_minutes=20)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
