import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


class DatalabTestCase(DataprocTestCase):
    COMPONENT = 'datalab'
    INIT_ACTIONS = ['datalab/datalab.sh']
    PYTHON_3_INIT_ACTIONS = ['conda/bootstrap-conda.sh']

    def verify_instance(self, name):
        self.assert_instance_command(
            name, "curl {} -L {}:8080 | grep 'Google Cloud DataLab'".format(
                "--retry 10 --retry-delay 10 --retry-connrefused", name))

    def buildParameters():
        """Builds parameters from flags arguments passed to the test."""
        flags_parameters = FLAGS.params
        params = []
        if not flags_parameters[0]:
            # Default parameters
            params = [
                ("STANDARD", ["m"], "python2"),
                ("STANDARD", ["m"], "python3"),
            ]
        else:
            for param in flags_parameters:
                (config, machine_suffixes, python_version) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                params.append((config, machine_suffixes, python_version))
        return params

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_datalab(self, configuration, machine_suffixes,
                     python):
        init_actions = self.INIT_ACTIONS
        metadata = 'INIT_ACTIONS_REPO={}'.format(self.INIT_ACTIONS_REPO)
        if python == "python3":
            init_actions = self.PYTHON_3_INIT_ACTIONS + init_actions

        self.createCluster(configuration,
                           init_actions,
                           metadata=metadata,
                           scopes='cloud-platform',
                           timeout_in_minutes=30)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
  del sys.argv[1:]
    unittest.main()
