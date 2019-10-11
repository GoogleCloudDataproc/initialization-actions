import random
import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


class RStudioTestCase(DataprocTestCase):
    COMPONENT = 'rstudio'
    INIT_ACTIONS = ['rstudio/rstudio.sh']

    def buildParameters():
        """Builds parameters from flags arguments passed to the test.

        If specified, parameters are given as strings, example:
        'STANDARD 1.0 rstudio empty' or 'SINGLE 1.2 empty password'
        """
        flags_parameters = FLAGS.params
        params = []
        if not flags_parameters[0]:
            # Default parameters
            params = [
                ("SINGLE", "1.0", "rstudio", "password"),
                ("SINGLE", "1.1", "rstudio", "password"),
                ("SINGLE", "1.2", "rstudio", "password"),
                ("SINGLE", "1.3", "rstudio", "password"),
                ("SINGLE", "1.3", "", "password"),  # default username
                ("SINGLE", "1.3", "rstudio", ""),  # no auth
                ("SINGLE", "1.3", "", ""),  # default username and no auth
                ("SINGLE", "1.4", "rstudio", "password"),
            ]
        else:
            for param in flags_parameters:
                (config, version, user, password) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                if user == 'empty':
                    user = ''
                if password == 'empty':
                    password = ''
                params.append((config, version, machine_suffixes, user, password))
        return params

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_rstudio(self, configuration, dataproc_version, user, password):
        metadata = "rstudio-password={}".format(password)
        if user:
            metadata += ",rstudio-user={}".format(user)
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           metadata=metadata)
        instance_name = self.getClusterName() + "-m"
        self.assert_instance_command(
            instance_name, "curl http://{}:8787".format(instance_name))

if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
