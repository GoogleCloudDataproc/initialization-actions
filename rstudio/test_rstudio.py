import random
import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
FLAGS(sys.argv)


class RStudioTestCase(DataprocTestCase):
    COMPONENT = 'rstudio'
    INIT_ACTIONS = ['rstudio/rstudio.sh']

    @parameterized.expand(
        [
            ("SINGLE", "rstudio", "password"),
            ("SINGLE", "", "password"),  # default username
            ("SINGLE", "rstudio", ""),  # no auth
            ("SINGLE", "", ""),  # default username and no auth
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_rstudio(self, configuration, user, password):
        metadata = "rstudio-password={}".format(password)
        if user:
            metadata += ",rstudio-user={}".format(user)
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata=metadata)
        instance_name = self.getClusterName() + "-m"
        self.assert_instance_command(
            instance_name, "curl http://{}:8787".format(instance_name))

if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
