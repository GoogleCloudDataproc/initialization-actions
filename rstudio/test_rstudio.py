import random
import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RStudioTestCase(DataprocTestCase):
    COMPONENT = 'rstudio'
    INIT_ACTIONS = ['rstudio/rstudio.sh']

    @parameterized.expand(
        [
            ("SINGLE", "1.0", "rstudio", "password"),
            ("SINGLE", "1.1", "rstudio", "password"),
            ("SINGLE", "1.2", "rstudio", "password"),
            ("SINGLE", "1.3", "rstudio", "password"),
            ("SINGLE", "1.3", "", "password"),  # default username
            ("SINGLE", "1.3", "rstudio", ""),  # no auth
            ("SINGLE", "1.3", "", ""),  # default username and no auth
            ("SINGLE", "1.4", "rstudio", "password"),
        ],
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
