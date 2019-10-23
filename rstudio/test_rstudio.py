from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RStudioTestCase(DataprocTestCase):
    COMPONENT = 'rstudio'
    INIT_ACTIONS = ['rstudio/rstudio.sh']

    @parameterized.parameters(
        ("SINGLE", "rstudio", "password"),
        ("SINGLE", "", "password"),  # default username
        ("SINGLE", "rstudio", ""),  # no auth
        ("SINGLE", "", ""),  # default username and no auth
    )
    def test_rstudio(self, configuration, user, password):
        metadata = "rstudio-password={}".format(password)
        if user:
            metadata += ",rstudio-user={}".format(user)
        self.createCluster(configuration, self.INIT_ACTIONS, metadata=metadata)
        instance_name = self.getClusterName() + "-m"
        self.assert_instance_command(
            instance_name, "curl http://{}:8787".format(instance_name))


if __name__ == '__main__':
    absltest.main()
