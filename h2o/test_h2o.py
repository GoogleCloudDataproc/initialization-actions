import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class H2OTestCase(DataprocTestCase):
    COMPONENT = "h2o"
    INIT_ACTIONS = ["h2o/h2o.sh"]
    SAMPLE_H2O_JOB_PATH = "h2o/sample-script.py"

    @parameterized.parameters("SINGLE", "STANDARD", "HA")
    def test_h2o(self, configuration):
        # Init action supported on Dataproc 1.3+
        if self.getImageVersion() < pkg_resources.parse_version("1.3"):
            return
        init_actions = self.INIT_ACTIONS
        optional_components = ["ANACONDA"]
        if self.getImageVersion() < pkg_resources.parse_version("1.4"):
            init_actions = ["conda/bootstrap-conda.sh"] + init_actions
            optional_components = None

        self.createCluster(configuration,
                           init_actions,
                           optional_components=optional_components,
                           scopes="cloud-platform")

        self.assert_dataproc_job(
            self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                                 self.SAMPLE_H2O_JOB_PATH))


if __name__ == "__main__":
    absltest.main()
