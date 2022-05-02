import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class H2OTestCase(DataprocTestCase):
    COMPONENT = "h2o"
    INIT_ACTIONS = ["h2o/h2o.sh"]
    SAMPLE_H2O_JOB_PATH = "h2o/sample-script.py"

    @parameterized.parameters("STANDARD", "HA")
    def test_h2o(self, configuration):
        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            self.skipTest("Not supported in pre-2.0 images")

        optional_components = None
        init_actions = self.INIT_ACTIONS

        self.createCluster(configuration,
                           init_actions,
                           machine_type="e2-highmem-4",
                           optional_components=optional_components,
                           scopes="cloud-platform")

        self.assert_dataproc_job(
            self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                                 self.SAMPLE_H2O_JOB_PATH))


if __name__ == "__main__":
    absltest.main()
