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
        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            self.skipTest("Not supported on Dataproc {}".format(
                self.getImageVersion()))

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           timeout_in_minutes=20,
                           machine_type="e2-standard-8",
                           scopes="cloud-platform")

        self.assert_dataproc_job(
            self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                                 self.SAMPLE_H2O_JOB_PATH))


if __name__ == "__main__":
    absltest.main()
