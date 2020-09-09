import os
import pkg_resources

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class FlinkTestCase(DataprocTestCase):
  COMPONENT = "flink"
  INIT_ACTIONS = ["flink/flink.sh"]
  TEST_SCRIPT_FILE_NAME = "validate.sh"

  HA_PROPERTIES = "yarn:yarn.resourcemanager.am.max-attempts=4"

  def verify_instance(self, name, yarn_session=True):
    test_script_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), self.TEST_SCRIPT_FILE_NAME)
    self.upload_test_file(test_script_path, name)
    self.__run_test_file(name, yarn_session)
    self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

  def __run_test_file(self, name, yarn_session):
    self.assert_instance_command(
        name, "bash {} {}".format(self.TEST_SCRIPT_FILE_NAME, yarn_session))

  @parameterized.parameters(
      ("STANDARD", ["m"]),
      ("HA", ["m-0", "m-1", "m-2"]),
  )
  def test_flink(self, configuration, machine_suffixes):
    # Skip on 2.0+ version of Dataproc because it's not supported
    if self.getImageVersion() >= pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in 2.0+ images")

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        properties=self.HA_PROPERTIES if configuration is "HA" else None)
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))

  @parameterized.parameters(
      ("SINGLE", ["m"]),
      ("STANDARD", ["m"]),
      ("HA", ["m-0", "m-1", "m-2"]),
  )
  def test_flink_with_optional_metadata(self, configuration, machine_suffixes):
    # Skip on 2.0+ version of Dataproc because it's not supported
    if self.getImageVersion() >= pkg_resources.parse_version("2.0"):
      self.skipTest("Not supported in 2.0+ images")

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata="flink-start-yarn-session=false",
        properties=self.HA_PROPERTIES if configuration is "HA" else None)
    for machine_suffix in machine_suffixes:
      self.verify_instance(
          "{}-{}".format(self.getClusterName(), machine_suffix),
          yarn_session=False)


if __name__ == "__main__":
  absltest.main()
