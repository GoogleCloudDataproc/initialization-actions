from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class OpenTelemetryTestCase(DataprocTestCase):
  COMPONENT = 'open-telemetry-agent'
  INIT_ACTIONS = ['otel/otel.sh']
  TEST_SCRIPT_FILE_NAME = 'otel/test-otel.py'

  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    cls.PROJECT_METADATA = '{}:{}'.format(cls.PROJECT, cls.REGION)

  def setUp(self):
    super().setUp()

  def tearDown(self):
    super().tearDown()

  def verify_service_status(self):
    self.assert_command(cmd="systemctl status otelcol-contrib")

  @parameterized.parameters(
      'SINGLE',
      'STANDARD',
      'HA',
  )
  def test_cloud_sql_proxy(self, configuration):
    self.createCluster(configuration, self.INIT_ACTIONS)
    self.verify_service_status()


if __name__ == '__main__':
  absltest.main()
