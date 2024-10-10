from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class OpenTelemetryTestCase(DataprocTestCase):
  COMPONENT = 'otel'
  INIT_ACTIONS = ['otel/otel.sh']

  def verify_service_status(self):
    instance_name = self.getClusterName() + '-m'
    self.assert_instance_command(instance=instance_name, cmd="systemctl status otelcol-contrib")

  @parameterized.parameters(
    'SINGLE',
    'STANDARD',
    'KERBEROS',
  )
  def test_otel(self, configuration):
    self.createCluster(configuration, self.INIT_ACTIONS)
    self.verify_service_status()


if __name__ == '__main__':
  absltest.main()
