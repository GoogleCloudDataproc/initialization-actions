import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class ConnectorsTestCase(DataprocTestCase):
    COMPONENT = "connectors"
    INIT_ACTIONS = ['connectors/connectors.sh']

    BQ_CONNECTOR_VERSION = "1.0.1"
    BQ_CONNECTOR_URL = "gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-1.0.1.jar"

    GCS_CONNECTOR_VERSION = "2.0.1"
    GCS_CONNECTOR_URL = "gs://hadoop-lib/gcs/gcs-connector-hadoop2-2.0.1.jar"

    def verify_instances(self, cluster, instances, connector,
                         connector_version):
        for instance in instances:
            self._verify_instance("{}-{}".format(cluster, instance), connector,
                                  connector_version)

    def _verify_instance(self, instance, connector, connector_version):
        self.assert_instance_command(
            instance,
            "test -f {}/{}-hadoop2-{}.jar".format(self._connectors_dir(),
                                                  connector,
                                                  connector_version))
        self.assert_instance_command(
            instance, "test -L {}/{}.jar".format(self._connectors_dir(),
                                                 connector, connector_version))

    def _connectors_dir(self):
        if self.getImageVersion() < pkg_resources.parse_version("1.4"):
            return "/usr/lib/hadoop/lib"
        return "/usr/local/share/google/dataproc/lib"

    @parameterized.parameters(("SINGLE", ["m"]),
                              ("HA", ["m-0", "m-1", "m-2", "w-0", "w-1"]))
    def test_gcs_connector_version(self, configuration, instances):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata="gcs-connector-version={}".format(
                               self.GCS_CONNECTOR_VERSION))
        self.verify_instances(self.getClusterName(), instances,
                              "gcs-connector", self.GCS_CONNECTOR_VERSION)

    @parameterized.parameters(("SINGLE", ["m"]),
                              ("HA", ["m-0", "m-1", "m-2", "w-0", "w-1"]))
    def test_bq_connector_version(self, configuration, instances):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata="bigquery-connector-version={}".format(
                               self.BQ_CONNECTOR_VERSION))
        self.verify_instances(self.getClusterName(), instances,
                              "bigquery-connector", self.BQ_CONNECTOR_VERSION)

    @parameterized.parameters(("SINGLE", ["m"]),
                              ("HA", ["m-0", "m-1", "m-2", "w-0", "w-1"]))
    def test_gcs_connector_url(self, configuration, instances):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata="gcs-connector-url={}".format(
                               self.GCS_CONNECTOR_URL))
        self.verify_instances(self.getClusterName(), instances,
                              "gcs-connector", self.GCS_CONNECTOR_VERSION)

    @parameterized.parameters(("SINGLE", ["m"]),
                              ("HA", ["m-0", "m-1", "m-2", "w-0", "w-1"]))
    def test_bq_connector_url(self, configuration, instances):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata="bigquery-connector-url={}".format(
                               self.BQ_CONNECTOR_URL))
        self.verify_instances(self.getClusterName(), instances,
                              "bigquery-connector", self.BQ_CONNECTOR_VERSION)


if __name__ == '__main__':
    absltest.main()
