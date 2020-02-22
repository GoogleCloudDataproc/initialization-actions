import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class ConnectorsTestCase(DataprocTestCase):
    COMPONENT = "connectors"
    INIT_ACTIONS = ['connectors/connectors.sh']

    BQ_CONNECTOR_VERSION = "1.0.1"
    GCS_CONNECTOR_VERSION = "2.0.1"

    BQ_CONNECTOR_URL = "gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-1.0.1.jar"
    GCS_CONNECTOR_URL = "gs://hadoop-lib/gcs/gcs-connector-hadoop2-2.0.1.jar"

    def verify_instance(self, name, connector, connector_version):
        self.__submit_pig_job(
            name, "sh test -f {}/{}-hadoop2-{}.jar".format(
                self.__connectors_dir(), connector, connector_version))

        self.__submit_pig_job(
            name, "sh test -L {}/{}.jar".format(self.__connectors_dir(),
                                                connector, connector_version))

    def __connectors_dir(self):
        if self.getImageVersion() < pkg_resources.parse_version("1.4"):
            return "/usr/lib/hadoop/lib"
        return "/usr/local/share/google/dataproc/lib"

    def __submit_pig_job(self, cluster_name, job):
        self.assert_dataproc_job(cluster_name, 'pig', "-e '{}'".format(job))

    @parameterized.parameters(
        "SINGLE",
        "HA",
    )
    def test_gcs_connector_version(self, configuration):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata="gcs-connector-version={}".format(
                self.GCS_CONNECTOR_VERSION))
        self.verify_instance(self.getClusterName(), "gcs-connector",
                             self.GCS_CONNECTOR_VERSION)

    @parameterized.parameters(
        "SINGLE",
        "HA",
    )
    def test_bq_connector_version(self, configuration):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata="bigquery-connector-version={}".format(
                self.BQ_CONNECTOR_VERSION))
        self.verify_instance(self.getClusterName(), "bigquery-connector",
                             self.BQ_CONNECTOR_VERSION)

    @parameterized.parameters(
        "SINGLE",
        "HA",
    )
    def test_gcs_connector_url(self, configuration):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata="gcs-connector-url={}".format(
                self.GCS_CONNECTOR_URL))
        self.verify_instance(self.getClusterName(), "gcs-connector",
                             self.GCS_CONNECTOR_VERSION)

    @parameterized.parameters(
        "SINGLE",
        "HA",
    )
    def test_bq_connector_url(self, configuration):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata="bigquery-connector-url={}".format(
                self.BQ_CONNECTOR_URL))
        self.verify_instance(self.getClusterName(), "bigquery-connector",
                             self.BQ_CONNECTOR_VERSION)

if __name__ == '__main__':
    absltest.main()
