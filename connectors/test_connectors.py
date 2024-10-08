import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized
import logging
import os

from integration_tests.dataproc_test_case import DataprocTestCase

logging.basicConfig(level=os.getenv("LOG_LEVEL", logging.INFO))

class ConnectorsTestCase(DataprocTestCase):


    SPARK_BQ_CONNECTOR_VERSION = "0.31.1"
    COMPONENT = "connectors"
    INIT_ACTIONS = ['connectors/connectors.sh']

    BQ_CONNECTOR_VERSION = "1.2.0"
    BQ_CONNECTOR_URL = "gs://hadoop-lib/bigquery/bigquery-connector-{}-1.2.0.jar"


    SPARK_BQ_CONNECTOR_URL = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_{}-{}.jar"

    def verify_instances(self, cluster, instances, connector,
                         connector_version):
        for instance in instances:
            self._verify_instance("{}-{}".format(cluster, instance), connector,
                                  connector_version)

    def _verify_instance(self, instance, connector, connector_version):
        connectors_dir = "/usr/local/share/google/dataproc/lib"
        if connector == "spark-bigquery-connector":
            connector_jar = "spark-bigquery-with-dependencies_{}-{}.jar".format(
                self._scala_version(), connector_version)
        else:
            connector_jar = "{}-{}-{}.jar".format(connector,
                                                  self._hadoop_version(),
                                                  connector_version)

        self.assert_instance_command(
            instance, "test -f {}/{}".format(connectors_dir, connector_jar))
        self.assert_instance_command(
            instance, "test -L {}/{}.jar".format(connectors_dir, connector))

    def _hadoop_version(self):
        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            return "hadoop2"
        return "hadoop3"

    def _scala_version(self):
        return "2.12"

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("HA", ["m-0", "m-1", "m-2", "w-0", "w-1"]),
        ("KERBEROS", ["m"]),
    )
    def test_bq_connector_version(self, configuration, instances):
        if self.getImageOs() == 'rocky':
          self.skipTest("Not supported in Rocky Linux-based images")

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata="bigquery-connector-version={}".format(
                               self.BQ_CONNECTOR_VERSION))
        self.verify_instances(self.getClusterName(), instances,
                              "bigquery-connector", self.BQ_CONNECTOR_VERSION)

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("HA", ["m-0", "m-1", "m-2", "w-0", "w-1"]),
        ("KERBEROS", ["m"]),
    )
    def test_spark_bq_connector_version(self, configuration, instances):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            self.SPARK_BQ_CONNECTOR_VERSION = "0.29.0"
        else:
            self.SPARK_BQ_CONNECTOR_VERSION = "0.31.1"

        if self.getImageOs() == 'rocky':
          self.skipTest("Not supported in Rocky Linux-based images")

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata="spark-bigquery-connector-version={}".format(
                self.SPARK_BQ_CONNECTOR_VERSION))
        self.verify_instances(self.getClusterName(), instances,
                              "spark-bigquery-connector",
                              self.SPARK_BQ_CONNECTOR_VERSION)

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("HA", ["m-0", "m-1", "m-2", "w-0", "w-1"]),
        ("KERBEROS", ["m"]),
    )
    def test_bq_connector_url(self, configuration, instances):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            self.SPARK_BQ_CONNECTOR_VERSION = "0.29.0"
        else:
            self.SPARK_BQ_CONNECTOR_VERSION = "0.31.1"

        if self.getImageOs() == 'rocky':
          self.skipTest("Not supported in Rocky Linux-based images")

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata="bigquery-connector-url={}".format(
                               self.BQ_CONNECTOR_URL.format(
                                   self._hadoop_version())))
        self.verify_instances(self.getClusterName(), instances,
                              "bigquery-connector", self.BQ_CONNECTOR_VERSION)

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("HA", ["m-0", "m-1", "m-2", "w-0", "w-1"]),
        ("KERBEROS", ["m"]),
    )
    def test_spark_bq_connector_url(self, configuration, instances):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            self.SPARK_BQ_CONNECTOR_VERSION = "0.29.0"
        else:
            self.SPARK_BQ_CONNECTOR_VERSION = "0.31.1"

        if self.getImageOs() == 'rocky':
          self.skipTest("Not supported in Rocky Linux-based images")

        logging.warning("image version: " + str( self.getImageVersion() ) )
        logging.warning("SPARK_BQ_CONNECTOR_VERSION=" + self.SPARK_BQ_CONNECTOR_VERSION )
        logging.warning("scala_version=" + self._scala_version())

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata="spark-bigquery-connector-url={}".format(
                               self.SPARK_BQ_CONNECTOR_URL.format(
                                   self._scala_version(),
                                   self.SPARK_BQ_CONNECTOR_VERSION
                               )))
        self.verify_instances(self.getClusterName(), instances,
                              "spark-bigquery-connector",
                              self.SPARK_BQ_CONNECTOR_VERSION)


if __name__ == '__main__':
    absltest.main()
