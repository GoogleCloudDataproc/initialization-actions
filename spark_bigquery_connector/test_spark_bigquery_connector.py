import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class SparkBQConnectorTestCase(DataprocTestCase):
    COMPONENT = 'sbqconnector'
    INIT_ACTIONS = ['spark_bigquery_connector/install_connector.sh']

    @parameterized.parameters(
        "SINGLE",
        "STANDARD",
    )
    def test_connector(self, configuration):
        # Init action supported on Dataproc 1.4+
        if self.getImageVersion() < pkg_resources.parse_version("1.4"):
            return

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            timeout_in_minutes=30,
            machine_type="n1-standard-4")

        # Verify a cluster using PySpark job
        self.assert_dataproc_job(
            self.name, 'pyspark', 
            'file:///opt/spark-bigquery-connector/tests/spark_bq_test.py')


if __name__ == '__main__':
    absltest.main()
