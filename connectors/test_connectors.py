import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class ConnectorsTestCase(DataprocTestCase):
    COMPONENT = "connectors"
    INIT_ACTIONS = ['connectors/connectors.sh']

    BQ_CONNECTOR_VERSION = "1.0.0"
    GCS_CONNECTOR_VERSION = "2.0.0"

    def verify_instance(self, name, dataproc_version, connector,
                        connector_version):
        self.__submit_pig_job(
            name, "sh test -f {}/{}-hadoop2-{}.jar".format(
                self.__connectors_dir(dataproc_version), connector,
                connector_version))

        self.__submit_pig_job(
            name, "sh test -L {}/{}.jar".format(
                self.__connectors_dir(dataproc_version), connector,
                connector_version))

    @staticmethod
    def __connectors_dir(dataproc_version):
        if dataproc_version < "1.4":
            return "/usr/lib/hadoop/lib"
        return "/usr/local/share/google/dataproc/lib"

    def __submit_pig_job(self, cluster_name, job):
        self.assert_command(
            "gcloud dataproc jobs submit pig --format json --cluster {} -e '{}'"
            .format(cluster_name, job))

    @parameterized.expand(
        [
            ("SINGLE", "1.2"),
            ("STANDARD", "1.2"),
            ("HA", "1.2"),
            ("SINGLE", "1.3"),
            ("STANDARD", "1.3"),
            ("HA", "1.3"),
            ("SINGLE", "1.4"),
            ("STANDARD", "1.4"),
            ("HA", "1.4"),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_gcs_connector(self, configuration, dataproc_version):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           metadata="gcs-connector-version={}".format(
                               self.GCS_CONNECTOR_VERSION))
        self.verify_instance(self.getClusterName(), dataproc_version,
                             "gcs-connector", self.GCS_CONNECTOR_VERSION)

    @parameterized.expand(
        [
            ("SINGLE", "1.2"),
            ("STANDARD", "1.2"),
            ("HA", "1.2"),
            ("SINGLE", "1.3"),
            ("STANDARD", "1.3"),
            ("HA", "1.3"),
            ("SINGLE", "1.4"),
            ("STANDARD", "1.4"),
            ("HA", "1.4"),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_bq_connector(self, configuration, dataproc_version):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           metadata="bigquery-connector-version={}".format(
                               self.BQ_CONNECTOR_VERSION))
        self.verify_instance(self.getClusterName(), dataproc_version,
                             "bigquery-connector", self.BQ_CONNECTOR_VERSION)


if __name__ == '__main__':
    unittest.main()
