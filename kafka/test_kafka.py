import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class KafkaTestCase(DataprocTestCase):
    COMPONENT = 'kafka'
    INIT_ACTIONS = ['kafka/kafka.sh']
    KAFKA_CRUISE_CONTROL_INIT_ACTION = ['kafka/kafka.sh', 'kafka/cruise-control.sh']
    KAFKA_MANAGER_INIT_ACTION = ['kafka/kafka.sh', 'kafka/kafka-manager.sh']
    TEST_SCRIPT_FILE_NAME = 'validate.sh'
    PYTHON2_VERSION = 'python2.7'
    PYTHON3_VERSION = 'python3'
    KAFKA_VERIFY_SCRIPT_PYTHON = 'verify_kafka_running.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME), name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        self.assert_instance_command(
            name, "bash {}".format(self.TEST_SCRIPT_FILE_NAME))

    def __submit_pyspark_job(self, cluster_name):
        if self.getImageVersion() >= pkg_resources.parse_version("2.1"):
            python_version = self.PYTHON3_VERSION
        else:
            python_version = self.PYTHON2_VERSION
        self.assert_dataproc_job(cluster_name, 'pyspark',
                                 '{}/{}/{} --properties=spark.pyspark.python={},spark.pyspark.driver.python={}'
                                 ' -- /var/lib/kafka-logs'
                                 .format(self.INIT_ACTIONS_REPO,
                                         self.COMPONENT,
                                         self.KAFKA_VERIFY_SCRIPT_PYTHON,
                                         python_version,
                                         python_version))

    @parameterized.parameters(
        'STANDARD',
        'HA',
        'KERBEROS',
    )
    def test_kafka_job(self, configuration):
        if self.getImageOs() == 'rocky':
            self.skipTest("Not supported in Rocky Linux-based images")

        metadata = 'run-on-master=true,install-kafka-python=true'
        properties = 'dataproc:alpha.components=ZOOKEEPER'
        self.createCluster(configuration, self.INIT_ACTIONS, metadata=metadata,
                           properties=properties)
        self.__submit_pyspark_job(self.getClusterName())

    @parameterized.parameters(
        'STANDARD',
        'HA',
        'KERBEROS',
    )
    def test_kafka_cruise_control_job(self, configuration):
        if self.getImageOs() == 'rocky':
            self.skipTest("Not supported in Rocky Linux-based images")

        metadata = 'run-on-master=true,install-kafka-python=true'
        properties = 'dataproc:alpha.components=ZOOKEEPER'
        self.createCluster(configuration, self.KAFKA_CRUISE_CONTROL_INIT_ACTION, metadata=metadata,
                           properties=properties)
        self.__submit_pyspark_job(self.getClusterName())

    @parameterized.parameters(
        'STANDARD',
        'HA',
        'KERBEROS',
    )
    def test_kafka_manager_job(self, configuration):
        if self.getImageOs() == 'rocky':
            self.skipTest("Not supported in Rocky Linux-based images")

        if self.getImageVersion() <= pkg_resources.parse_version("2.0"):
            self.skipTest("Java 11 or higher is required for CMAK")

        metadata = 'run-on-master=true,kafka-enable-jmx=true,install-kafka-python=true'
        properties = 'dataproc:alpha.components=ZOOKEEPER'
        self.createCluster(configuration, self.KAFKA_MANAGER_INIT_ACTION, metadata=metadata,
                           properties=properties)
        self.__submit_pyspark_job(self.getClusterName())


if __name__ == '__main__':
    absltest.main()
