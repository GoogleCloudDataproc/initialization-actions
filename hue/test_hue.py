import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HueTestCase(DataprocTestCase):
    COMPONENT = 'hue'
    INIT_ACTIONS = ['hue/hue.sh']
    PYTHON2_BINARY = 'python2'
    PYTHON3_BINARY = 'python3'
    HUE_VERIFY_SCRIPT = 'verify_hue_running.py'
    HUE_VERIFY_SCRIPT_ADDITIONAL = 'run_queries.py'

    def verify_instance(self, instance_name):
        verify_cmd_fmt = '''
            COUNTER=0
            until curl -L {}:8888 | grep '{}' && exit 0 || ((COUNTER >= 60)); do
              ((COUNTER++))
              sleep 5
            done
            exit 1
            '''
        self.assert_instance_command(
            instance_name,
            verify_cmd_fmt.format(instance_name,
                                  "<h3>Query. Explore. Repeat.</h3>"))

    def __submit_pyspark_job(self, cluster_name):
        if self.getImageVersion() >= pkg_resources.parse_version("2.1"):
            python_binary = self.PYTHON3_BINARY
        else:
            python_binary = self.PYTHON2_BINARY
        properties = 'spark.pyspark.python={},spark.pyspark.driver.python={}'.format(python_binary,
                                                                                     python_binary)
        main_python_file = '{}/{}/{}'.format(self.INIT_ACTIONS_REPO,
                                             self.COMPONENT,
                                             self.HUE_VERIFY_SCRIPT)
        additional_python_file = '{}/{}/{}'.format(self.INIT_ACTIONS_REPO,
                                                   self.COMPONENT,
                                                   self.HUE_VERIFY_SCRIPT_ADDITIONAL)
        self.assert_dataproc_job(cluster_name, 'pyspark', '{} --files={} --properties={}'.format(main_python_file,
                                                                                                 additional_python_file,
                                                                                                 properties))

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
        ("HA", ["m-0"]),
    )
    def test_hue(self, configuration, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
        'SINGLE',
        'STANDARD',
    )
    def test_hue_job(self, configuration):
        if self.getImageVersion() >= pkg_resources.parse_version("2.2"):
            self.skipTest("Not supported in 2.2 image")
        self.createCluster(configuration, self.INIT_ACTIONS)
        self.__submit_pyspark_job(self.getClusterName())


if __name__ == '__main__':
    absltest.main()
