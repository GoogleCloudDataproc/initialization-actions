import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HueTestCase(DataprocTestCase):
    COMPONENT = 'hue'
    INIT_ACTIONS = ['hue/hue.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_hue_running.py'

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

    @parameterized.expand(
        [
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("HA", "1.2", ["m-0"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.3", ["m-0"]),
            ("SINGLE", "1.4", ["m"]),
            ("STANDARD", "1.4", ["m"]),
            ("HA", "1.4", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hue(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    unittest.main()
