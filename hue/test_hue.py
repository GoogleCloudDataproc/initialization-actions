import os
import pkg_resources

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HueTestCase(DataprocTestCase):
    COMPONENT = 'hue'
    INIT_ACTIONS = ['hue/hue.sh']
    HUE_SCRIPT = 'test_hue_with_hive.sh'

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
        self._run_hue_test_script(instance_name)

    def _run_hue_test_script(self, instance_name):
        verify_cmd = "/bin/bash {}".format(self.HUE_SCRIPT)
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)), self.HUE_SCRIPT),
            instance_name)
        self.assert_instance_command(instance_name, verify_cmd)
        self.remove_test_script(self.HUE_SCRIPT, instance_name)

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


if __name__ == '__main__':
    absltest.main()
