from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class ZeppelinTestCase(DataprocTestCase):
    COMPONENT = 'zeppelin'
    INIT_ACTIONS = ['zeppelin/zeppelin.sh']

    def verify_instance(self, instance_name):
        verify_cmd_fmt = '''
            COUNTER=0
            until curl -L {}:8080 | grep '{}' && exit 0 || ((COUNTER >= 60)); do
              ((COUNTER++))
              sleep 5
            done
            exit 1
            '''
        self.assert_instance_command(
            instance_name,
            verify_cmd_fmt.format(instance_name,
                                  "<h3>Query. Explore. Repeat.</h3>"))

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
