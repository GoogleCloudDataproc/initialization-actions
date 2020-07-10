from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class DrElephantTestCase(DataprocTestCase):
    COMPONENT = 'dr-elephant'
    INIT_ACTIONS = ['dr-elephant/dr-elephant.sh']

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
            verify_cmd_fmt.format(
                instance_name, "<p>I looked through <b>1</b> jobs today.<br>"))
        self.assert_instance_command(
            instance_name,
            verify_cmd_fmt.format(instance_name, "<div>QuasiMonteCarlo</div>"))

    @parameterized.parameters(
        ("STANDARD", ["m"]),
        ("HA", ["m-0"]),
    )
    def test_dr_elephant(self, configuration, machine_suffixes):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            timeout_in_minutes=30)

        # Submit a job to check if statistic is generated
        self.assert_dataproc_job(
            self.name, 'hadoop', '''\
                --jar file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar \
                -- pi 4 1000
            ''')

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
