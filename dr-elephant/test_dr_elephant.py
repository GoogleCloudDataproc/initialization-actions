import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class DrElephantTestCase(DataprocTestCase):
    COMPONENT = 'dr-elephant'
    INIT_ACTIONS = ['dr-elephant/dr-elephant.sh']

    def verify_instance(self, instance_name):
        verify_cmd_fmt = '''\
            while ! curl -L {}:8080 | grep '{}'; do sleep 5; done
            '''
        self.assert_instance_command(
            instance_name,
            verify_cmd_fmt.format(
                instance_name, "<p>I looked through <b>1</b> jobs today.<br>"))
        self.assert_instance_command(
            instance_name,
            verify_cmd_fmt.format(instance_name, "<div>Spark Pi</div>"))

    @parameterized.expand(
        [
            ("STANDARD", "1.2", ["m"]),
            ("HA", "1.2", ["m-0"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.3", ["m-0"]),
            ("STANDARD", "1.4", ["m"]),
            ("HA", "1.4", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_dr_elephant(self, configuration, dataproc_version,
                         machine_suffixes):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           timeout_in_minutes=30,
                           machine_type="n1-standard-2")

        # Submit a job to check if statistic is generated
        self.assert_dataproc_job(
            self.name, 'spark', '''\
                --class org.apache.spark.examples.SparkPi \
                --jars file:///usr/lib/spark/examples/jars/spark-examples.jar \
                -- 1000
            ''')

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    unittest.main()
