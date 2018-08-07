import unittest

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase


class KafkaTestCase(DataprocTestCase):
    COMPONENT = 'kafka'
    INIT_ACTION = 'gs://dataproc-initialization-actions/kafka/kafka.sh'
    TEST_SCRIPT_FILE_NAME = 'validate.sh'

    def verify_instance(self, name):
        self.upload_test_file(name)
        self.__run_test_script(name)
        self.remove_test_script(name)

    def __run_test_script(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "bash {}"'.format(
                name,
                self.TEST_SCRIPT_FILE_NAME,
            )
        )
        self.assertEqual(ret_code, 0, "Failed to vaildate cluster. Last error: {}".format(stderr))

    @parameterized.expand([
        ("HA", "1.2", ["m-0", "m-1", "m-2"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_kafka(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )


if __name__ == '__main__':
    unittest.main()
