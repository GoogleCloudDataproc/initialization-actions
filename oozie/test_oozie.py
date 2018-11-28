import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class OozieTestCase(DataprocTestCase):
    COMPONENT = 'oozie'
    INIT_ACTION = 'gs://dataproc-initialization-actions/oozie/oozie.sh'
    TEST_SCRIPT_FILE_NAME = 'validate.sh'

    def verify_instance(self, name):
        self.upload_test_file(name)
        self.__run_test_file(name)
        self.remove_test_script(name)

    def __run_test_file(self, name):
        cmd = 'gcloud compute ssh {} -- bash {}'.format(
            name,
            self.TEST_SCRIPT_FILE_NAME
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        print(ret_code, stderr, stdout)
        self.assertEqual(ret_code, 0, "Failed to run test file. Error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.1", ["m"]),
        ("SINGLE", "1.2", ["m"]),
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.1", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("STANDARD", "1.3", ["m"]),
        ("HA", "1.1", ["m-0", "m-1", "m-2"]),
        ("HA", "1.2", ["m-0", "m-1", "m-2"]),
        ("HA", "1.3", ["m-0", "m-1", "m-2"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_oozie(self, configuration, dataproc_version, machine_suffixes):
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
