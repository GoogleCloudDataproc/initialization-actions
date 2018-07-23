import unittest

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase


class ZeppelinTestCase(DataprocTestCase):
    COMPONENT = 'zeppelin'
    INIT_ACTION = 'gs://polidea-dataproc-utils/zeppelin/zeppelin-a2.sh'
    GOOGLE_TEST_SCRIPT_FILE_NAME = "verify_zeppelin_running.py"
    GOOGLE_TEST_SCRIPT_BUCKET = "hcd-pub-2f10d78d114f6aaec76462e3c310f31f/init-action-test-scripts"

    def verify_instance(self, name):
        self.__download_test_script(name)
        self.__run_test_script(name)
        self.__remove_test_script(name)

    def __download_test_script(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "gsutil cp gs://{}/{} ."'.format(
                name,
                self.GOOGLE_TEST_SCRIPT_BUCKET,
                self.GOOGLE_TEST_SCRIPT_FILE_NAME,
            )
        )
        self.assertEqual(ret_code, 0, "Failed to download test file. Error: {}".format(stderr))

    def __run_test_script(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "python {}"'.format(
                name,
                self.GOOGLE_TEST_SCRIPT_FILE_NAME,
            )
        )
        self.assertEqual(ret_code, 0, "Failed to vaildate cluster. Last error: {}".format(stderr))

    def __remove_test_script(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "rm {}"'.format(
                name,
                self.GOOGLE_TEST_SCRIPT_FILE_NAME,
            )
        )
        self.assertEqual(ret_code, 0, "Failed to remove test file. Error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.0", ["m"]),
        ("STANDARD", "1.0", ["m"]),
        ("HA", "1.0", ["m-0"]),
        ("SINGLE", "1.1", ["m"]),
        ("STANDARD", "1.1", ["m"]),
        ("HA", "1.1", ["m-0"]),
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_zeppelin(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @parameterized.expand([
        ("SINGLE", "1.0"),
        ("STANDARD", "1.0", ["m"]),
        ("HA", "1.0", ["m-0"]),
        ("SINGLE", "1.1", ["m"]),
        ("STANDARD", "1.1", ["m"]),
        ("HA", "1.1", ["m-0"]),
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_zeppelin_on_invalid_port(self, configuration, dataproc_version):
        with self.assertRaises(AssertionError):
            self.createCluster(configuration, self.INIT_ACTION, dataproc_version, metadata="zeppelin-port=not_a_port")


if __name__ == '__main__':
    unittest.main()
