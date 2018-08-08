import unittest

import time

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase


class DatalabTestCase(DataprocTestCase):
    COMPONENT = 'datalab'
    INIT_ACTION = 'gs://dataproc-initialization-actions/datalab/datalab.sh'
    GOOGLE_TEST_SCRIPT_FILE_NAME = "verify_datalab_running.py"
    GOOGLE_TEST_SCRIPT_BUCKET = "hcd-pub-2f10d78d114f6aaec76462e3c310f31f/init-action-test-scripts"

    def verify_instance(self, name):
        self.__download_test_script(name)
        self.__run_test_script(name)
        self.__remove_test_script(name)

    def __download_test_script(self, name):
        ret_code, stdout, stderr = self.ssh_cmd(
              name,
              '"gsutil cp gs://{}/{} ."'.format(
                  self.GOOGLE_TEST_SCRIPT_BUCKET,
                  self.GOOGLE_TEST_SCRIPT_FILE_NAME))
        self.assertEqual(ret_code, 0, "Failed to download test file. Error: {}".format(stderr))

    def __run_test_script(self, name):
        ret_code = -100
        stderr = None

        for _ in range(6):
            # Wait up to 60 seconds for notebook to be running
            ret_code, stdout, stderr = self.run_command(
                'gcloud compute ssh {} -- "python {}"'.format(
                    name,
                    self.GOOGLE_TEST_SCRIPT_FILE_NAME,
                )
            )
            if ret_code == 0:
                break
            time.sleep(10)
        self.assertEqual(ret_code, 0, "Failed to vaildate cluster in 6 attempts. Last error: {}".format(stderr))

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
        ("HA", "1.0", ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.1", ["m"]),
        ("STANDARD", "1.1", ["m"]),
        ("HA", "1.1", ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0", "m-1", "m-2"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_datalab(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @parameterized.expand([
        ("SINGLE", "1.0", ["m"]),
        ("STANDARD", "1.0", ["m"]),
        ("HA", "1.0", ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.1", ["m"]),
        ("STANDARD", "1.1", ["m"]),
        ("HA", "1.1", ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0", "m-1", "m-2"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_datalab_with_latest_docker_image(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(
            configuration,
            self.INIT_ACTION,
            dataproc_version,
            metadata="docker-image=gcr.io/cloud-datalab/datalab:latest"
        )
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @parameterized.expand([
        ("SINGLE", "1.0", ["m"]),
        ("STANDARD", "1.0", ["m"]),
        ("HA", "1.0", ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.1", ["m"]),
        ("STANDARD", "1.1", ["m"]),
        ("HA", "1.1", ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0", "m-1", "m-2"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_datalab_with_spark_packages(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(
            configuration,
            self.INIT_ACTION,
            dataproc_version,
            metadata="spark-packages=com.databricks:spark-avro_2.11:4.0.0"
        )
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @unittest.expectedFailure
    def test_datalab_with_bad_docker_image(self):
        # probably testing many configurations and versions is not crucial here
        self.createCluster(
            "SINGLE",
            self.INIT_ACTION,
            "1.2",
            metadata="docker-image=bad-image"
        )


if __name__ == '__main__':
    unittest.main()
