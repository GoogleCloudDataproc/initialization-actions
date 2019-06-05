import random
import unittest
import os

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RapidsTestCase(DataprocTestCase):
    COMPONENT = 'rapids'
    INIT_ACTION = 'gs://dataproc-initialization-actions/rapids/rapids.sh'
    METADATA = 'INIT_ACTIONS_REPO=https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git,INIT_ACTIONS_BRANCH=master'
    TEST_SCRIPT_FILE_NAME = 'verify_rapids.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME
            ),
            name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "/opt/conda/anaconda/envs/RAPIDS/bin/python {}"'.format(
                name,
                self.TEST_SCRIPT_FILE_NAME,
            )
        )
        self.assertEqual(ret_code, 0, "Failed to validate RAPIDS install. Last error: {}".format(stderr))

    @parameterized.expand([
        ("STANDARD", "1.3", ["m"])
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_rapids(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTION,
                           dataproc_version, metadata=self.METADATA, beta=True,
                           master_accelerator='type=nvidia-tesla-p100',
                           worker_accelerator='type=nvidia-tesla-p100',
                           optional_components='ANACONDA',
                           timeout_in_minutes=20)

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )


if __name__ == '__main__':
    unittest.main()
