import random
import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RapidsTestCase(DataprocTestCase):
    COMPONENT = 'rapids'
    #INIT_ACTION = 'gs://dataproc-initialization-actions/rapids/rapids.sh'
    #METADATA = 'INIT_ACTIONS_REPO=https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git,INIT_ACTIONS_BRANCH=master'
    INIT_ACTION = 'gs://rapidsai-test-1/rapids/rapids.sh'
    METADATA = 'INIT_ACTIONS_REPO=https://github.com/randerzander/dataproc-initialization-actions.git,INIT_ACTIONS_BRANCH=master'
    TEST_SCRIPT_FILE_NAME = 'verify_rapids.py'

    def verify_instance(self, name):
        self.__verify_gpu_driver(name)

    def __verify_gpu_driver(self, name):
        ret_code, stdout, stderr = self.run_command(
            "gcloud compute ssh {} -- \"nvidia-smi\"".format(
                name
            )
        )
        self.assertEqual(ret_code, 0, "Failed to install GPU driver. Error: {}".format(stderr))

    def __verify_rapids(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "source activate RAPIDS && python {}"'.format(
                name,
                self.TEST_SCRIPT_FILE_NAME,
            )
        )
        self.assertEqual(ret_code, 0, "Failed to validate RAPIDS install. Error: {}".format(stderr))

    @parameterized.expand([
        ("STANDARD", "1.3", ["m"], 1, 2)
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_rapids(self, configuration, dataproc_version, machine_suffixes, coordinators, workers):
        self.createCluster(configuration, self.INIT_ACTION,
                           dataproc_version, metadata=self.METADATA, beta=True,
                           zone='us-east1-c',
                           master_accelerator='type=nvidia-tesla-t4',
                           worker_accelerator='type=nvidia-tesla-t4',
                           optional_components='ANACONDA')

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )
        for machine in coordinators:
            self.__verify_rapids(machine)


if __name__ == '__main__':
    unittest.main()
