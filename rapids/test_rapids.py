import os
import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class RapidsTestCase(DataprocTestCase):
    COMPONENT = 'rapids'
    INIT_ACTIONS = ['rapids/rapids.sh']
    TEST_SCRIPT_FILE_NAME = 'verify_rapids.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} --command="{}"'.format(
                name, "/opt/conda/anaconda/envs/RAPIDS/bin/python {}".format(
                    self.TEST_SCRIPT_FILE_NAME)))
        self.assertEqual(
            ret_code, 0,
            "Failed to validate RAPIDS install. Last error: {}".format(stderr))

    @parameterized.expand(
        [("STANDARD", "1.3", ["m"])],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_rapids(self, configuration, dataproc_version, machine_suffixes):
        metadata = ('RAPIDS_INIT_ACTION_DIR={}/rapids'
                    ',CONDA_INIT_ACTION_DIR={}/conda').format(
            self.INIT_ACTIONS_REPO, self.INIT_ACTIONS_REPO)

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           metadata=metadata,
                           beta=True,
                           master_accelerator='type=nvidia-tesla-p100',
                           worker_accelerator='type=nvidia-tesla-p100',
                           optional_components='ANACONDA',
                           timeout_in_minutes=20)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    unittest.main()
