import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class DatalabTestCase(DataprocTestCase):
    COMPONENT = 'datalab'
    INIT_ACTIONS = ['datalab/datalab.sh']
    PYTHON_3_INIT_ACTIONS = [
        'conda/bootstrap-conda.sh', 'conda/install-conda-env.sh'
    ]

    def verify_instance(self, name):
        curl_retry_params = "--retry 10 --retry-delay 10 --retry-connrefused"
        cmd = 'gcloud compute ssh {} --command={}'.format(
            name,
            '\'curl {} -L {}:8080 | grep "Google Cloud DataLab"\''.format(
                curl_retry_params, name))
        ret_code, _, stderr = self.run_command(cmd)
        self.assertEqual(
            ret_code, 0, "Failed to validate Datalab installation.\n{}".format(
                "Validation command:\n{}\nLast error:\n{}").format(
                    cmd, stderr))

    @parameterized.expand(
        [
            ("STANDARD", "1.1", ["m"], "python2"),
            ("STANDARD", "1.1", ["m"], "python3"),
            ("STANDARD", "1.2", ["m"], "python2"),
            ("STANDARD", "1.2", ["m"], "python3"),
            ("STANDARD", "1.3", ["m"], "python2"),
            ("STANDARD", "1.3", ["m"], "python3"),
            ("STANDARD", "1.4", ["m"], "python2"),
            ("STANDARD", "1.4", ["m"], "python3"),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_datalab(self, configuration, dataproc_version, machine_suffixes,
                     python):
        init_actions = self.INIT_ACTIONS
        metadata = 'DOCKER_INIT_ACTION_DIR={}/docker'.format(
            self.INIT_ACTIONS_REPO)
        if python == "python3":
            init_actions = self.PYTHON_3_INIT_ACTIONS + init_actions
            metadata += ',CONDA_PACKAGES="python==3.5"'

        self.createCluster(configuration,
                           init_actions,
                           dataproc_version,
                           metadata=metadata,
                           scopes='cloud-platform',
                           timeout_in_minutes=20)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    unittest.main()
