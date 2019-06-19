import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class JupyterTestCase(DataprocTestCase):
    COMPONENT = 'jupyter'
    INIT_ACTIONS = ['jupyter/jupyter.sh']

    def verify_instance(self, name, jupyter_port):
        curl_retry_params = "--retry 10 --retry-delay 10 --retry-connrefused"
        cmd = 'gcloud compute ssh {} --command={}'.format(
            name, '\'curl {} -L {}:{} | grep "Jupyter Notebook"\''.format(
                curl_retry_params, name, jupyter_port))
        ret_code, _, stderr = self.run_command(cmd)
        self.assertEqual(
            ret_code, 0, "Failed to validate Jupyter installation.\n{}".format(
                "Validation command:\n{}\nLast error:\n{}").format(
                    cmd, stderr))

    @parameterized.expand(
        [
            ("SINGLE", "1.1", ["m"]),
            ("STANDARD", "1.1", ["m"]),
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_jupyter(self, configuration, dataproc_version, machine_suffixes):
        metadata = 'INIT_ACTIONS_REPO={}'.format(self.INIT_ACTIONS_REPO)
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           metadata=metadata,
                           timeout_in_minutes=20)

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix), "8123")

    @parameterized.expand(
        [
            ("SINGLE", "1.1", ["m"]),
            ("STANDARD", "1.1", ["m"]),
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_jupyter_with_metadata(self, configuration, dataproc_version,
                                   machine_suffixes):
        jupyter_port = "8125"

        metadata = 'INIT_ACTIONS_REPO={}'.format(self.INIT_ACTIONS_REPO)
        metadata += ',JUPYTER_PORT={},JUPYTER_CONDA_PACKAGES={}'.format(
            jupyter_port, "numpy:pandas:scikit-learn")

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           metadata=metadata,
                           timeout_in_minutes=20)

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix),
                jupyter_port)


if __name__ == '__main__':
    unittest.main()
