import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
FLAGS(sys.argv)


class JupyterTestCase(DataprocTestCase):
    COMPONENT = 'jupyter'
    INIT_ACTIONS = ['jupyter/jupyter.sh']

    def verify_instance(self, name, jupyter_port):
        verify_cmd = "curl {} -L {}:{} | grep 'Jupyter Notebook'".format(
            "--retry 10 --retry-delay 10 --retry-connrefused", name,
            jupyter_port)
        self.assert_instance_command(name, verify_cmd)

    @parameterized.expand(
        [
            ("SINGLE", ["m"]),
            ("STANDARD", ["m"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_jupyter(self, configuration, machine_suffixes):
        metadata = 'INIT_ACTIONS_REPO={}'.format(self.INIT_ACTIONS_REPO)
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata=metadata,
                           timeout_in_minutes=15,
                           machine_type="n1-standard-2")

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix), "8123")

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_jupyter_with_metadata(self, configuration, machine_suffixes):
        jupyter_port = "8125"

        metadata = 'INIT_ACTIONS_REPO={}'.format(self.INIT_ACTIONS_REPO)
        metadata += ',JUPYTER_PORT={},JUPYTER_CONDA_PACKAGES={}'.format(
            jupyter_port, "numpy:pandas:scikit-learn")

        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           metadata=metadata,
                           timeout_in_minutes=15,
                           machine_type="n1-standard-2")

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix),
                jupyter_port)


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
