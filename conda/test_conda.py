import json
import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

CONDA_BINARY = "/opt/conda/bin/conda"
PIP_BINARY = "/opt/conda/bin/pip"
PYTHON_VERSION_KEY = "python_version"


class CondaTestCase(DataprocTestCase):
    COMPONENT = "conda"
    INIT_ACTIONS = ["conda/bootstrap-conda.sh", "conda/install-conda-env.sh"]

    # Test packages
    CONDA_PKGS = ["numpy", "pandas", "jupyter"]
    PIP_PKGS = ["pandas-gbq"]

    def _verify_python_version(self, instance, expected_python):
        _, stdout, _ = self.assert_instance_command(
            instance, CONDA_BINARY + " info --json")
        python_version = json.loads(stdout)[PYTHON_VERSION_KEY]
        self.assertTrue(
            python_version.startswith(expected_python),
            "Unexpected Python version. Wanted {}, got {}".format(
                expected_python, python_version))

    def _verify_conda_packages(self, instance, conda_packages):
        _, stdout, _ = self.assert_instance_command(instance,
                                                    CONDA_BINARY + " list")
        installed_packages = self._parse_packages(stdout)
        for package in conda_packages:
            self.assertIn(
                package, installed_packages,
                "Expected package {} to be installed, but wasn't."
                " Packages installed: {}".format(package, installed_packages))

    def _verify_pip_packages(self, instance, pip_packages):
        _, stdout, _ = self.assert_instance_command(instance,
                                                    PIP_BINARY + " list")
        installed_packages = self._parse_packages(stdout)
        for package in pip_packages:
            self.assertIn(
                package, installed_packages,
                "Expected package {} to be installed, but wasn't."
                " Packages installed: {}".format(package, installed_packages))

    @staticmethod
    def _parse_packages(stdout):
        return set(l.split()[0] for l in stdout.splitlines()
                   if not l.startswith("#"))

    @parameterized.expand(
        [
            ("STANDARD", "1.0", "3.5", [], []),
            ("STANDARD", "1.0", "3.5", CONDA_PKGS, PIP_PKGS),
            ("STANDARD", "1.1", "3.5", [], []),
            ("STANDARD", "1.1", "3.5", CONDA_PKGS, PIP_PKGS),
            ("STANDARD", "1.2", "3.6", [], []),
            ("STANDARD", "1.2", "3.6", CONDA_PKGS, PIP_PKGS),
            ("STANDARD", "1.3", "3.6", [], []),
            ("STANDARD", "1.3", "3.6", CONDA_PKGS, PIP_PKGS),
            ("STANDARD", "1.4", "3.6", [], []),
            ("STANDARD", "1.4", "3.6", CONDA_PKGS, PIP_PKGS),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_conda(self, configuration, dataproc_version, expected_python,
                   conda_packages, pip_packages):
        metadata = "'CONDA_PACKAGES={},PIP_PACKAGES={}'".format(
            " ".join(conda_packages), " ".join(pip_packages))
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           machine_type="n1-standard-2",
                           metadata=metadata)

        instance_name = self.getClusterName() + "-m"
        self._verify_python_version(instance_name, expected_python)
        self._verify_pip_packages(instance_name, pip_packages)
        self._verify_conda_packages(instance_name, conda_packages)


if __name__ == "__main__":
    unittest.main()
