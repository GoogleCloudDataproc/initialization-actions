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

  def _verify_python_version(self, expected_python):
    _, stdout, _ = self.assert_instance_command(self.getClusterName() + "-m", CONDA_BINARY + " info --json")
    python_version = json.loads(stdout)[PYTHON_VERSION_KEY]
    self.assertTrue(python_version.startswith(expected_python),
                    "Unexpected Python version. Wanted {}, got {}".format(expected_python, python_version))

  def _verify_conda_packages(self, conda_packages):
    _, stdout, _ = self.assert_instance_command(self.getClusterName() + "-m", CONDA_BINARY + " list")
    installed_packages = self._parse_packages(stdout)
    for package in conda_packages:
      self.assertIn(package, installed_packages,
                    "Expected package {} to be installed, but wasn't. Packages installed: {}".format(package, installed_packages))


  def _verify_pip_packages(self, pip_packages):
    _, stdout, _ = self.assert_instance_command(self.getClusterName() + "-m", PIP_BINARY + " list")
    installed_packages = self._parse_packages(stdout)
    for package in pip_packages:
      self.assertIn(package, installed_packages,
                    "Expected package {} to be installed, but wasn't. Packages installed: {}".format(package, installed_packages))

  def _parse_packages(self, stdout):
    packages = set()
    for line in stdout.splitlines():
      if not line.startswith("#"):
        packages.add(line.split()[0])

    return packages

  @parameterized.expand(
      [
          ("STANDARD", "1.0", [], [], "3.5"),
          ("STANDARD", "1.0", ["numpy", "pandas", "jupyter"], ["pandas-gbq"], "3.5"),
          ("STANDARD", "1.1", [], [], "3.5"),
          ("STANDARD", "1.1", ["numpy", "pandas", "jupyter"], ["pandas-gbq"], "3.5"),
          ("STANDARD", "1.2", [], [], "3.6"),
          ("STANDARD", "1.2", ["numpy", "pandas", "jupyter"], ["pandas-gbq"], "3.6"),
          ("STANDARD", "1.3", [], [], "3.6"),
          ("STANDARD", "1.3", ["numpy", "pandas", "jupyter"], ["pandas-gbq"], "3.6"),
          ("STANDARD", "1.4", [], [], "3.6"),
          ("STANDARD", "1.4", ["numpy", "pandas", "jupyter"], ["pandas-gbq"], "3.6"),
      ],
      testcase_func_name=DataprocTestCase.generate_verbose_test_name)
  def test_conda(self, configuration, dataproc_version, conda_packages, pip_packages, expected_python):
    metadata = "'CONDA_PACKAGES={},PIP_PACKAGES={}'".format(
        " ".join(conda_packages), " ".join(pip_packages))
    self.createCluster(configuration,
                       self.INIT_ACTIONS,
                       dataproc_version,
                       machine_type="n1-standard-2",
                       metadata=metadata)

    self._verify_python_version(expected_python)
    self._verify_pip_packages(pip_packages)
    self._verify_conda_packages(conda_packages)


if __name__ == "__main__":
  unitttest.main()
