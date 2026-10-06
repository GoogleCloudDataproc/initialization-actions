import json
import packaging.version
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class PipCachedTestCase(DataprocTestCase):
    COMPONENT = "python"
    INIT_ACTIONS = ["python/pip-install-cached.sh"]

    # Test packages
    PIP_PKGS = ["pandas-gbq"]
    GCS_BUCKET = None

    def setUp(self):
        super().setUp()
        self.GCS_BUCKET = "test-pip-cached-{}-{}".format(self.datetime_str(),
                                                         self.random_str())
        self.assert_command('gsutil mb -c regional -l {} gs://{}'.format(
            self.REGION, self.GCS_BUCKET))

    def tearDown(self):
        self.assert_command('gsutil -m rm -rf gs://{}'.format(self.GCS_BUCKET))
        super().tearDown()

    def _verify_pip_packages(self, instance, pip_packages):
        # Try both pip and python3 -m pip to be robust
        _, stdout, _ = self.assert_instance_command(instance, "python3 -m pip list")
        installed_packages = self._parse_packages(stdout)
        for package in pip_packages:
            if package not in installed_packages:
                # Try pip list as fallback
                _, stdout_pip, _ = self.assert_instance_command(instance, "pip list")
                installed_packages_pip = self._parse_packages(stdout_pip)
                self.assertIn(
                    package, installed_packages_pip,
                    "Expected package {} to be installed, but wasn't."
                    " Packages installed (python3 -m pip): {}"
                    " Packages installed (pip): {}".format(
                        package, installed_packages, installed_packages_pip))

    def _verify_cache_created(self):
        ret_code, stdout, _ = self.run_command("gsutil ls gs://{}".format(self.GCS_BUCKET))
        self.assertEqual(ret_code, 0, "Failed to list bucket")
        self.assertTrue(len(stdout.strip()) > 0, "Cache was not created in GCS")

    @staticmethod
    def _parse_packages(stdout):
        return set(
            l.split()[0] for l in stdout.splitlines() if not l.startswith("#"))

    @parameterized.parameters(
        ("STANDARD", PIP_PKGS),
    )
    def test_pip_cached(self, configuration, pip_packages):
        metadata = "'PIP_PACKAGES={},CACHE_BUCKET={}'".format(
            " ".join(pip_packages), self.GCS_BUCKET)
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata=metadata)

        instance_name = self.getClusterName() + "-m"
        self._verify_pip_packages(instance_name, pip_packages)
        self._verify_cache_created()

if __name__ == "__main__":
    absltest.main()
