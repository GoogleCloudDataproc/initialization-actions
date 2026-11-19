import json
import packaging.version
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

CONDA_BINARY = "/opt/conda/bin/conda"

class CondaCachedTestCase(DataprocTestCase):
    COMPONENT = "python"
    INIT_ACTIONS = ["python/conda-install-cached.sh"]

    # Test packages
    CONDA_PKGS = ["numpy"]
    GCS_BUCKET = None

    def setUp(self):
        super().setUp()
        self.GCS_BUCKET = "test-conda-cached-{}-{}".format(self.datetime_str(),
                                                           self.random_str())
        self.assert_command('gsutil mb -c regional -l {} gs://{}'.format(
            self.REGION, self.GCS_BUCKET))

    def tearDown(self):
        if self.GCS_BUCKET:
            self.run_command('gsutil -m rm -rf gs://{}'.format(self.GCS_BUCKET))
        super().tearDown()

    def _verify_conda_packages(self, instance, conda_packages):
        _, stdout, _ = self.assert_instance_command(instance, CONDA_BINARY + " list")
        installed_packages = self._parse_packages(stdout)
        for package in conda_packages:
            self.assertIn(
                package, installed_packages,
                "Expected package {} to be installed, but wasn't."
                " Packages installed: {}".format(package, installed_packages))

    def _verify_cache_created(self):
        ret_code, stdout, _ = self.run_command("gsutil ls gs://{}".format(self.GCS_BUCKET))
        self.assertEqual(ret_code, 0, "Failed to list bucket")
        self.assertTrue(len(stdout.strip()) > 0, "Cache was not created in GCS")

    @staticmethod
    def _parse_packages(stdout):
        return set(
            l.split()[0] for l in stdout.splitlines() if not l.startswith("#"))

    @parameterized.parameters(
        ("STANDARD", CONDA_PKGS),
    )
    def test_conda_cached(self, configuration, conda_packages):
        metadata = "'CONDA_PACKAGES={},CACHE_BUCKET={}'".format(
            " ".join(conda_packages), self.GCS_BUCKET)
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            metadata=metadata)

        instance_name = self.getClusterName() + "-m"
        self._verify_conda_packages(instance_name, conda_packages)
        self._verify_cache_created()

if __name__ == "__main__":
    absltest.main()
