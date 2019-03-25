import unittest
import datetime
from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HBaseTestCase(DataprocTestCase):
    COMPONENT = 'hbase'
    INIT_ACTION = 'gs://dataproc-initialization-actions/hbase/hbase.sh'
    HELPER_ACTIONS = 'gs://dataproc-initialization-actions/zookeeper/zookeeper.sh'
    GCS_BUCKET = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        _, region, _ = cls.run_command("gcloud config get-value compute/region")
        cls.REGION = region.strip() or "global"

    def setUp(self):
        super().setUp()
        self.GCS_BUCKET = "test-{}-bucket".format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        ret_code, stdout, stderr = self.run_command(
            'gsutil mb -c regional -l {} gs://{}'.format(self.REGION, self.GCS_BUCKET)
        )
        self.assertEqual(ret_code, 0, "Failed to create bucket {}. Last error: {}".format(
            self.GCS_BUCKET, stderr))

    def tearDown(self):
        super().tearDown()
        ret_code, stdout, stderr = self.run_command(
            'gsutil rm -r gs://{}'.format(self.GCS_BUCKET)
        )
        self.assertEqual(ret_code, 0, "Failed to remove bucket {}. Last error: {}".format(
            self.GCS_BUCKET, stderr))

    def verify_instance(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} --command '
            '"hbase org.apache.hadoop.hbase.IntegrationTestsDriver -r {}"'.format(
                name,
                "org.apache.hadoop.hbase.mapreduce.IntegrationTestImportTsv"
            )
        )
        self.assertEqual(ret_code, 0, "Failed to validate cluster. Error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.2", ["m"], HELPER_ACTIONS),
        ("STANDARD", "1.2", ["m"], HELPER_ACTIONS),
        ("HA", "1.2", ["m-0"], ""),
        ("SINGLE", "1.3", ["m"], HELPER_ACTIONS),
        ("STANDARD", "1.3", ["m"], HELPER_ACTIONS),
        ("HA", "1.3", ["m-0"], ""),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hbase(self, configuration, dataproc_version, machine_suffixes, helper_actions):
        if helper_actions:
            init_actions = "{},{}".format(helper_actions, self.INIT_ACTION)
        else:
            init_actions = self.INIT_ACTION
        self.createCluster(configuration, init_actions, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @parameterized.expand([
        ("SINGLE", "1.2", ["m"], HELPER_ACTIONS),
        ("STANDARD", "1.2", ["m"], HELPER_ACTIONS),
        ("HA", "1.2", ["m-0"], ""),
        ("SINGLE", "1.3", ["m"], HELPER_ACTIONS),
        ("STANDARD", "1.3", ["m"], HELPER_ACTIONS),
        ("HA", "1.3", ["m-0"], ""),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hbase_on_gcs(self, configuration, dataproc_version, machine_suffixes, helper_actions):
        if helper_actions:
            init_actions = "{},{}".format(helper_actions, self.INIT_ACTION)
        else:
            init_actions = self.INIT_ACTION
        self.createCluster(configuration, init_actions, dataproc_version,
                           metadata='hbase-root-dir=gs://{}/'.format(
                               self.GCS_BUCKET)
                           )
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )


if __name__ == '__main__':
    unittest.main()
