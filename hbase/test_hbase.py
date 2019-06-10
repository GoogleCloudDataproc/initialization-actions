import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HBaseTestCase(DataprocTestCase):
    COMPONENT = 'hbase'
    INIT_ACTIONS = ['hbase/hbase.sh']
    INIT_ACTIONS_FOR_NOT_HA = ['zookeeper/zookeeper.sh']
    GCS_BUCKET = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        _, region, _ = cls.run_command(
            "gcloud config get-value compute/region")
        _, zone, _ = cls.run_command("gcloud config get-value compute/zone")
        cls.REGION = region.strip() or zone.strip()[:-2]

    def setUp(self):
        super().setUp()
        self.GCS_BUCKET = "test-hbase-{}-{}".format(self.datetime_str(),
                                                    self.random_str())
        cmd = 'gsutil mb -c regional -l {} gs://{}'.format(
            self.REGION, self.GCS_BUCKET)
        ret_code, _, stderr = self.run_command(cmd)
        self.assertEqual(
            ret_code, 0, "Failed to create bucket {}. Last error: {}".format(
                self.GCS_BUCKET, stderr))

    def tearDown(self):
        super().tearDown()
        cmd = 'gsutil -m rm -rf gs://{}'.format(self.GCS_BUCKET)
        ret_code, _, stderr = self.run_command(cmd)
        self.assertEqual(
            ret_code, 0, "Failed to remove bucket {}. Last error: {}".format(
                self.GCS_BUCKET, stderr))

    def verify_instance(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} --command="hbase {} -r {}"'.format(
                name, 'org.apache.hadoop.hbase.IntegrationTestsDriver',
                'org.apache.hadoop.hbase.mapreduce.IntegrationTestImportTsv'))
        self.assertEqual(
            ret_code, 0,
            "Failed to validate cluster. Error: {}".format(stderr))

    @parameterized.expand(
        [
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("HA", "1.2", ["m-0"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.3", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hbase(self, configuration, dataproc_version, machine_suffixes):
        init_actions = self.INIT_ACTIONS
        if configuration != "HA":
            init_actions = self.INIT_ACTIONS_FOR_NOT_HA + init_actions
        self.createCluster(configuration, init_actions, dataproc_version)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.expand(
        [
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("HA", "1.2", ["m-0"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.3", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hbase_on_gcs(self, configuration, dataproc_version,
                          machine_suffixes):
        init_actions = self.INIT_ACTIONS
        if configuration != "HA":
            init_actions = self.INIT_ACTIONS_FOR_NOT_HA + init_actions
        test_dir = "{}-{}-{}".format(configuration.lower(),
                                     dataproc_version.replace(".", "-"),
                                     self.random_str())
        metadata = 'hbase-root-dir=gs://{}/{}'.format(self.GCS_BUCKET,
                                                      test_dir)
        self.createCluster(configuration,
                           init_actions,
                           dataproc_version,
                           metadata=metadata)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    unittest.main()
