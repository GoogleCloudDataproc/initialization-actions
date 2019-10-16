import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
FLAGS(sys.argv)


class HBaseTestCase(DataprocTestCase):
    COMPONENT = 'hbase'
    INIT_ACTIONS = ['hbase/hbase.sh']
    INIT_ACTIONS_FOR_NOT_HA = ['zookeeper/zookeeper.sh']
    GCS_BUCKET = None

    def setUp(self):
        super().setUp()
        self.GCS_BUCKET = "test-hbase-{}-{}".format(self.datetime_str(),
                                                    self.random_str())
        self.assert_command('gsutil mb -c regional -l {} gs://{}'.format(
            self.REGION, self.GCS_BUCKET))

    def tearDown(self):
        super().tearDown()
        self.assert_command('gsutil -m rm -rf gs://{}'.format(self.GCS_BUCKET))

    def verify_instance(self, name):
        self.assert_instance_command(
            name, "hbase {} -r {}".format(
                'org.apache.hadoop.hbase.IntegrationTestsDriver',
                'org.apache.hadoop.hbase.mapreduce.IntegrationTestImportTsv'))

    @parameterized.expand(
        [
            ("SINGLE", ["m"]),
            ("STANDARD", ["m"]),
            ("HA", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hbase(self, configuration, machine_suffixes):
        init_actions = self.INIT_ACTIONS
        if configuration != "HA":
            init_actions = self.INIT_ACTIONS_FOR_NOT_HA + init_actions
        self.createCluster(configuration, init_actions)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.expand(
        [
            ("SINGLE", ["m"]),
            ("STANDARD", ["m"]),
            ("HA", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hbase_on_gcs(self, configuration,
                          machine_suffixes):
        init_actions = self.INIT_ACTIONS
        if configuration != "HA":
            init_actions = self.INIT_ACTIONS_FOR_NOT_HA + init_actions
        test_dir = "{}-{}-{}".format(configuration.lower(),
                                     FLAGS.image_version.replace(".", "-"),
                                     self.random_str())
        metadata = 'hbase-root-dir=gs://{}/{}'.format(self.GCS_BUCKET,
                                                      test_dir)
        self.createCluster(configuration,
                           init_actions,
                           metadata=metadata)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()

