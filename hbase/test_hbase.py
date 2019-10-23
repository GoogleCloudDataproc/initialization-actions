from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


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
        self.assert_command('gsutil -m rm -rf gs://{}'.format(self.GCS_BUCKET))
        super().tearDown()

    def verify_instance(self, name):
        self.assert_instance_command(
            name, "hbase {} -r {}".format(
                'org.apache.hadoop.hbase.IntegrationTestsDriver',
                'org.apache.hadoop.hbase.mapreduce.IntegrationTestImportTsv'))

    @parameterized.parameters(
            ("SINGLE", ["m"]),
            ("STANDARD", ["m"]),
            ("HA", ["m-0"]),
    )
    def test_hbase(self, configuration, machine_suffixes):
        init_actions = self.INIT_ACTIONS
        if configuration != "HA":
            init_actions = self.INIT_ACTIONS_FOR_NOT_HA + init_actions
        self.createCluster(configuration,
                           init_actions,
                           machine_type="n1-standard-2")

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
            ("SINGLE", ["m"]),
            ("STANDARD", ["m"]),
            ("HA", ["m-0"]),
    )
    def test_hbase_on_gcs(self, configuration,
                          machine_suffixes):
        init_actions = self.INIT_ACTIONS
        if configuration != "HA":
            init_actions = self.INIT_ACTIONS_FOR_NOT_HA + init_actions
        metadata = 'hbase-root-dir=gs://{}/test-dir'.format(self.GCS_BUCKET)
        self.createCluster(configuration,
                           init_actions,
                           metadata=metadata,
                           machine_type="n1-standard-2")

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()

