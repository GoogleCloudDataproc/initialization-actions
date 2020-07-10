import pkg_resources
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
        # Skip on 2.0+ version of Dataproc because it's not supported
        if self.getImageVersion() >= pkg_resources.parse_version("2.0"):
            return

        init_actions = self.INIT_ACTIONS
        if configuration != "HA":
            init_actions = self.INIT_ACTIONS_FOR_NOT_HA + init_actions
        self.createCluster(configuration, init_actions)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
        ("HA", ["m-0"]),
    )
    def test_hbase_on_gcs(self, configuration, machine_suffixes):
        # Skip on 2.0+ version of Dataproc because it's not supported
        if self.getImageVersion() >= pkg_resources.parse_version("2.0"):
            return

        init_actions = self.INIT_ACTIONS
        if configuration != "HA":
            init_actions = self.INIT_ACTIONS_FOR_NOT_HA + init_actions

        metadata = 'hbase-root-dir=gs://{}/test-dir'.format(self.GCS_BUCKET)
        if self.getImageVersion() > pkg_resources.parse_version("1.4"):
            self.initClusterName(configuration)
            hdfs_host = self.getClusterName()
            if configuration != "HA":
                hdfs_host += '-m'
            metadata += ',hbase-wal-dir=hdfs://{}/hbase-wal'.format(hdfs_host)

        self.createCluster(
            configuration,
            init_actions,
            metadata=metadata)

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
