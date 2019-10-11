import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
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
        self.assert_command('gsutil -m rm -rf gs://{}'.format(self.GCS_BUCKET))
        super().tearDown()

    def verify_instance(self, name):
        self.assert_instance_command(
            name, "hbase {} -r {}".format(
                'org.apache.hadoop.hbase.IntegrationTestsDriver',
                'org.apache.hadoop.hbase.mapreduce.IntegrationTestImportTsv'))

    def buildParameters():
        """Builds parameters from flags arguments passed to the test."""
        flags_parameters = FLAGS.params
        params = []
        if not flags_parameters[0]:
            # Default parameters
            params = [
                ("SINGLE", "1.2", ["m"]),
                ("STANDARD", "1.2", ["m"]),
                ("HA", "1.2", ["m-0"]),
                ("SINGLE", "1.3", ["m"]),
                ("STANDARD", "1.3", ["m"]),
                ("HA", "1.3", ["m-0"])
            ]
        else:
            for param in flags_parameters:
                (config, version, machine_suffixes) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                params.append((config, version, machine_suffixes))
        return params

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hbase(self, configuration, dataproc_version, machine_suffixes):
        init_actions = self.INIT_ACTIONS
        if configuration != "HA":
            init_actions = self.INIT_ACTIONS_FOR_NOT_HA + init_actions
        self.createCluster(configuration,
                           init_actions,
                           dataproc_version,
                           machine_type="n1-standard-2")

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.expand(
        buildParameters(),
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
                           metadata=metadata,
                           machine_type="n1-standard-2")

        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()

