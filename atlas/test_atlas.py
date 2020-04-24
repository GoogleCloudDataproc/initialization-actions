"""
This module provides testing functionality of the Atlas Init Action.

Test logic:
1. Run Atlas's quick_start to populate data
2. Query for data via Atlas REST API
3. Validate that response returns expected result
"""
import hashlib
import random
import unittest
import os

from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase


class AtlasTestCase(DataprocTestCase):
    COMPONENT = 'atlas'
    ATLAS_HOME = '/usr/lib/atlas/apache-atlas-1.2.0'
    INIT_ACTION = 'gs://roderickyao/atlas/atlas.sh'  # TODO(yhqs540): change to official init-action before merging
    ZK_INIT_ACTION = 'gs://dataproc-initialization-actions/zookeeper/zookeeper.sh'
    HBASE_INIT_ACTION = 'gs://dataproc-initialization-actions/hbase/hbase.sh'
    SOLR_INIT_ACTION = 'gs://dataproc-initialization-actions/solr/solr.sh'
    KAFKA_INIT_ACTION = 'gs://dataproc-initialization-actions/kafka/kafka.sh'

    POPULATE_SCRIPT = 'populate_atlas.sh'
    VALIDATE_SCRIPT = 'validate_atlas.py'

    def verify_instance(self, name, username='admin', password='admin'):
        # install expect on cluster, required for populate script
        self.run_command_on_cluster(name, "yes | sudo apt-get install expect")

        # upload files to populate Atlas and to verify it
        populate_atlas_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.POPULATE_SCRIPT)
        populate_atlas_remote_path = os.path.join('/tmp', self.POPULATE_SCRIPT)
        validate_atlas_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.VALIDATE_SCRIPT)
        validate_atlas_remote_path = os.path.join('/tmp', self.VALIDATE_SCRIPT)

        self.upload_test_file(populate_atlas_path, name, populate_atlas_remote_path)
        self.upload_test_file(validate_atlas_path, name, validate_atlas_remote_path)

        # populate test data from Atlas provided quick_start
        self.run_command_on_cluster(name, "chmod +x {}".format(populate_atlas_remote_path))
        self.run_command_on_cluster(name, "sudo {} {} {}".format(
            populate_atlas_remote_path, username, password
        ))

        # creating hive table
        hive_table_name = "table_{}".format(random.randint(0, 1000))
        self.run_command_on_cluster(name,
                                    "hive -e\\\"create table {} (pkey int);\\\"".format(hive_table_name))

        # creating Hbase table
        hbase_table_name = "table_{}".format(random.randint(0, 1000))
        self.run_command_on_cluster(name, "echo \\\"create '{}','family'\\\" | hbase shell -n".format(
            hbase_table_name))

        # validate quick_start artifacts
        self.run_command_on_cluster(name, "python {} {} {}".format(
            validate_atlas_remote_path,
            username,
            password
        ))

        # checking Hive table info in Atlas
        self.run_command_on_cluster(name, "python {} {} {}".format(
            os.path.join('/tmp', self.VALIDATE_SCRIPT),
            username,
            password,
            hive_table_name,
            2
        ))

        # checking HBase table info in Atlas
        self.run_command_on_cluster(name, "python {} {} {}".format(
            os.path.join('/tmp', self.VALIDATE_SCRIPT),
            username,
            password,
            hbase_table_name,
            1
        ))

    @parameterized.expand([
        # ("SINGLE", "1.5", ["m"]),
        ("STANDARD", "1.5", ["m"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_atlas(self, configuration, dataproc_version, machine_suffixes):
        init_actions = ",".join([
            self.ZK_INIT_ACTION,
            self.HBASE_INIT_ACTION,
            self.SOLR_INIT_ACTION,
            self.INIT_ACTION
        ])
        self.createCluster(configuration, init_actions, dataproc_version, timeout_in_minutes=30)

        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    def test_atlas_overrides_admin_credentials(self):
        init_actions = ",".join([
            self.ZK_INIT_ACTION,
            self.HBASE_INIT_ACTION,
            self.SOLR_INIT_ACTION,
            self.INIT_ACTION
        ])
        username = 'dataproc-user'
        password = 'dataproc-password'
        metadata = "ATLAS_ADMIN_USERNAME={},ATLAS_ADMIN_PASSWORD_SHA256={}".format(
            username,
            hashlib.sha256(password.encode('utf-8')).hexdigest()
        )
        self.createCluster("SINGLE", init_actions, "1.5", timeout_in_minutes=30, metadata=metadata)
        self.verify_instance("{}-m".format(self.getClusterName()), username, password)

    @parameterized.expand([
        ("HA", "1.5", ["m-0", "m-1", "m-2"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_atlas_HA(self, configuration, dataproc_version, machine_suffixes):
        init_actions = ",".join([
            self.HBASE_INIT_ACTION,
            self.KAFKA_INIT_ACTION,
            self.SOLR_INIT_ACTION,
            self.INIT_ACTION,
        ])
        self.createCluster(configuration, init_actions, dataproc_version, timeout_in_minutes=30)

        atlas_statuses = []
        for machine_suffix in machine_suffixes:
            machine_name = "{}-{}".format(self.getClusterName(), machine_suffix)
            self.verify_instance(machine_name)
            _, out, _ = self.run_command_on_cluster(
                machine_name,
                "sudo {}}/bin/atlas_admin.py -u admin:admin -status".format(ATLAS_HOME)
            )
            atlas_statuses.append(out.strip())
        self.assertEqual(1, atlas_statuses.count("ACTIVE"))
        self.assertEqual(2, atlas_statuses.count("PASSIVE"))

    def test_atlas_fails_without_zookeeper(self):
        init_actions = ",".join([
            self.HBASE_INIT_ACTION,
            self.SOLR_INIT_ACTION,
            self.INIT_ACTION
        ])
        with self.assertRaises(AssertionError):
            self.createCluster("SINGLE", init_actions, "1.5")

    def test_atlas_fails_without_hbase(self):
        init_actions = ",".join([
            self.ZK_INIT_ACTION,
            self.SOLR_INIT_ACTION,
            self.INIT_ACTION
        ])
        with self.assertRaises(AssertionError):
            self.createCluster("SINGLE", init_actions, "1.5")

    def test_atlas_fails_without_solr(self):
        init_actions = ",".join([
            self.ZK_INIT_ACTION,
            self.HBASE_INIT_ACTION,
            self.INIT_ACTION
        ])
        with self.assertRaises(AssertionError):
            self.createCluster("SINGLE", init_actions, "1.5")

    def test_atlas_fails_without_kafka_on_HA(self):
        init_actions = ",".join([
            self.HBASE_INIT_ACTION,
            self.SOLR_INIT_ACTION,
            self.INIT_ACTION
        ])
        with self.assertRaises(AssertionError):
            self.createCluster("HA", init_actions, "1.5")


if __name__ == '__main__':
    unittest.main()
