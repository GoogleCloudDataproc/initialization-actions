"""
This module provides testing functionality of the Atlas Initialization Action.

Test logic:
1. Run Atlas's  `quick_start.py` to populate data
2. Query for data via Atlas REST API
3. Validate that response returns expected result
"""

import hashlib
import os
import random

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class AtlasTestCase(DataprocTestCase):
    COMPONENT = 'atlas'
    OPTIONAL_COMPONENTS = ['ZOOKEEPER', 'HBASE', 'SOLR']
    OPTIONAL_COMPONENTS_HA = ['ZOOKEEPER', 'HBASE', 'SOLR']
    ATLAS_HOME = '/usr/lib/atlas'
    INIT_ACTIONS = ['atlas/atlas.sh']

    POPULATE_SCRIPT = 'populate_atlas.sh'
    VALIDATE_SCRIPT = 'validate_atlas.py'

    def verify_instance(self, instance, username='admin', password='admin'):
        # install expect on cluster, required for populate script
        self.assert_instance_command(instance,
                                     "sudo apt-get install -y expect")
        self.assert_instance_command(instance,
                                     "sudo apt-get install -y python-pip")
        self.assert_instance_command(instance,
                                     "pip install requests")

        # Upload files to populate Atlas and to verify it
        populate_atlas_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), self.POPULATE_SCRIPT)
        self.assert_command('gcloud compute scp {} {}:/tmp'.format(populate_atlas_path, instance))

        validate_atlas_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), self.VALIDATE_SCRIPT)
        self.assert_command('gcloud compute scp {} {}:/tmp'.format(validate_atlas_path, instance))

        self.assert_instance_command(
                    instance, "chmod +x /tmp/{}".format(self.POPULATE_SCRIPT))
        self.assert_instance_command(
                    instance, "chmod +x /tmp/{}".format(self.VALIDATE_SCRIPT))

        # Populate test data from Atlas provided quick_start.py
        self.assert_instance_command(
            instance, "sudo /tmp/{} {} {}".format(self.POPULATE_SCRIPT,
                                             username, password))

        # Creating Hive table
        hive_table_name = "table_{}".format(random.randint(0, 1000))
        self.assert_instance_command(
            instance, "hive -e\\\"create table {} (pkey int);\\\"".format(
                hive_table_name))

        # Creating HBase table
        hbase_table_name = "table_{}".format(random.randint(0, 1000))
        self.assert_instance_command(
            instance,
            "echo \\\"create '{}','family'\\\" | hbase shell -n".format(
                hbase_table_name))

        # Validate quick_start.py artifacts
        self.assert_instance_command(
            instance, "python /tmp/{} {} {}".format(self.VALIDATE_SCRIPT,
                                               username, password))

        # Checking Hive table info in Atlas
        self.assert_instance_command(
            instance, "python /tmp/{} {} {}".format(self.VALIDATE_SCRIPT,
                username, password, hive_table_name, 2))

        # Checking HBase table info in Atlas
        self.assert_instance_command(
            instance, "python /tmp/{} {} {}".format(self.VALIDATE_SCRIPT,
                username, password, hbase_table_name, 1))

        self.remove_test_script(os.path.join('/tmp', self.VALIDATE_SCRIPT), instance)
        self.remove_test_script(os.path.join('/tmp', self.POPULATE_SCRIPT), instance)

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
        ("HA", ["m-0", "m-1", "m-2"]),
    )
    def test_atlas(self, configuration, machine_suffixes):
        image_version = self.getImageVersion()
        if image_version < pkg_resources.parse_version("1.5") or \
            image_version > pkg_resources.parse_version("2.0"):
          return

        init_actions = self.INIT_ACTIONS
        optional_components = self.OPTIONAL_COMPONENTS
        if configuration == "HA":
            init_actions = ['kafka/kafka.sh'] + init_actions
            optional_components = self.OPTIONAL_COMPONENTS_HA
            # Kafka requires property under HA
            metadata = 'run-on-master=true'
            self.createCluster(configuration,
                                       init_actions,
                                       beta=True,
                                       metadata=metadata,
                                       timeout_in_minutes=30,
                                       optional_components=optional_components,
                                       machine_type="e2-standard-4")
        else:
            self.createCluster(configuration,
                               init_actions,
                               beta=True,
                               timeout_in_minutes=30,
                               optional_components=optional_components,
                               machine_type="e2-standard-4")

        atlas_statuses = []
        for machine_suffix in machine_suffixes:
            machine_name = "{}-{}".format(self.getClusterName(),
                                          machine_suffix)

            _, out, _ = self.assert_instance_command(
                machine_name,
                "sudo {}/bin/atlas_admin.py -u admin:admin -status".format(
                    self.ATLAS_HOME))

            # In the case of HA, the populate script should only be ran on
            # the ACTIVE Atlas node.
            if out.strip() == "ACTIVE":
                self.verify_instance(machine_name)

            atlas_statuses.append(out.strip())

        self.assertEqual(1, atlas_statuses.count("ACTIVE"))
        if configuration == "HA":
            self.assertEqual(2, atlas_statuses.count("PASSIVE"))

    @parameterized.parameters(("SINGLE", ["m"]))
    def test_atlas_overrides_admin_credentials(self, configuration,
                                               machine_suffixes):
      image_version = self.getImageVersion()
      if image_version < pkg_resources.parse_version("1.5") or \
          image_version > pkg_resources.parse_version("2.0"):
        return

        username = 'dataproc-user'
        password = 'dataproc-password'
        password_sha256 = hashlib.sha256(password.encode('utf-8')).hexdigest()
        metadata = \
          "ATLAS_ADMIN_USERNAME={},ATLAS_ADMIN_PASSWORD_SHA256={}".format(
              username, password_sha256)
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           beta=True,
                           timeout_in_minutes=30,
                           metadata=metadata,
                           optional_components=self.OPTIONAL_COMPONENTS,
                           machine_type="e2-standard-4")
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix),
                username, password)

    @parameterized.parameters("ZOOKEEPER", "HBASE", "SOLR")
    def test_atlas_fails_without_component(self, component):
      image_version = self.getImageVersion()
      if image_version < pkg_resources.parse_version("1.5") or \
          image_version > pkg_resources.parse_version("2.0"):
        return

        with self.assertRaises(AssertionError):
            self.createCluster(
                "SINGLE",
                self.INIT_ACTIONS,
                beta=True,
                timeout_in_minutes=30,
                machine_type="e2-standard-4",
                optional_components=self.OPTIONAL_COMPONENTS.remove(component))

    def test_atlas_ha_fails_without_kafka(self):
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            return

        with self.assertRaises(AssertionError):
            self.createCluster("HA",
                               self.INIT_ACTIONS,
                               timeout_in_minutes=30,
                               beta=True,
                               machine_type="e2-standard-4",
                               optional_components=self.OPTIONAL_COMPONENTS_HA)


if __name__ == '__main__':
    absltest.main()
