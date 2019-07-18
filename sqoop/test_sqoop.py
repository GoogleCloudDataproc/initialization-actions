import unittest
import datetime
from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class SqoopTestCase(DataprocTestCase):
    COMPONENT = 'sqoop'
    INIT_ACTIONS = ['sqoop/sqoop.sh']
    SQL_TO_HDFS_HELPER_ACTIONS = ['cloud-sql-proxy/cloud-sql-proxy.sh']
    SQL_TO_BIGTABLE_HELPER_ACTIONS = \
        ['cloud-sql-proxy/cloud-sql-proxy.sh', 'bigtable/bigtable.sh']
    SQL_TO_HBASE_HELPER_ACTIONS = \
        ['cloud-sql-proxy/cloud-sql-proxy.sh', 'zookeeper/zookeeper.sh', 'hbase/hbase.sh']
    CLOUD_SQL_INSTANCE_NAME = None
    CLOUD_BIGTABLE_INSTANCE_NAME = None
    TEST_DB_URI = 'gs://example-db-poli/sqldumpfile.gz'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        _, region, _ = cls.run_command("gcloud config get-value compute/region")
        cls.REGION = region.strip() or "global"
        _, zone, _ = cls.run_command("gcloud config get-value compute/zone")
        cls.ZONE = zone.strip()
        _, project, _ = cls.run_command("gcloud config get-value project")
        project = project.strip()
        cls.CLOUD_SQL_METADATA = '{}:{}'.format(project, cls.REGION)
        cls.CLOUD_SQL_INSTANCE_NAME = "test-{}-sql-db".format(
            datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        )
        cls.CLOUD_BIGTABLE_INSTANCE_NAME = "test-{}-db".format(
            datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        )
        cls.CLOUD_BIGTABLE_METADATA = "bigtable-instance={},bigtable-project={}" \
            .format(cls.CLOUD_BIGTABLE_INSTANCE_NAME, project)

        ret_code, _, stderr = cls.run_command(
            'gcloud sql instances create {} --region {}'.format(
                cls.CLOUD_SQL_INSTANCE_NAME, cls.REGION)
        )
        assert ret_code == 0, "Failed to create sql instance {}. Last error: {}".format(
            cls.CLOUD_SQL_INSTANCE_NAME, stderr)

        ret_code, _, stderr = cls.run_command(
            'gcloud sql import sql {} {}'.format(
                cls.CLOUD_SQL_INSTANCE_NAME, cls.TEST_DB_URI)
        )
        assert ret_code == 0, "Failed to import test employees db {} to {}. Last error: {}".format(
            cls.TEST_DB_URI, cls.CLOUD_SQL_INSTANCE_NAME, stderr)

        ret_code, _, stderr = cls.run_command(
            'gcloud beta bigtable instances create {} --cluster {} --cluster-zone {} '
            '--display-name={} --instance-type=DEVELOPMENT'.format(
                cls.CLOUD_BIGTABLE_INSTANCE_NAME, cls.CLOUD_BIGTABLE_INSTANCE_NAME,
                cls.ZONE, cls.CLOUD_BIGTABLE_INSTANCE_NAME)
        )
        assert ret_code == 0, "Failed to create bigtable instance {}. Last error: {}".format(
            cls.CLOUD_BIGTABLE_INSTANCE_NAME, stderr)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        ret_code, _, stderr = cls.run_command(
            'gcloud sql instances delete {}'.format(
                cls.CLOUD_SQL_INSTANCE_NAME)
        )
        assert ret_code == 0, "Failed to delete sql instance {}. Last error: {}".format(
            cls.CLOUD_SQL_INSTANCE_NAME, stderr)

        ret_code, _, stderr = cls.run_command(
            'gcloud beta bigtable instances delete {}'.format(
                cls.CLOUD_BIGTABLE_INSTANCE_NAME)
        )
        assert ret_code == 0, "Failed to delete bigtable instance {}. Last error: {}".format(
            cls.CLOUD_BIGTABLE_INSTANCE_NAME, stderr)

    def verify_instance(self, name):
        ret_code, _, stderr = self.run_command(
            'gcloud compute ssh {} --command "/usr/lib/sqoop/bin/sqoop version"'.format(
                name
            )
        )
        self.assertEqual(ret_code, 0, "Failed to validate cluster. Error: {}".format(stderr))

    def verify_importing_to_hdfs(self, name):
        ret_code, _, stderr = self.run_command(
            'gcloud compute ssh {} --command "/usr/lib/sqoop/bin/sqoop {}"'.format(
                name,
                "import --connect jdbc:mysql://localhost:3307/employees "
                "--username root "
                "--table employees --m 1"
            )
        )
        self.assertEqual(ret_code, 0, "Failed to validate cluster. Error: {}".format(stderr))

    def verify_importing_to_bigtable_hbase(self, name):
        hbase_table_name = "employees_{}".format(self.random_str(4))
        ret_code, _, stderr = self.run_command(
            'gcloud compute ssh {} --command "/usr/lib/sqoop/bin/sqoop {}"'.format(
                name,
                "import --connect jdbc:mysql://localhost:3307/employees "
                "--username root --table employees --columns \"emp_no,first_name\" "
                "--hbase-table {} --column-family my-column-family "
                "--hbase-row-key emp_no --m 1 --hbase-create-table".format(
                    hbase_table_name)
            )
        )
        self.assertEqual(ret_code, 0, "Failed to validate cluster. Error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0"]),
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.3", ["m"]),
        ("HA", "1.3", ["m-0"]),
    ], name_func=DataprocTestCase.generate_verbose_test_name)
    def test_sqoop(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @parameterized.expand([
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0"]),
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.3", ["m"]),
        ("HA", "1.3", ["m-0"]),
    ], name_func=DataprocTestCase.generate_verbose_test_name)
    def test_sqoop_import_from_cloud_sql_to_hdfs(
            self, configuration, dataproc_version, machine_suffixes):
        init_actions = self.INIT_ACTIONS + self.SQL_TO_HDFS_HELPER_ACTIONS
        self.createCluster(configuration, init_actions, dataproc_version,
                           metadata='enable-cloud-sql-hive-metastore=false,'
                                    'additional-cloud-sql-instances={}:{}=tcp:3307'.
                           format(self.CLOUD_SQL_METADATA, self.CLOUD_SQL_INSTANCE_NAME),
                           scopes='sql-admin'
                           )
        for machine_suffix in machine_suffixes:
            self.verify_importing_to_hdfs(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @parameterized.expand([
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0"]),
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.3", ["m"]),
        ("HA", "1.3", ["m-0"]),
    ], name_fun=DataprocTestCase.generate_verbose_test_name)
    def test_sqoop_import_from_cloud_sql_to_bigtable(
            self, configuration, dataproc_version, machine_suffixes):
        init_actions = self.INIT_ACTIONS + self.SQL_TO_BIGTABLE_HELPER_ACTIONS
        self.createCluster(configuration, init_actions, dataproc_version,
                           metadata='enable-cloud-sql-hive-metastore=false,'
                                    'additional-cloud-sql-instances={}:{}=tcp:3307,{}'.
                           format(self.CLOUD_SQL_METADATA, self.CLOUD_SQL_INSTANCE_NAME,
                                  self.CLOUD_BIGTABLE_METADATA),
                           scopes='cloud-platform'
                           )
        for machine_suffix in machine_suffixes:
            self.verify_importing_to_bigtable_hbase(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @parameterized.expand([
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0"]),
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.3", ["m"]),
        ("HA", "1.3", ["m-0"]),
    ], name_func=DataprocTestCase.generate_verbose_test_name)
    def test_sqoop_import_from_cloud_sql_to_hbase(
            self, configuration, dataproc_version, machine_suffixes):
        init_actions = self.INIT_ACTIONS + self.SQL_TO_HBASE_HELPER_ACTIONS
        if configuration == "HA":
            init_actions.remove('zookeeper/zookeeper.sh')
        self.createCluster(configuration, init_actions, dataproc_version,
                           metadata='enable-cloud-sql-hive-metastore=false,'
                                    'additional-cloud-sql-instances={}:{}=tcp:3307'.
                           format(self.CLOUD_SQL_METADATA, self.CLOUD_SQL_INSTANCE_NAME),
                           scopes='sql-admin',
                           machine_type="n1-standard-2"
                           )
        for machine_suffix in machine_suffixes:
            self.verify_importing_to_bigtable_hbase(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )


if __name__ == '__main__':
    unittest.main()
