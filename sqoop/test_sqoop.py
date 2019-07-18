import json
import unittest
import datetime
from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

FAILED_CMD_MSG_FMT = "Failed to execute command:\n{}\nSTDOUT:\n{}\nSTDERR:\n{}"


class SqoopTestCase(DataprocTestCase):
    COMPONENT = 'sqoop'
    INIT_ACTIONS = ['sqoop/sqoop.sh']
    SQL_TO_HDFS_HELPER_ACTIONS = ['cloud-sql-proxy/cloud-sql-proxy.sh']
    SQL_TO_BIGTABLE_HELPER_ACTIONS = [
        'cloud-sql-proxy/cloud-sql-proxy.sh', 'bigtable/bigtable.sh'
    ]
    SQL_TO_HBASE_HELPER_ACTIONS = [
        'cloud-sql-proxy/cloud-sql-proxy.sh', 'zookeeper/zookeeper.sh',
        'hbase/hbase.sh'
    ]
    CLOUD_SQL_INSTANCE_NAME = None
    CLOUD_BIGTABLE_INSTANCE_NAME = None
    TEST_DB_PATH = "/sqoop/test_sql_db_dump.gz"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        _, zone, _ = cls.run_command("gcloud config get-value compute/zone")
        cls.ZONE = zone.strip()
        _, region, _ = cls.run_command(
            "gcloud config get-value compute/region")
        cls.REGION = region.strip() or zone.strip()[:-2]
        _, project, _ = cls.run_command("gcloud config get-value project")
        project = project.strip()
        cls.CLOUD_SQL_METADATA = '{}:{}'.format(project, cls.REGION)
        cls.CLOUD_SQL_INSTANCE_NAME = "test-{}-sql-db".format(
            datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        cls.CLOUD_BIGTABLE_INSTANCE_NAME = "test-{}-db".format(
            datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        cls.CLOUD_BIGTABLE_METADATA = \
            "bigtable-instance={},bigtable-project={}".format(
                cls.CLOUD_BIGTABLE_INSTANCE_NAME, project)

        create_sql_cmd = "gcloud sql instances create {} --region {} {}".format(
            cls.CLOUD_SQL_INSTANCE_NAME, cls.REGION, "--async --format=json")
        ret_code, stdout, stderr = cls.run_command(create_sql_cmd)
        assert ret_code == 0, FAILED_CMD_MSG_FMT.format(
            create_sql_cmd, stdout, stderr)

        operation_id = json.loads(stdout.strip())['name']
        wait_sql_cmd = 'gcloud sql operations wait {} --timeout=600'.format(
            operation_id)
        ret_code, stdout, stderr = cls.run_command(wait_sql_cmd)
        assert ret_code == 0, FAILED_CMD_MSG_FMT.format(
            wait_sql_cmd, stdout, stderr)

        describe_sql_cmd = "gcloud sql instances describe {} --format=json".format(
            cls.CLOUD_SQL_INSTANCE_NAME)
        ret_code, stdout, stderr = cls.run_command(describe_sql_cmd)
        assert ret_code == 0, FAILED_CMD_MSG_FMT.format(
            describe_sql_cmd, stdout, stderr)

        service_account = json.loads(
            stdout.strip())['serviceAccountEmailAddress']
        grant_sql_permissions_cmd = "gsutil acl ch -u {}:R {}".format(
            service_account, cls.INIT_ACTIONS_REPO + cls.TEST_DB_PATH)
        ret_code, stdout, stderr = cls.run_command(grant_sql_permissions_cmd)
        assert ret_code == 0, FAILED_CMD_MSG_FMT.format(
            grant_sql_permissions_cmd, stdout, stderr)

        import_sql_cmd = 'gcloud sql import sql {} {}'.format(
            cls.CLOUD_SQL_INSTANCE_NAME,
            cls.INIT_ACTIONS_REPO + cls.TEST_DB_PATH)
        ret_code, stdout, stderr = cls.run_command(import_sql_cmd)
        assert ret_code == 0, FAILED_CMD_MSG_FMT.format(
            import_sql_cmd, stdout, stderr)

        create_bt_cmd = 'gcloud beta bigtable instances create {} --cluster {}' \
                        ' --cluster-zone {} --display-name={}' \
                        ' --instance-type=DEVELOPMENT'.format(
            cls.CLOUD_BIGTABLE_INSTANCE_NAME, cls.CLOUD_BIGTABLE_INSTANCE_NAME,
            cls.ZONE, cls.CLOUD_BIGTABLE_INSTANCE_NAME)
        ret_code, stdout, stderr = cls.run_command(create_bt_cmd)
        assert ret_code == 0, FAILED_CMD_MSG_FMT.format(
            create_bt_cmd, stdout, stderr)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        delete_sql_cmd = 'gcloud sql instances delete {}'.format(
            cls.CLOUD_SQL_INSTANCE_NAME)
        ret_code, stdout, stderr = cls.run_command(delete_sql_cmd)
        assert ret_code == 0, FAILED_CMD_MSG_FMT.format(
            delete_sql_cmd, stdout, stderr)

        delete_bt_fmt = 'gcloud beta bigtable instances delete {}'.format(
            cls.CLOUD_BIGTABLE_INSTANCE_NAME)
        ret_code, _, stderr = cls.run_command(delete_bt_fmt)
        assert ret_code == 0, FAILED_CMD_MSG_FMT.format(
            delete_bt_fmt, stdout, stderr)

    def verify_instance(self, name):
        self.assert_instance_command(name, "/usr/lib/sqoop/bin/sqoop version")

    def verify_importing_to_hdfs(self, name):
        self.assert_instance_command(
            name, "/usr/lib/sqoop/bin/sqoop import"
            " --connect jdbc:mysql://localhost:3307/employees"
            " --username root --table employees --m 1")

    def verify_importing_to_bigtable_hbase(self, name):
        hbase_table_name = "employees_{}".format(self.random_str(4))
        self.assert_instance_command(
            name, "/usr/lib/sqoop/bin/sqoop import"
            " --connect jdbc:mysql://localhost:3307/employees"
            " --username root --table employees --columns emp_no,first_name"
            " --hbase-table {} --column-family my-column-family"
            " --hbase-row-key emp_no --m 1 --hbase-create-table".format(
                hbase_table_name))

    @parameterized.expand(
        [
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("HA", "1.2", ["m-0"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.3", ["m-0"]),
            ("SINGLE", "1.4", ["m"]),
            ("STANDARD", "1.4", ["m"]),
            ("HA", "1.4", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_sqoop(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTIONS, dataproc_version)
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
            ("SINGLE", "1.4", ["m"]),
            ("STANDARD", "1.4", ["m"]),
            ("HA", "1.4", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_sqoop_import_from_cloud_sql_to_hdfs(self, configuration,
                                                 dataproc_version,
                                                 machine_suffixes):
        init_actions = self.INIT_ACTIONS + self.SQL_TO_HDFS_HELPER_ACTIONS
        self.createCluster(
            configuration,
            init_actions,
            dataproc_version,
            metadata='enable-cloud-sql-hive-metastore=false,'
            'additional-cloud-sql-instances={}:{}=tcp:3307'.format(
                self.CLOUD_SQL_METADATA, self.CLOUD_SQL_INSTANCE_NAME),
            scopes='sql-admin')
        for machine_suffix in machine_suffixes:
            self.verify_importing_to_hdfs("{}-{}".format(
                self.getClusterName(), machine_suffix))

    @parameterized.expand(
        [
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("HA", "1.2", ["m-0"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.3", ["m-0"]),
            ("SINGLE", "1.4", ["m"]),
            ("STANDARD", "1.4", ["m"]),
            ("HA", "1.4", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_sqoop_import_from_cloud_sql_to_bigtable(self, configuration,
                                                     dataproc_version,
                                                     machine_suffixes):
        init_actions = self.INIT_ACTIONS + self.SQL_TO_BIGTABLE_HELPER_ACTIONS
        self.createCluster(
            configuration,
            init_actions,
            dataproc_version,
            metadata='enable-cloud-sql-hive-metastore=false,'
            'additional-cloud-sql-instances={}:{}=tcp:3307,{}'.format(
                self.CLOUD_SQL_METADATA, self.CLOUD_SQL_INSTANCE_NAME,
                self.CLOUD_BIGTABLE_METADATA),
            scopes='cloud-platform')
        for machine_suffix in machine_suffixes:
            self.verify_importing_to_bigtable_hbase("{}-{}".format(
                self.getClusterName(), machine_suffix))

    @parameterized.expand(
        [
            ("SINGLE", "1.2", ["m"]),
            ("STANDARD", "1.2", ["m"]),
            ("HA", "1.2", ["m-0"]),
            ("SINGLE", "1.3", ["m"]),
            ("STANDARD", "1.3", ["m"]),
            ("HA", "1.3", ["m-0"]),
            ("SINGLE", "1.4", ["m"]),
            ("STANDARD", "1.4", ["m"]),
            ("HA", "1.4", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_sqoop_import_from_cloud_sql_to_hbase(self, configuration,
                                                  dataproc_version,
                                                  machine_suffixes):
        init_actions = self.INIT_ACTIONS + self.SQL_TO_HBASE_HELPER_ACTIONS
        if configuration == "HA":
            init_actions.remove('zookeeper/zookeeper.sh')
        self.createCluster(
            configuration,
            init_actions,
            dataproc_version,
            metadata='enable-cloud-sql-hive-metastore=false,'
            'additional-cloud-sql-instances={}:{}=tcp:3307'.format(
                self.CLOUD_SQL_METADATA, self.CLOUD_SQL_INSTANCE_NAME),
            scopes='sql-admin',
            machine_type="n1-standard-2")
        for machine_suffix in machine_suffixes:
            self.verify_importing_to_bigtable_hbase("{}-{}".format(
                self.getClusterName(), machine_suffix))


if __name__ == '__main__':
    unittest.main()
