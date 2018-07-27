import unittest
import random

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase


class CloudSqlProxyTestCase(DataprocTestCase):
    COMPONENT = 'cloud-sql-proxy'
    INIT_ACTION = 'gs://polidea-dataproc-utils/cloud-sql-proxy/cloud-sql-proxy.sh'
    TEST_SCRIPT_LOCATION = 'dataproc-initialization-actions/cloud-sql-proxy'
    TEST_SCRIPT_FILE_NAME = 'pyspark_metastore_test.py'
    SCOPES = 'sql-admin'
    PROPERTIES = 'hive:hive.metastore.warehouse.dir=gs://polidea-hive-metastore/hive-warehouse'
    WAREHOUSE_BUCKET = 'gs://polidea-hive-metastore/hive-warehouse'
    DB_NAME = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        _, region, _ = cls.run_command("gcloud config get-value compute/region")
        cls.REGION = region.strip().decode("utf-8") or "global"
        cls.PROJECT_METADATA = 'polidea-dataproc-testing:{}'.format(cls.REGION)

    def setUp(self):
        super().setUp()
        self.DB_NAME = "test-{}-db".format(random.randint(1, 10000))
        ret_code, stdout, stderr = self.run_command(
            'gcloud sql instances create {} --region {}'.format(self.DB_NAME, self.REGION)
        )
        self.assertEqual(ret_code, 0, "Failed to create sql instance {}. Last error: {}".format(self.DB_NAME, stderr))

    def tearDown(self):
        super().tearDown()
        ret_code, stdout, stderr = self.run_command(
            'gcloud sql instances delete {}'.format(self.DB_NAME)
        )
        self.assertEqual(ret_code, 0, "Failed to delete sql instance {}. Last error: {}".format(self.DB_NAME, stderr))

    def verify_instance(self, name):
        self.__submit_pyspark_job(name)

    def __submit_pyspark_job(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud dataproc jobs submit pyspark --cluster {} gs://{}/{}'.format(
                name,
                self.TEST_SCRIPT_LOCATION,
                self.TEST_SCRIPT_FILE_NAME
            )
        )
        self.assertEqual(ret_code, 0, "Failed to vaildate cluster. Last error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.0"),
        ("STANDARD", "1.0"),
        ("HA", "1.0"),
        ("SINGLE", "1.1"),
        ("STANDARD", "1.1"),
        ("HA", "1.1"),
        ("SINGLE", "1.2"),
        ("STANDARD", "1.2"),
        ("HA", "1.2"),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_cloud_sql_proxy(self, configuration, dataproc_version):
        self.createCluster(
            configuration,
            self.INIT_ACTION,
            dataproc_version,
            metadata='hive-metastore-instance={}:{}'.format(
                self.PROJECT_METADATA, self.DB_NAME
            ),
            scopes=self.SCOPES,
            properties=self.PROPERTIES,
        )

        self.verify_instance(self.getClusterName())


if __name__ == '__main__':
    unittest.main()
