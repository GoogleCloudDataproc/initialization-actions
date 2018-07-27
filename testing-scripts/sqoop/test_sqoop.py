
"""
This module provides testing functionality of the Sqoop Init Action.

Test logic:
1. Fill some data into cloud SQL using cloud-sql-proxy.
2. Use sqoop import job to get data from cloud SQL and put it into hdfs.

Note:
    Hive init action was added to provide additional libs to sqoop.
    In that example mysql connector will be added to sqoop's classpath.
"""

import unittest
import random

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase


class SqoopTestCase(DataprocTestCase):
    COMPONENT = 'sqoop'
    INIT_ACTION = 'gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh,' \
                  'gs://dataproc-initialization-actions/hive-hcatalog/hive-hcatalog.sh,' \
                  'gs://polidea-dataproc-utils/sqoop/sqoop.sh'
    PROPERTIES = 'hive:hive.metastore.warehouse.dir=gs://polidea-hive-metastore/hive-warehouse'
    TEST_SCRIPT_FILE_NAME = "verify_sqoop_import.py"
    SCOPES = 'sql-admin'
    SQL_DB_NAME = None


    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        _, region, _ = cls.run_command("gcloud config get-value compute/region")
        cls.REGION = region.strip().decode("utf-8") or "global"
        cls.PROJECT_METADATA = 'polidea-dataproc-testing:{}'.format(cls.REGION)

    def setUp(self):
        super().setUp()
        self.SQL_DB_NAME = "test-{}-db".format(random.randint(1, 10000))
        ret_code, stdout, stderr = self.run_command(
            'gcloud sql instances create {} --region {}'.format(self.SQL_DB_NAME, self.REGION)
        )
        self.assertEqual(ret_code, 0, "Failed to create sql instance {}. Last error: {}".format(self.SQL_DB_NAME, stderr))

    def tearDown(self):
        super().tearDown()
        ret_code, stdout, stderr = self.run_command(
            'gcloud sql instances delete {}'.format(self.SQL_DB_NAME)
        )
        self.assertEqual(ret_code, 0, "Failed to delete sql instance {}. Last error: {}".format(self.SQL_DB_NAME, stderr))

    def _run_import_job(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "python {}"'.format(
                name,
                self.TEST_SCRIPT_FILE_NAME,
            )
        )
        self.assertEqual(ret_code, 0, "Failed to validate cluster. Last error: {}".format(stderr))

    def verify_instance(self, name):
        self.upload_test_file(name)
        self._run_import_job(name)

    @parameterized.expand([
        # ("SINGLE", "1.0"),
        # ("SINGLE", "1.1"),
        ("SINGLE", "1.2", ["m"]),
        # ("STANDARD", "1.0"),
        # ("STANDARD", "1.1"),
        ("STANDARD", "1.2", ["m"]),
        # ("HA", "1.0"),
        # ("HA", "1.1"),
        ("HA", "1.2", ["m"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_sqoop(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(
            configuration,
            self.INIT_ACTION,
            dataproc_version,
            metadata='hive-metastore-instance={}:{}'.format(
                self.PROJECT_METADATA, self.SQL_DB_NAME
            ),
            scopes=self.SCOPES,
            properties=self.PROPERTIES,
        )
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )


if __name__ == '__main__':
    unittest.main()
