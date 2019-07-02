import json
import random
import unittest
from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HiveHCatalogTestCase(DataprocTestCase):
    COMPONENT = 'hive-hcatalog'
    INIT_ACTIONS = ['hive-hcatalog/hive-hcatalog.sh']

    def verify_instance(self, name, should_repeat_job):
        table_name = "table_{}".format(random.randint(0, 1000))
        test_value = random.randint(0, 1000)

        self.__create_table(name, table_name)
        self.__insert_value(name, table_name, test_value, should_repeat_job)
        self.__read_value(name, table_name)

    def __create_table(self, name, table_name):
        status, stderr = self.__submit_hive_job(
            name, "CREATE TABLE {}(key string, value int)".format(table_name))
        self.assertEqual(status, "DONE",
                         "Failed to create table. Error: {}".format(stderr))

    def __insert_value(self, name, table_name, test_value, should_repeat_job):
        status, stderr = self.__submit_hive_job(
            name,
            "INSERT INTO TABLE {} VALUES ('key', '{}')".format(
                table_name, test_value),
            should_repeat_job=should_repeat_job)
        self.assertEqual(
            status, "DONE",
            "Failed to insert value to table. Error: {}".format(stderr))

    def __read_value(self, name, table_name):
        for i in range(6):
            # Do it several times to detect flakiness
            status, stderr = self.__submit_hive_job(
                name,
                "SELECT value FROM {} WHERE key='key'".format(table_name))
            self.assertEqual(
                status, "DONE",
                "Failed to read value from table. Error: {}".format(stderr))

    def __submit_hive_job(self, cluster_name, job, should_repeat_job=False):
        status = None
        cmd = "gcloud dataproc jobs submit hive --format json --cluster {} -e \"{}\"".format(
            cluster_name, job)
        if should_repeat_job:
            cmd += " --max-failures-per-hour=5"
        ret_code, stdout, stderr = self.run_command(cmd)
        if ret_code == 0:
            stdout_dict = json.loads(stdout)
            status = stdout_dict.get("status", {}).get("state")
        return status, stderr

    @parameterized.expand(
        [
            ("SINGLE", "1.0", False),
            ("STANDARD", "1.0", False),
            ("HA", "1.0", False),
            ("SINGLE", "1.1", False),
            ("STANDARD", "1.1", False),
            ("HA", "1.1", False),
            ("SINGLE", "1.2", False),
            ("STANDARD", "1.2", False),
            ("HA", "1.2", False),
            ("SINGLE", "1.3", True),
            ("STANDARD", "1.3", True),
            ("HA", "1.3", True),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_hive(self, configuration, dataproc_version, should_repeat_job):
        self.createCluster(configuration, self.INIT_ACTIONS, dataproc_version)
        self.verify_instance(self.getClusterName(), should_repeat_job)


if __name__ == '__main__':
    unittest.main()
