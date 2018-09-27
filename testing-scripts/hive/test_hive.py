import json
import random
import unittest

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase


class HiveHCatalogTestCase(DataprocTestCase):
    COMPONENT = 'hive-hcatalog'
    INIT_ACTION = 'gs://dataproc-initialization-actions/hive-hcatalog/hive-hcatalog.sh'

    def verify_instance(self, name):
        table_name = "table_{}".format(random.randint(0, 1000))
        test_value = random.randint(0, 1000)

        self.__create_table(name, table_name)
        self.__insert_value(name, table_name, test_value)
        self.__read_value(name, table_name)

    def __create_table(self, name, table_name):
        status, stderr = self.__submit_hive_job(
            name,
            "CREATE TABLE {}(key string, value int)".format(
                table_name
            )
        )
        self.assertEqual(status, "DONE", "Failed to create table. Error: {}".format(stderr))

    def __insert_value(self, name, table_name, test_value):
        status, stderr = self.__submit_hive_job(
            name,
            "INSERT INTO TABLE {} VALUES ('key', '{}')".format(
                table_name, test_value
            )
        )
        self.assertEqual(status, "DONE", "Failed to insert value to table. Error: {}".format(stderr))

    def __read_value(self, name, table_name):
        for i in range(6):
            # Do it several times to detect flakiness
            status, stderr = self.__submit_hive_job(
                name,
                "SELECT value FROM {} WHERE key='key'".format(
                    table_name
                )
            )
            self.assertEqual(status, "DONE", "Failed to read value from table. Error: {}".format(stderr))

    def __submit_hive_job(self, cluster_name, job):
        status = None
        cmd = "gcloud dataproc jobs submit hive --format json --cluster {} -e \"{}\"".format(
            cluster_name, job
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        print(stdout)
        print(stderr)
        if ret_code == 0:
            stdout_dict = json.loads(stdout)
            status = stdout_dict.get("status", {}).get("state")
        return status, stderr

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
    def test_hive(self, configuration, dataproc_version):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version)
        self.verify_instance(self.getClusterName())


if __name__ == '__main__':
    unittest.main()
