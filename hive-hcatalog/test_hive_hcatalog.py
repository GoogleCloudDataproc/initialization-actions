import json
import random

from absl.testing import absltest
from absl.testing import parameterized

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
        jop_params = "--format json -e \"{}\"".format(job)
        if should_repeat_job:
            jop_params += " --max-failures-per-hour=5"
        ret_code, stdout, stderr = self.assert_dataproc_job(
            cluster_name, 'hive', jop_params)
        status = None
        if ret_code == 0:
            stdout_dict = json.loads(stdout)
            status = stdout_dict.get("status", {}).get("state")
        return status, stderr

    @parameterized.parameters(
        ("SINGLE", False),
        ("STANDARD", False),
        ("HA", False),
        ("SINGLE", True),
        ("STANDARD", True),
        ("HA", True),
    )
    def test_hive_hcatalog(self, configuration, should_repeat_job):
        self.createCluster(configuration, self.INIT_ACTIONS)
        self.verify_instance(self.getClusterName(), should_repeat_job)


if __name__ == '__main__':
    absltest.main()
