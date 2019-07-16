import random
import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class PrestoTestCase(DataprocTestCase):
    COMPONENT = 'presto'
    INIT_ACTIONS = ['presto/presto.sh']

    def verify_instance(self, name, coordinators, workers):
        schema = "schema_{}".format(random.randint(0, 1000))
        table = "table_{}".format(random.randint(0, 1000))

        self.__verify_coordinators_count(name, coordinators)
        self.__verify_workers_count(name, workers)

        self.__create_schema_via_hive(name, schema)
        self.__verify_schema_via_presto(name, schema)

        self.__create_table(name, table, schema)
        self.__insert_data_into_table_via_hive(name, table, schema)
        self.__validate_data_in_table_via_presto(name, table, schema)

    def __create_schema_via_hive(self, name, schema):
        query = "create schema {};".format(schema)
        self.assert_instance_command(name, "hive -e '{}'".format(query))

    def __verify_schema_via_presto(self, name, schema):
        query = "show schemas;"
        _, stdout, _ = self.assert_instance_command(
            name,
            "presto --catalog=hive --execute='{}' --output-format TSV".format(
                query))
        schemas = str(stdout).split("\n")
        self.assertIn(schema, schemas,
                      "Schema {} not found in {}".format(schema, schemas))

    def __create_table(self, name, table, schema):
        query = "create table {}(number int) STORED AS SEQUENCEFILE;".format(
            table)
        self.assert_instance_command(
            name, "hive --database {} -e '{}'".format(schema, query))

    def __insert_data_into_table_via_hive(self, name, table, schema):
        query = "insert into {} values {};".format(
            table, ",".join(["({})".format(x % 2) for x in range(400)]))
        self.assert_instance_command(
            name, "hive --database {} -e '{}'".format(schema, query))

    def __validate_data_in_table_via_presto(self, name, table, schema):
        query = "SELECT number, count(*) AS total FROM {} GROUP BY number ORDER BY number DESC;".format(
            table)
        _, stdout, _ = self.assert_instance_command(
            name,
            "presto --catalog=hive --schema={} --execute='{}' --output-format TSV"
            .format(schema, query))
        self.assertEqual(stdout, "1\t200\n0\t200\n")

    def __verify_coordinators_count(self, name, coordinators):
        query = "select count(*) from system.runtime.nodes where coordinator=true"
        _, stdout, _ = self.assert_instance_command(
            name, "presto --execute '{}' --output-format TSV".format(query))
        self.assertEqual(
            coordinators, int(stdout),
            "Bad number of coordinators. Expected: {}\tFound: {}".format(
                coordinators, stdout))

    def __verify_workers_count(self, name, workers):
        query = "select count(*) from system.runtime.nodes where coordinator=false"
        _, stdout, _ = self.assert_instance_command(
            name, "presto --execute '{}' --output-format TSV".format(query))
        self.assertEqual(
            workers, int(stdout),
            "Bad number of workers. Expected: {}\tFound: {}".format(
                workers, stdout))

    @parameterized.expand(
        [
            ("SINGLE", "1.0", ["m"], 1, 0),
            ("STANDARD", "1.0", ["m"], 1, 2),
            ("HA", "1.0", ["m-0"], 1, 2),
            ("SINGLE", "1.1", ["m"], 1, 0),
            ("STANDARD", "1.1", ["m"], 1, 2),
            ("HA", "1.1", ["m-0"], 1, 2),
            ("SINGLE", "1.2", ["m"], 1, 0),
            ("STANDARD", "1.2", ["m"], 1, 2),
            ("HA", "1.2", ["m-0"], 1, 2),
            ("SINGLE", "1.3", ["m"], 1, 0),
            ("STANDARD", "1.3", ["m"], 1, 2),
            ("HA", "1.3", ["m-0"], 1, 2),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_presto(self, configuration, dataproc_version, machine_suffixes,
                    coordinators, workers):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           machine_type="n1-standard-2")
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix),
                coordinators, workers)


if __name__ == '__main__':
    unittest.main()
