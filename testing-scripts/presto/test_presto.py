import random
import unittest

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase


class PrestoTestCase(DataprocTestCase):
    COMPONENT = 'presto'
    INIT_ACTION = 'gs://dataproc-initialization-actions/presto/presto.sh'

    def setUp(self):
        pass

    def verify_instance(self, name, coordinators, workers):
        schema = "test_{}".format(random.randint(0, 1000))
        table = "table{}".format(random.randint(0, 1000))

        self.__create_schema(name, schema)
        self.__create_table(name, table)  # TODO maybe insert sth and query for it
        self.__verify_schema(name, schema)
        self.__verify_coordinators_count(coordinators, name)
        self.__verify_workers_count(coordinators, name, workers)

    def __create_schema(self, name, schema):
        ret_code, stdout, stderr = self.run_command(
            "gcloud compute ssh {} -- \"hive -e 'create schema {};'\"".format(
                name,
                schema
            )
        )
        self.assertEqual(ret_code, 0, "Failed to create Schema. Error: {}".format(stderr))

    def __create_table(self, name, table):
        ret_code, stdout, stderr = self.run_command(
            "gcloud compute ssh {} -- \"hive -e 'create table {}(number int) STORED AS SEQUENCEFILE;'\"".format(
                name,
                table
            )
        )
        self.assertEqual(ret_code, 0, "Failed to create Table. Error: {}".format(stderr))

    def __verify_schema(self, name, schema):
        ret_code, stdout, stderr = self.run_command(
            "gcloud compute ssh {} -- \"presto --catalog=hive --execute='show schemas;'\"".format(
                name
            )
        )
        self.assertEqual(ret_code, 0, "Failed to fetch Schema. Error: {}".format(stderr))
        self.assertIn(schema, str(stdout), "Schema {} not found in {}".format(schema, stdout))

    def __verify_coordinators_count(self, coordinators, name):
        ret_code, stdout, stderr = self.run_command(
            "gcloud compute ssh {} -- \"presto --execute 'select count(*) from system.runtime.nodes where coordinator=true'\"".format(
                name
            )
        )
        self.assertEqual(ret_code, 0, "Failed to check number of coodrinators. Error: {}".format(stderr))
        self.assertIn(str(coordinators), str(stdout), "Bad number of coordinators. Expected: {}\tFound: {}".format(
            coordinators, str(stdout)
        ))

    def __verify_workers_count(self, coordinators, name, workers):
        ret_code, stdout, stderr = self.run_command(
            "gcloud compute ssh {} -- \"presto --execute 'select count(*) from system.runtime.nodes where coordinator=false'\"".format(
                name
            )
        )
        self.assertEqual(ret_code, 0, "Failed to check number of coodrinators. Error: {}".format(stderr))
        self.assertIn(str(workers), str(stdout), "Bad number of workers. Expected: {}\tFound: {}".format(
            coordinators, str(stdout)
        ))

    @parameterized.expand([
        ("SINGLE", "1.0", ["m"], 1, 0),
        ("STANDARD", "1.0", ["m"], 1, 2),
        ("HA", "1.0", ["m-0"], 1, 2),
        ("SINGLE", "1.1", ["m"], 1, 0),
        ("STANDARD", "1.1", ["m"], 1, 2),
        ("HA", "1.1", ["m-0"], 1, 2),
        ("SINGLE", "1.2", ["m"], 1, 0),
        ("STANDARD", "1.2", ["m"], 1, 2),
        ("HA", "1.2", ["m-0"], 1, 2),
    ])
    def test_pesto(self, configuration, dataproc_version, machine_suffixes, coordinators, workers):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                ),
                coordinators,
                workers
            )


if __name__ == '__main__':
    unittest.main()
