import unittest
import json
from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class SolrTestCase(DataprocTestCase):
    COMPONENT = 'solr'
    INIT_ACTION = 'gs://dataproc-initialization-actions/solr/solr.sh'
    SOLR_DIR = '/opt/solr'
    SOLR_EXAMPLE_DOC = 'https://raw.githubusercontent.com/apache/lucene-solr/master/solr/example/films/films.json'

    def create_core_collection(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} --command "{}"'.format(
                name,
                "sudo runuser -l solr -s /bin/bash "
                "-c '{}/bin/solr create -c films -s 2 -rf 2'".format(
                    self.SOLR_DIR
                )
            )
        )
        self.assertEqual(ret_code, 0, "Failed to create core or collection. Error: {}".format(stderr))

    def use_api_to_update_schema(self, name):
        json = '{\\"add-field\\": {\\"name\\":\\"name\\", \\"type\\":\\"text_general\\", \\"multiValued\\":false, \\"stored\\":true}}'
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} --command "{}"'.format(
                name,
                "curl -X POST -H 'Content-type:application/json' --data-binary '{}' "
                "http://localhost:8983/solr/films/schema".format(
                    json
                )
            )
        )
        self.assertEqual(ret_code, 0, "Failed to update schema using API. Error: {}".format(stderr))

    def use_api_to_create_catch_all_rule(self, name):
        json = '{\\"add-copy-field\\" : {\\"source\\":\\"*\\",\\"dest\\":\\"_text_\\"}}'
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} --command "{}"'.format(
                name,
                "curl -X POST -H 'Content-type:application/json' --data-binary '{}' " 
                "http://localhost:8983/solr/films/schema".format(
                    json
                )
            )
        )
        self.assertEqual(ret_code, 0, "Failed to create rule using API. Error: {}".format(stderr))

    def post_test_data(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} --command "{}"'.format(
                name,
                "wget -q {} -O /tmp/films.json".format(
                    self.SOLR_EXAMPLE_DOC
                )
            )
        )
        self.assertEqual(ret_code, 0, "Failed to get test data. Error: {}".format(stderr))
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} --command "{}"'.format(
                name,
                "sudo runuser -l solr -s /bin/bash "
                "-c '{}/bin/post -c films  /tmp/films.json'".format(
                    self.SOLR_DIR
                )
            )
        )
        self.assertEqual(ret_code, 0, "Failed to post data. Error: {}".format(stderr))

    def run_test_query(self, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} --command "{}"'.format(
                name,
                "curl --silent 'http://localhost:8983/solr/films/select?q=Comedy&rows=0'"
            )
        )
        self.assertEqual(ret_code, 0, "Failed to query solr using API. Error: {}".format(stderr))
        out_json = json.loads(stdout)
        self.assertEqual(out_json['response']['numFound'], 417,
                         "Failed to get right number of matches. Got:{}".format(stdout))

    def verify_instance(self, name):
        self.create_core_collection(name)
        self.use_api_to_update_schema(name)
        self.use_api_to_create_catch_all_rule(name)
        self.post_test_data(name)
        self.run_test_query(name)

    @parameterized.expand([
        ("SINGLE", "1.2", ["m"]),
        ("STANDARD", "1.2", ["m"]),
        ("HA", "1.2", ["m-0"]),
        ("SINGLE", "1.3", ["m"]),
        ("STANDARD", "1.3", ["m"]),
        ("HA", "1.3", ["m-0"]),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_solr(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )


if __name__ == '__main__':
    unittest.main()